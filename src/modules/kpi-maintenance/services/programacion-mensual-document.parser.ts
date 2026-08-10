import { BadRequestException } from '@nestjs/common';
import * as mammoth from 'mammoth';
import { dirname, join, sep } from 'path';
import * as XLSX from 'xlsx';

export type MonthlyScheduleDocumentRow = {
  section_hint: string | null;
  equipo_hint: string;
  fecha_programada: string;
  mantenimiento: string;
  horas_mantenimiento: number | null;
  horometro_anterior: number | null;
  horometro_actual: number | null;
  permiso_trabajo: string | null;
  source: Record<string, unknown>;
};

export type MonthlyScheduleDocumentResult = {
  rows: MonthlyScheduleDocumentRow[];
  warnings: string[];
  metadata: {
    format: 'PDF' | 'WORD' | 'EXCEL';
    location_hint: string | null;
    sheets?: string[];
    pages?: number;
  };
};

function cleanText(value: unknown) {
  return String(value ?? '')
    .replace(/\u00a0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeToken(value: unknown) {
  return cleanText(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
}

function decodeHtml(value: string) {
  return cleanText(
    value
      .replace(/<br\s*\/?>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/&quot;/gi, '"')
      .replace(/&#39;/gi, "'"),
  );
}

function parseDateOnly(value: unknown): string | null {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }
  if (typeof value === 'number' && Number.isFinite(value)) {
    const decoded = XLSX.SSF.parse_date_code(value);
    if (decoded) {
      return `${decoded.y}-${String(decoded.m).padStart(2, '0')}-${String(decoded.d).padStart(2, '0')}`;
    }
  }
  const raw = cleanText(value).replace(/\s+/g, '');
  const match = raw.match(/^(\d{1,4})[\/\-.](\d{1,2})[\/\-.](\d{2,4})$/);
  if (!match) return null;
  let year: number;
  let month: number;
  let day: number;
  if (match[1].length === 4) {
    year = Number(match[1]);
    month = Number(match[2]);
    day = Number(match[3]);
  } else {
    day = Number(match[1]);
    month = Number(match[2]);
    year = Number(match[3]);
  }
  if (year < 100) year += 2000;
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() + 1 !== month ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return date.toISOString().slice(0, 10);
}

function parseHours(value: unknown): number | null {
  const match = cleanText(value)
    .replace(',', '.')
    .match(/(\d+(?:\.\d+)?)\s*(?:H|HR|HRS|HORA|HORAS)\b/i);
  const parsed = match
    ? Number(match[1])
    : Number(cleanText(value).replace(',', '.'));
  return Number.isFinite(parsed) && parsed > 0
    ? Number(parsed.toFixed(2))
    : null;
}

function parseNumber(value: unknown): number | null {
  const normalized = cleanText(value)
    .replace(/[^0-9,.-]/g, '')
    .replace(',', '.');
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? Number(parsed.toFixed(2)) : null;
}

function headerIndexes(cells: unknown[]) {
  const normalized = cells.map(normalizeToken);
  const find = (predicate: (value: string) => boolean) =>
    normalized.findIndex(predicate);
  const unit = find((value) => /UNIDAD|EQUIPO|ACTIVO|SISTEMA/.test(value));
  const date = find((value) => /FECHA/.test(value));
  const maintenance = find(
    (value) =>
      /MANTENIMIENTO|ACTIVIDAD|TRABAJO/.test(value) && !/HORAS/.test(value),
  );
  const maintenanceHours = find(
    (value) =>
      /HORAS/.test(value) && /MANTENIMIENTO|DURACION|ASIGNADAS/.test(value),
  );
  const previousHorometer = find(
    (value) => /HOROMETRO/.test(value) && /ANTERIOR|ULTIMO/.test(value),
  );
  const horometer = normalized.findIndex(
    (value, index) => /HOROMETRO/.test(value) && index !== previousHorometer,
  );
  const permit = find((value) => /PERMISO|N.?\s*OT|ORDEN/.test(value));
  return {
    unit,
    date,
    maintenance,
    maintenanceHours,
    previousHorometer,
    horometer,
    permit,
    valid: unit >= 0 && date >= 0 && maintenance >= 0,
  };
}

function buildRow(
  cells: unknown[],
  indexes: ReturnType<typeof headerIndexes>,
  sectionHint: string | null,
  source: Record<string, unknown>,
): MonthlyScheduleDocumentRow | null {
  const date = parseDateOnly(cells[indexes.date]);
  const equipoHint = cleanText(cells[indexes.unit]);
  const mantenimiento = cleanText(cells[indexes.maintenance]);
  if (!date || !equipoHint || !mantenimiento) return null;
  return {
    section_hint: sectionHint,
    equipo_hint: equipoHint,
    fecha_programada: date,
    mantenimiento,
    horas_mantenimiento:
      indexes.maintenanceHours >= 0
        ? parseHours(cells[indexes.maintenanceHours])
        : null,
    horometro_anterior:
      indexes.previousHorometer >= 0
        ? parseNumber(cells[indexes.previousHorometer])
        : null,
    horometro_actual:
      indexes.horometer >= 0 ? parseNumber(cells[indexes.horometer]) : null,
    permiso_trabajo:
      indexes.permit >= 0 ? cleanText(cells[indexes.permit]) || null : null,
    source,
  };
}

function likelySection(cells: unknown[]) {
  const values = cells.map(cleanText).filter(Boolean);
  if (values.length !== 1) return null;
  const value = values[0];
  const normalized = normalizeToken(value);
  if (
    !value ||
    value.length > 80 ||
    /UNIDAD|FECHA|MANTENIMIENTO|HOROMETRO|PERMISO|FIRMAN|REALIZADO|APROBADO/.test(
      normalized,
    )
  ) {
    return null;
  }
  return value;
}

function parseTabularRows(
  tableRows: unknown[][],
  sourceFactory: (rowIndex: number) => Record<string, unknown>,
  fallbackSection: string | null,
) {
  const rows: MonthlyScheduleDocumentRow[] = [];
  let indexes: ReturnType<typeof headerIndexes> | null = null;
  let section = fallbackSection;
  for (let index = 0; index < tableRows.length; index += 1) {
    const cells = tableRows[index] ?? [];
    const detectedHeader = headerIndexes(cells);
    if (detectedHeader.valid) {
      indexes = detectedHeader;
      continue;
    }
    if (!indexes) {
      section = likelySection(cells) ?? section;
      continue;
    }
    const row = buildRow(cells, indexes, section, sourceFactory(index));
    if (row) rows.push(row);
  }
  return rows;
}

async function parseExcel(
  buffer: Buffer,
): Promise<MonthlyScheduleDocumentResult> {
  const workbook = XLSX.read(buffer, { type: 'buffer', cellDates: true });
  const rows: MonthlyScheduleDocumentRow[] = [];
  for (const sheetName of workbook.SheetNames) {
    const sheet = workbook.Sheets[sheetName];
    if (!sheet) continue;
    const values = XLSX.utils.sheet_to_json<unknown[]>(sheet, {
      header: 1,
      defval: null,
      raw: true,
    });
    rows.push(
      ...parseTabularRows(
        values,
        (rowIndex) => ({
          format: 'EXCEL',
          sheet: sheetName,
          row: rowIndex + 1,
        }),
        sheetName,
      ),
    );
  }
  return {
    rows,
    warnings: [],
    metadata: {
      format: 'EXCEL',
      location_hint: null,
      sheets: [...workbook.SheetNames],
    },
  };
}

async function parseWord(
  buffer: Buffer,
): Promise<MonthlyScheduleDocumentResult> {
  const converted = await mammoth.convertToHtml({ buffer });
  const html = converted.value;
  const locationMatch = decodeHtml(html).match(
    /Locaci[oó]n:\s*(.*?)(?:Sitio del trabajo:|A continuaci[oó]n|$)/i,
  );
  const locationHint = cleanText(locationMatch?.[1]) || null;
  const rows: MonthlyScheduleDocumentRow[] = [];
  let tableIndex = 0;
  for (const tableMatch of html.matchAll(/<table[\s\S]*?<\/table>/gi)) {
    tableIndex += 1;
    const tableHtml = tableMatch[0];
    const values = [...tableHtml.matchAll(/<tr[\s\S]*?<\/tr>/gi)].map(
      (rowMatch) =>
        [...rowMatch[0].matchAll(/<t[dh][\s\S]*?<\/t[dh]>/gi)].map(
          (cellMatch) => decodeHtml(cellMatch[0]),
        ),
    );
    rows.push(
      ...parseTabularRows(
        values,
        (rowIndex) => ({
          format: 'WORD',
          table: tableIndex,
          row: rowIndex + 1,
        }),
        locationHint,
      ),
    );
  }
  return {
    rows,
    warnings: converted.messages
      .map((item) => cleanText(item.message))
      .filter(Boolean),
    metadata: { format: 'WORD', location_hint: locationHint },
  };
}

type PdfTextItem = { x: number; y: number; text: string };

function joinPdfCell(items: PdfTextItem[], fromX: number, toX: number) {
  return cleanText(
    items
      .filter((item) => item.x >= fromX && item.x < toX)
      .sort((a, b) => b.y - a.y || a.x - b.x)
      .map((item) => item.text)
      .join(' '),
  );
}

function compactPdfValue(value: string) {
  let compacted = cleanText(value)
    .replace(/\s*([\/\-])\s*/g, '$1')
    .replace(/\b(UG[NS]?)\s+(\d+)\b/gi, '$1$2')
    .replace(/(\d)\s+(H|HRS)\b/gi, '$1$2');
  while (/(\d)\s+(?=\d)/.test(compacted)) {
    compacted = compacted.replace(/(\d)\s+(?=\d)/g, '$1');
  }
  return compacted;
}

function detectPdfSection(
  lines: Array<{ y: number; text: string; minX: number; maxX: number }>,
  rowY: number,
) {
  return (
    lines
      .filter((line) => {
        if (line.y <= rowY || line.minX >= 160 || line.maxX >= 260) {
          return false;
        }
        return lines.some(
          (candidate) =>
            candidate.y < line.y &&
            candidate.y > line.y - 100 &&
            /UNIDAD|FECHA|MANTENIMIENTO|HOROMETRO/i.test(candidate.text),
        );
      })
      .sort((a, b) => a.y - b.y)
      .map((line) => cleanText(line.text))
      .find((value) => {
        const normalized = normalizeToken(value).replace(/\s+/g, '');
        return (
          value.length <= 35 &&
          /[A-Z]/.test(normalized) &&
          !/UNIDAD|FECHA|TIPO|HORAS|MANTENIMIENTO|PROGRAMADO|HOROMETRO|PERMISO|TRABAJO/.test(
            normalized,
          )
        );
      }) ?? null
  );
}

async function parsePdf(
  buffer: Buffer,
): Promise<MonthlyScheduleDocumentResult> {
  const { getDocument } = await import('pdfjs-dist/legacy/build/pdf.mjs');
  const standardFontDataUrl = `${join(
    dirname(require.resolve('pdfjs-dist/package.json')),
    'standard_fonts',
  )}${sep}`;
  const document = await getDocument({
    data: new Uint8Array(buffer),
    disableFontFace: true,
    useSystemFonts: false,
    standardFontDataUrl,
  }).promise;
  const rows: MonthlyScheduleDocumentRow[] = [];
  let locationHint: string | null = null;

  for (let pageNumber = 1; pageNumber <= document.numPages; pageNumber += 1) {
    const page = await document.getPage(pageNumber);
    const content = await page.getTextContent();
    const items: PdfTextItem[] = (content.items as any[])
      .map((item) => ({
        x: Number(item?.transform?.[4] ?? 0),
        y: Number(item?.transform?.[5] ?? 0),
        text: cleanText(item?.str),
      }))
      .filter((item) => item.text);
    const lineMap = new Map<number, PdfTextItem[]>();
    for (const item of items) {
      const existingY = [...lineMap.keys()].find(
        (key) => Math.abs(key - item.y) <= 2,
      );
      const key = existingY ?? Math.round(item.y);
      lineMap.set(key, [...(lineMap.get(key) ?? []), item]);
    }
    const lines = [...lineMap.entries()].map(([y, lineItems]) => {
      const sorted = lineItems.sort((a, b) => a.x - b.x);
      return {
        y,
        text: sorted.map((item) => item.text).join(' '),
        minX: Math.min(...sorted.map((item) => item.x)),
        maxX: Math.max(...sorted.map((item) => item.x)),
      };
    });
    if (!locationHint) {
      const locationLine = lines.find((line) =>
        /Locaci[oó]n:/i.test(line.text),
      );
      locationHint =
        cleanText(locationLine?.text.replace(/^.*?Locaci[oó]n:\s*/i, '')) ||
        null;
    }

    const dateRows = lines
      .map((line) => ({
        y: line.y,
        date: parseDateOnly(
          compactPdfValue(
            joinPdfCell(
              items.filter((item) => Math.abs(item.y - line.y) <= 3),
              75,
              145,
            ),
          ),
        ),
      }))
      .filter((item): item is { y: number; date: string } => Boolean(item.date))
      .sort((a, b) => b.y - a.y);
    const unitStarts = lines
      .map((line) => ({
        y: line.y,
        text: compactPdfValue(
          joinPdfCell(
            items.filter((item) => Math.abs(item.y - line.y) <= 2),
            0,
            75,
          ),
        ),
      }))
      .filter((line) => /^(?:UG[NS]?\d+|SISTEMA\b)/i.test(line.text))
      .sort((a, b) => b.y - a.y);

    for (let index = 0; index < dateRows.length; index += 1) {
      const dateRow = dateRows[index];
      const above =
        index === 0 ? dateRow.y + 22 : (dateRows[index - 1].y + dateRow.y) / 2;
      const below =
        index === dateRows.length - 1
          ? dateRow.y - 22
          : (dateRow.y + dateRows[index + 1].y) / 2;
      const band = items.filter((item) => item.y <= above && item.y >= below);
      const unitStart = unitStarts
        .filter((item) => item.y >= dateRow.y - 2 && item.y - dateRow.y <= 25)
        .sort((a, b) => a.y - b.y)[0];
      const nextUnitStart = unitStart
        ? unitStarts
            .filter((item) => item.y < unitStart.y)
            .sort((a, b) => b.y - a.y)[0]
        : null;
      const unitBand = unitStart
        ? items.filter(
            (item) =>
              item.y <= unitStart.y + 2 &&
              item.y > (nextUnitStart?.y ?? dateRow.y - 35) + 2,
          )
        : band;
      const cells = [
        compactPdfValue(joinPdfCell(unitBand, 0, 75)),
        dateRow.date,
        compactPdfValue(joinPdfCell(band, 145, 240)),
        compactPdfValue(joinPdfCell(band, 240, 330)),
        compactPdfValue(joinPdfCell(band, 330, 420)),
        compactPdfValue(joinPdfCell(band, 420, 510)),
        compactPdfValue(joinPdfCell(band, 510, Number.POSITIVE_INFINITY)),
      ];
      const indexes = {
        unit: 0,
        date: 1,
        maintenance: 2,
        maintenanceHours: 3,
        previousHorometer: 4,
        horometer: 5,
        permit: 6,
        valid: true,
      };
      const row = buildRow(
        cells,
        indexes,
        detectPdfSection(lines, dateRow.y) ?? locationHint,
        {
          format: 'PDF',
          page: pageNumber,
          y: Number(dateRow.y.toFixed(2)),
        },
      );
      if (row) rows.push(row);
    }
  }
  return {
    rows,
    warnings: [],
    metadata: {
      format: 'PDF',
      location_hint: locationHint,
      pages: document.numPages,
    },
  };
}

export async function parseMonthlyScheduleDocument(
  buffer: Buffer,
  fileName: string,
): Promise<MonthlyScheduleDocumentResult> {
  const extension = String(fileName || '')
    .toLowerCase()
    .match(/\.[^.]+$/)?.[0];
  if (extension === '.pdf') return parsePdf(buffer);
  if (extension === '.docx') return parseWord(buffer);
  if (['.xlsx', '.xls', '.csv'].includes(extension || ''))
    return parseExcel(buffer);
  if (extension === '.doc') {
    throw new BadRequestException(
      'El formato Word .doc no es compatible. Guarda el archivo como .docx e intenta nuevamente.',
    );
  }
  throw new BadRequestException(
    'Formato no compatible. Adjunta un archivo PDF, Word (.docx) o Excel (.xlsx, .xls, .csv).',
  );
}
