import { readFileSync } from 'fs';
import { join } from 'path';
import * as XLSX from 'xlsx';
import { KpiMaintenanceService } from './kpi-maintenance.service';

/*
 * Informe de laboratorio por etiquetas (Lubrisa VEN-FOR-006A versión 3): la
 * columna "Parámetro" nombra las filas, cada "Muestra N" es una columna y los
 * criterios de evaluación van a la derecha, en las columnas CY a DB.
 */
const createService = () => {
  const service = Object.create(KpiMaintenanceService.prototype);
  service.equipoRepo = { find: jest.fn().mockResolvedValue([]) };
  service.marcaRepo = { find: jest.fn().mockResolvedValue([]) };
  service.analisisLubricanteRepo = { find: jest.fn().mockResolvedValue([]) };
  return service;
};

const selectedEquipment = {
  equipo: {
    id: 'equipo-1',
    codigo: 'UGS-09',
    nombre: 'Unidad 09',
    modelo: '3512',
  },
  marcaNombre: 'CATERPILLAR',
  equipoCodigo: 'UGS-09',
  equipoNombre: 'Unidad 09',
};

const buildSheet = (cells: Record<string, unknown>) => {
  const sheet = XLSX.utils.aoa_to_sheet([[]]);
  for (const [ref, value] of Object.entries(cells)) {
    XLSX.utils.sheet_add_aoa(sheet, [[value]], {
      origin: ref,
      cellDates: true,
    });
  }
  return sheet;
};

const reportHeader = (compartimento: string) => ({
  A1: 'REPORTE DE ANÁLISIS DE LUBRICANTE',
  G2: 'Asesor:',
  I2: 'ASESOR DE PRUEBA',
  B3: 'Cuenta Principal:',
  D3: 'CLIENTE DE PRUEBA',
  G3: 'Compartimento:',
  I3: compartimento,
  G4: 'Equipo:',
  I4: 'UGS - 09',
  B5: 'Nombre del cliente:',
  D5: 'CLIENTE DE PRUEBA',
  G5: 'Marca:',
  G6: 'Serie:',
  I6: 'SERIE-1',
  B7: 'Dirección:',
  G7: 'Modelo:',
  G8: 'Lubricante:',
  I8: '15W40',
  G9: 'Marca del lubricante:',
  I9: 'GULF',
  G10: 'Tipo de aceite (mineral / sintético):',
  I10: 'MINERAL',
  CY2: 'Número de solicitud:',
  DB2: 'PC-2026-09-21',
  CY3: 'Aplicación (Rangos Generales Lubrisa):',
  DB3: 'Motor a Diesel (Off Road)',
  A11: 'INFORMACIÓN DE LA MUESTRA',
  A12: 'Parámetro',
  B12: 'Muestra 1',
  C12: 'Muestra 2',
  D12: 'Muestra 3',
  CY12: 'Precaución',
  DA12: 'Anormal',
  DC12: 'Referencia',
  A13: 'Código de muestra',
  A14: 'Fecha de muestreo',
  A15: 'Fecha de ingreso al laboratorio',
  A16: 'Fecha de ejecución del ensayo',
  A17: 'Fecha de informe',
  A18: 'Horas / Km del equipo',
  A19: 'Horas / Km del aceite',
  A20: 'Relleno de aceite, galones',
  A21: 'Aceite cambiado (SI / NO)',
  A22: 'CONDICIÓN DE LA MUESTRA (automática)',
  A24: 'ESTADO DEL LUBRICANTE',
});

const buildLabReportWorkbook = () => {
  const workbook = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(
    workbook,
    buildSheet({
      A1: 'DATOS DEL LABORATORIO',
      A7: 'Etiqueta de la macro',
      B7: 'Muestra 1',
      A9: 'Cu',
      B9: 82,
    }),
    'DATOS',
  );

  const motor = buildSheet({
    ...reportHeader('MOTOR'),
    B13: '09-26-G-1',
    C13: '09-26-G-2',
    B14: new Date(2026, 7, 1),
    C14: new Date(2026, 8, 10),
    B15: new Date(2026, 7, 5),
    C15: new Date(2026, 8, 21),
    C16: new Date(2026, 8, 30),
    B17: new Date(2026, 7, 9),
    C17: new Date(2026, 8, 30),
    B18: 500,
    C18: 673,
    B19: 150,
    C19: 350,
    C20: 2,
    C21: 'Sí',
    B22: 'NORMAL',
    C22: 'ANORMAL',
    A25: 'Grado de viscosidad (SAE J300)',
    B25: 'DENTRO DE GRADO',
    C25: 'FUERA DE GRADO',
    CY25: 'Grado declarado:',
    DA25: 'SAE 15W-40 – SAE J300',
    A26: 'Viscosidad a 100 °C, cSt (ASTM D445) *',
    B26: 14.1,
    C26: 12.1,
    CY26: 'ingresar ficha técnica',
    DA26: 'fuera de grado: ≥ 12,5 y < 16,3',
    A27: 'Viscosidad a 40 °C, cSt (ASTM D445) *',
    B27: 95,
    C27: 73.3,
    CZ27: '--',
    DB27: '--',
    A29: 'T.B.N., mg KOH/g (ASTM D2896) *',
    B29: 11,
    C29: 10.2,
    CY29: '≤',
    CZ29: 'ficha téc.',
    DA29: '≤',
    DB29: 'ficha téc.',
    A34: 'Agua por crepitación (Método interno cualitativo)',
    B34: 'NEGATIVO',
    C34: 'NEGATIVO',
    CZ34: '--',
    DA34: 'POSITIVO',
    A36: 'Glicol (ASTM E2412 / cualitativo)',
    C36: 'POSITIVO',
    CZ36: '--',
    DA36: 'POSITIVO',
    A37: 'Dilución por combustible, % peso',
    B37: 0.5,
    C37: 2,
    CY37: '≥',
    CZ37: 1.5,
    DA37: '≥',
    DB37: 5,
    A39: 'DEGRADACIÓN QUÍMICA (FTIR, ASTM E2412)',
    A40: 'Oxidación, Abs/cm (ASTM E2412)',
    B40: 9,
    CY40: '≥',
    CZ40: 18,
    DA40: '≥',
    DB40: 22,
    A42: 'Sulfatación, Abs/cm (ASTM E2412)',
    B42: 20,
    C42: 25.2,
    CY42: '≥',
    CZ42: 25,
    DA42: '≥',
    DB42: 32,
    A43: 'Hollín, % peso (ASTM E2412)',
    B43: 0.5,
    C43: 0.82,
    CY43: '≥',
    CZ43: 2,
    DA43: '≥',
    DB43: 3,
    A51: 'DESGASTE DEL EQUIPO – ppm (mg/kg) (ASTM D6595)',
    A55: 'Cu (Cobre) *',
    B55: 10,
    C55: 82,
    CY55: '≥',
    CZ55: 31,
    DA55: '>',
    DB55: 50,
    A72: 'OBSERVACIONES / RECOMENDACIONES DEL ANALISTA',
    A73: 'Desgaste de cobre superior al habitual. Seguir con el monitoreo.',
    A78: 'ELABORADO POR:',
    A106: 'ZONA DE CÁLCULO – NO EDITAR (filas ocultas)',
    A184: 'gráfico ← Si (Silicio)',
    B184: 999,
    C184: 999,
  });
  // Un error de fórmula no es un resultado: SheetJS lo lee con su código (42).
  motor.C40 = { t: 'e', v: 0x2a, w: '#N/A' };
  XLSX.utils.book_append_sheet(workbook, motor, 'MOTOR OFF ROAD');

  // Hoja de la plantilla sin muestras: solo trae el valor por defecto del agua.
  XLSX.utils.book_append_sheet(
    workbook,
    buildSheet({
      ...reportHeader(''),
      A25: 'Grado de viscosidad (SAE J300)',
      A26: 'Viscosidad a 100 °C, cSt (ASTM D445) *',
      A27: 'Viscosidad a 40 °C, cSt (ASTM D445) *',
      A29: 'T.B.N., mg KOH/g (ASTM D2896) *',
      A34: 'Agua por crepitación (Método interno cualitativo)',
      B34: 'NEGATIVO',
    }),
    'MOTOR CARRETERA',
  );

  // Los hidráulicos ponen la viscosidad a 40 °C antes que la de 100 °C.
  XLSX.utils.book_append_sheet(
    workbook,
    buildSheet({
      ...reportHeader(''),
      B13: '09-26-H-7',
      B14: new Date(2026, 8, 12),
      B17: new Date(2026, 8, 30),
      B22: 'PRECAUCIÓN',
      A25: 'Grado de viscosidad (ISO 3448)',
      A26: 'Viscosidad a 40 °C, cSt (ASTM D445) *',
      B26: 46.2,
      A27: 'Viscosidad a 100 °C, cSt (ASTM D445) *',
      B27: 6.8,
      A33: 'Conteo de partículas (ISO 4406), (ASTM D7596)',
      B33: '21/19/16',
      CY33: '+1',
      CZ33: 'código',
      DA33: '+2',
      DB33: 'códigos',
      DC33: '23/20/17',
      A34: 'Agua por crepitación (Método interno cualitativo)',
      B34: 'NEGATIVO',
      A35: 'Contenido de agua, % (ASTM D6304)',
      B35: 0.4,
      CY35: '≥',
      CZ35: 0.3,
      DA35: '≥',
      DB35: 1,
    }),
    'HIDRÁULICO-INDUSTRIAL',
  );

  return XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' }) as Buffer;
};

const detailOf = (analysis: any, parametro: string) =>
  analysis.detalles.find((item: any) => item.parametro === parametro);

describe('Carga masiva de análisis de lubricante: informe por etiquetas', () => {
  it('importa las muestras de cada hoja por sus etiquetas y omite las hojas vacías', async () => {
    const service = createService();
    const parsed = await service.parseLubricantWorkbook(
      buildLabReportWorkbook(),
      'JUSTICE_COMPANY - UGS - 09 - MOTOR - SPV.1_1.xlsx',
      selectedEquipment,
    );

    expect(parsed.warnings).toEqual([]);
    expect(
      parsed.analyses.map(
        (item: any) => item.payload_json.sample_info.numero_muestra,
      ),
    ).toEqual(['09-26-G-1', '09-26-G-2', '09-26-H-7']);

    const [first, latest] = parsed.analyses;
    expect(first).toMatchObject({
      cliente: 'CLIENTE DE PRUEBA',
      equipo_id: 'equipo-1',
      lubricante: '15W40',
      marca_lubricante: 'GULF',
      compartimento_principal: 'MOTOR',
      fecha_muestra: '2026-08-01',
      fecha_reporte: '2026-08-09',
      estado_diagnostico: 'NORMAL',
      diagnostico: undefined,
      payload_json: {
        sample_info: {
          fecha_ingreso: '2026-08-05',
          fecha_informe: '2026-08-09',
          horas_equipo: 500,
          horas_lubricante: 150,
          equipo_serie: 'SERIE-1',
          equipo_modelo: '3512',
          hoja_origen: 'MOTOR OFF ROAD',
        },
        informe_laboratorio: {
          asesor: 'ASESOR DE PRUEBA',
          numero_solicitud: 'PC-2026-09-21',
          aplicacion: 'Motor a Diesel (Off Road)',
          tipo_aceite: 'MINERAL',
          observaciones_analista: null,
        },
      },
    });
    expect(latest).toMatchObject({
      fecha_muestra: '2026-09-10',
      estado_diagnostico: 'ANORMAL',
      diagnostico:
        'Desgaste de cobre superior al habitual. Seguir con el monitoreo.',
      payload_json: {
        informe_laboratorio: {
          fecha_ensayo: '2026-09-30',
          relleno_galones: 2,
          aceite_cambiado: 'SI',
          observaciones_analista:
            'Desgaste de cobre superior al habitual. Seguir con el monitoreo.',
        },
      },
    });
  });

  it('evalúa cada resultado con los criterios del propio informe', async () => {
    const service = createService();
    const parsed = await service.parseLubricantWorkbook(
      buildLabReportWorkbook(),
      'informe.xlsx',
      selectedEquipment,
    );
    const [first, latest] = parsed.analyses;
    const level = (analysis: any, parametro: string) =>
      detailOf(analysis, parametro)?.nivel_alerta;

    // Fuera del grado SAE J300 (≥ 12,5 y < 16,3) es anormal.
    expect(level(first, 'Viscosidad a 100ºC, cSt')).toBe('NORMAL');
    expect(level(latest, 'Viscosidad a 100ºC, cSt')).toBe('ANORMAL');
    expect(level(latest, 'Grado de viscosidad')).toBe('ANORMAL');
    // El laboratorio declara la viscosidad a 40 °C informativa: no hereda la
    // banda de la de 100 °C, que la daría por anormal.
    expect(level(latest, 'Viscosidad a 40ºC, cSt')).toBe('NORMAL');
    // Hollín 0,82: normal con el criterio del laboratorio (≥ 2) aunque el
    // umbral fijo del sistema (≥ 0,75) diría precaución.
    expect(level(latest, 'Hollín, wt%')).toBe('NORMAL');
    expect(level(latest, 'Sulfatación, Abs/cm')).toBe('PRECAUCION');
    expect(level(latest, 'Cu (Cobre)')).toBe('ANORMAL');
    expect(level(first, 'Cu (Cobre)')).toBe('NORMAL');
    expect(level(latest, 'Glicol (cualitativo)')).toBe('ANORMAL');
    expect(level(latest, 'Dilución por combustible, % peso')).toBe(
      'PRECAUCION',
    );
    expect(level(latest, 'Humedad')).toBe('NORMAL');

    expect(detailOf(latest, 'Glicol (cualitativo)').resultado_texto).toBe(
      'POSITIVO',
    );
    expect(detailOf(first, 'Glicol (cualitativo)')).toBeUndefined();
    // El #N/A de la oxidación no se importa como 42.
    expect(detailOf(first, 'Oxidación, Abs/cm').resultado_numerico).toBe(9);
    expect(detailOf(latest, 'Oxidación, Abs/cm')).toBeUndefined();
    // La zona de cálculo oculta repite etiquetas ("gráfico ← Si (Silicio)").
    expect(detailOf(latest, 'Si (Silicio)')).toBeUndefined();
    expect(latest.payload_json.rangos_generales.VISCOSIDAD_100C).toMatchObject({
      alert: {
        expression: 'OUTSIDE_RANGE',
        min: 12.5,
        max: 16.3,
        max_exclusive: true,
      },
      authoritative: true,
    });
  });

  it('ubica cada parámetro por su etiqueta aunque la hoja cambie el orden de las filas', async () => {
    const service = createService();
    const parsed = await service.parseLubricantWorkbook(
      buildLabReportWorkbook(),
      'informe.xlsx',
      selectedEquipment,
    );
    const hydraulic = parsed.analyses[2];

    expect(hydraulic).toMatchObject({
      compartimento_principal: 'HIDRAULICO',
      estado_diagnostico: 'PRECAUCION',
      fecha_muestra: '2026-09-12',
    });
    expect(
      detailOf(hydraulic, 'Viscosidad a 40ºC, cSt').resultado_numerico,
    ).toBe(46.2);
    expect(
      detailOf(hydraulic, 'Viscosidad a 100ºC, cSt').resultado_numerico,
    ).toBe(6.8);
    expect(
      detailOf(hydraulic, 'Conteo de partículas (ISO 4406)'),
    ).toMatchObject({
      resultado_texto: '21/19/16',
      nivel_alerta: 'NORMAL',
    });
    expect(detailOf(hydraulic, 'Contenido de agua, %')).toMatchObject({
      resultado_numerico: 0.4,
      nivel_alerta: 'PRECAUCION',
    });
  });

  it('mantiene los criterios del laboratorio al guardar el análisis', async () => {
    const service = createService();
    const parsed = await service.parseLubricantWorkbook(
      buildLabReportWorkbook(),
      'informe.xlsx',
      selectedEquipment,
    );
    const latest = parsed.analyses[1];

    // Se guardan en el JSON y se vuelven a leer en cada consulta y edición.
    const prepared = await service.prepareAnalisisDetallesForSave({
      lubricante: latest.lubricante,
      compartimento: latest.compartimento_principal,
      equipo_id: latest.equipo_id,
      thresholds: JSON.parse(
        JSON.stringify(latest.payload_json.rangos_generales),
      ),
      detalles: latest.detalles,
    });
    const level = (parametro: string) =>
      prepared.find((item: any) => item.parametro === parametro)?.nivel_alerta;

    expect(level('Viscosidad a 100ºC, cSt')).toBe('ANORMAL');
    expect(level('Viscosidad a 40ºC, cSt')).toBe('NORMAL');
    expect(level('Hollín, wt%')).toBe('NORMAL');
    expect(level('Sulfatación, Abs/cm')).toBe('PRECAUCION');
    expect(level('Cu (Cobre)')).toBe('ANORMAL');
    expect(level('Glicol (cualitativo)')).toBe('ANORMAL');
    expect(service.inferAnalisisStateFromDetails(prepared, 'ANORMAL')).toBe(
      'ANORMAL',
    );
  });

  it('conserva las reglas por defecto cuando el criterio no viene del informe', () => {
    const service = createService();
    const thresholds = service.normalizeLubricantThresholdMap({
      HOLLIN: { warning: { raw: '≥ 2', operator: '>=', value: 2 } },
    });

    expect(thresholds.get('HOLLIN').authoritative).toBeUndefined();
    expect(
      service.evaluateLubricantDetailLevel(
        { parametro: 'Hollín, wt%', resultado_numerico: 0.82 },
        null,
        thresholds.get('HOLLIN'),
      ),
    ).toBe('PRECAUCION');
  });

  it('sigue aceptando el formato de carga anterior', async () => {
    const service = createService();
    const template = readFileSync(
      join(
        __dirname,
        '../../../../templates/analisis-lubricante/FORMATO_CARGA_ANALISIS_LUBRICANTE.xlsx',
      ),
    );
    const parsed = await service.parseLubricantWorkbook(
      template,
      'FORMATO_CARGA_ANALISIS_LUBRICANTE.xlsx',
      selectedEquipment,
    );

    expect(parsed.analyses.length).toBeGreaterThan(0);
    expect(parsed.analyses[0]).toMatchObject({
      compartimento_principal: 'MOTOR',
      fecha_muestra: '2026-03-28',
      payload_json: { sample_info: { numero_muestra: 'MUESTRA-001' } },
    });
    expect(parsed.analyses[0].payload_json.informe_laboratorio).toBeUndefined();
  });

  it('rechaza un libro que no trae ninguno de los dos formatos', async () => {
    const service = createService();
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(
      workbook,
      buildSheet({ A1: 'Otra cosa' }),
      'Hoja1',
    );

    await expect(
      service.parseLubricantWorkbook(
        XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' }),
        'otro.xlsx',
        selectedEquipment,
      ),
    ).rejects.toThrow(
      'El archivo no contiene hojas válidas de análisis de lubricante.',
    );
  });
});

/*
 * El formulario (front-justice, `lubricantParameterTemplates`) envía como
 * `parametro` "Viscosidad a 100C, cSt", sin el "º" de la etiqueta del backend.
 */
describe('Análisis de lubricante: etiquetas del formulario', () => {
  it('reconoce las viscosidades con la etiqueta que envía el formulario', () => {
    const service = createService();

    expect(
      service.getLubricantMetricDefinition('Viscosidad a 100C, cSt')?.key,
    ).toBe('VISCOSIDAD_100C');
    expect(
      service.getLubricantMetricDefinition('Viscosidad a 40C, cSt')?.key,
    ).toBe('VISCOSIDAD_40C');
  });

  it('guarda la viscosidad del formulario con su etiqueta, su unidad y sus reglas', async () => {
    const service = createService();
    const [detalle] = await service.prepareAnalisisDetallesForSave({
      lubricante: '15W40',
      compartimento: 'MOTOR',
      detalles: [
        {
          compartimento: 'MOTOR',
          parametro: 'Viscosidad a 100C, cSt',
          orden: 1,
          resultado_numerico: 12,
          resultado_texto: null,
          linea_base: 14,
        },
      ],
    });

    // 12 es un 14 % menos que la línea base: precaución por el multiplicador
    // de viscosidad (0,9). Sin definición la caída no se marcaba.
    expect(detalle).toMatchObject({
      parametro: 'Viscosidad a 100ºC, cSt',
      unidad: 'cSt',
      nivel_alerta: 'PRECAUCION',
    });
  });
});
