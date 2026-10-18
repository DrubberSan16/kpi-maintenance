/**
 * Tiempo de ejecución de la OT, descontando todos los intervalos en revisión.
 *
 * El inicio sale del registro de inicio de ejecución o de la primera entrada a
 * EN PROCESO, y el cierre de la última entrada a CERRADA. `started_at` y
 * `closed_at` solo son respaldo: guardar la cabecera después de cerrar los
 * reescribe con la hora de ese guardado (quedan iguales) y casi todos los
 * cebados salían con cero horas.
 */
export function workOrderWorkedHours(
  order: Record<string, any>, history: Record<string, any>[] = [], now = Date.now(),
): number | null {
  const timestamp = (value: unknown) => {
    if (value == null || value === '') return null;
    const text = String(value);
    const date = value instanceof Date ? value : new Date(
      /^\d{4}-\d\d-\d\d[T ]\d\d:\d\d(?::\d\d(?:\.\d+)?)?$/.test(text)
        ? `${text.replace(' ', 'T')}-05:00` : text,
    );
    return Number.isFinite(date.getTime()) ? date.getTime() : null;
  };
  const events = history.map((row, index): Record<string, any> & { at: number | null; index: number } => ({ ...row, at: timestamp(row.changed_at), index }))
    .filter(row => row.at != null).sort((a, b) => a.at! - b.at! || a.index - b.index);
  const startedAt = timestamp(order.started_at);
  const closedAt = timestamp(order.closed_at);
  // Iguales: un guardado posterior al cierre pisó los dos y no dicen nada.
  const reliableStartedAt = startedAt != null && startedAt !== closedAt ? startedAt : null;
  const firstStart = events.find(row => row.to_status === 'IN_PROGRESS' && row.from_status !== 'IN_PROGRESS');
  // Gana el más temprano: las reescrituras solo atrasan la hora, y si al
  // historial le falta la entrada inicial, `firstStart` es una reanudación.
  const fallbackStarts = [firstStart?.at, reliableStartedAt].filter((value): value is number => value != null);
  const start = timestamp(order.valor_json?.execution_start?.at)
    ?? (fallbackStarts.length ? Math.min(...fallbackStarts) : null);
  if (start == null) return null;
  // CERRADA -> CERRADA es una edición posterior, no un cierre.
  const closings = events.filter(row => row.to_status === 'CLOSED' && row.from_status !== 'CLOSED' && row.at! >= start);
  const closed = closings[closings.length - 1]?.at ?? closedAt;
  const end = Math.max(start, Math.min(now, closed ?? now));
  let cursor = start;
  let state = 'IN_PROGRESS';
  let elapsed = 0;
  for (const event of events) {
    if (event.at! < start) continue;
    if (event.at! > end) break;
    if (state !== 'REVIEW' && state !== 'CLOSED') elapsed += event.at! - cursor;
    cursor = event.at!;
    state = String(event.to_status || state).toUpperCase();
  }
  if (state !== 'REVIEW' && state !== 'CLOSED') elapsed += end - cursor;
  // Seis decimales (0,0036 s): los cebados se muestran con segundos.
  return Number((Math.max(0, elapsed) / 3600000).toFixed(6));
}
