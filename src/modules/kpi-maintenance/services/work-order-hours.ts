/** Tiempo de ejecución de la OT, descontando todos los intervalos en revisión. */
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
  const firstStart = events.find(row => row.to_status === 'IN_PROGRESS' && row.from_status !== 'IN_PROGRESS');
  const start = timestamp(order.started_at || order.valor_json?.execution_start?.at) ?? firstStart?.at;
  if (start == null) return null;
  const closed = timestamp(order.closed_at) ?? events.find(row => row.to_status === 'CLOSED' && row.at! >= start)?.at;
  const end = Math.max(start, Math.min(now, closed ?? now));
  let cursor = start;
  let state = 'IN_PROGRESS';
  let elapsed = 0;
  for (const event of events) {
    if (event.at! < start) continue;
    if (event.at! > end) break;
    if (state !== 'REVIEW') elapsed += event.at! - cursor;
    cursor = event.at!;
    state = String(event.to_status || state).toUpperCase();
  }
  if (state !== 'REVIEW') elapsed += end - cursor;
  return Number((Math.max(0, elapsed) / 3600000).toFixed(4));
}
