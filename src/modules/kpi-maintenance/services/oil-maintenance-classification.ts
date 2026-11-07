/** Clasificación del informe; conserva el tipo original de las OT históricas. */
export function oilMaintenanceClassification(row: Record<string, any>) {
  const original = String(row.maintenance_kind || '').trim().toUpperCase();
  const names = [row.procedure_name, row.plan_name, row.work_order_title].filter(Boolean).join(' ');
  const explicitMpg = /\bMPG\b/i.test(names);
  const isMpg = !['CEBADO', 'PROYECTO'].includes(original)
    && (['PREVENTIVO', 'MPG'].includes(original) || explicitMpg);
  // Las plantillas antiguas pueden tener frecuencia de programación (6 h)
  // distinta del intervalo del mantenimiento identificado en su nombre (500 h).
  const namedHours = names.match(/\bMPG\s*[-:]?\s*(\d+(?:[.,]\d+)?)\s*(?:HRS?\b|HORAS?\b)/i);
  const procedureHours = Number(row.procedure_hours);
  const hours = namedHours ? Number(namedHours[1].replace(',', '.'))
    : procedureHours > 0 ? procedureHours
    : String(row.plan_frequency_type).toUpperCase() === 'HORAS' ? Number(row.plan_frequency) : 0;
  return {
    maintenance_kind_original: original,
    maintenance_kind: isMpg ? 'PREVENTIVO' : original,
    mpg_interval_hours: isMpg && Number.isFinite(hours) && hours > 0 ? hours : null,
  };
}
