type EquipmentLike = object | null | undefined;

const EQUIPMENT_DESCRIPTION_FIELDS = [
  'codigo', 'nombre', 'nombre_real', 'descripcion', 'modelo', 'serie',
  'numero_serie', 'codigo_lubricante', 'grupo', 'grupo_nombre',
  'equipo_tipo', 'tipo_equipo', 'tipo', 'equipo_tipo_codigo',
  'equipo_tipo_nombre', 'equipo_tipo_descripcion', 'marca', 'marca_nombre',
  'location', 'ubicacion', 'location_nombre', 'location_codigo',
  'equipment_location_label', 'ubicacion_label', 'secciones', 'sections',
  'componentes', 'equipo_componentes', 'compartimientos', 'valor_json',
];

function containsSsa(value: unknown): boolean {
  if (typeof value === 'string') {
    const normalized = value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
    return /(^|[^A-Z])SSA([^A-Z]|$)/.test(normalized);
  }
  if (Array.isArray(value)) return value.some(containsSsa);
  if (value && typeof value === 'object') return Object.values(value).some(containsSsa);
  return false;
}

/** Busca la designación SSA en los datos del equipo, sin usar su auditoría. */
export function isSsaEquipment(equipment: EquipmentLike): boolean {
  if (!equipment) return false;
  const fields = equipment as Record<string, unknown>;
  if (typeof fields.is_ssa === 'boolean') return fields.is_ssa;
  return EQUIPMENT_DESCRIPTION_FIELDS.some(field => containsSsa(fields[field]));
}

export function operatorMaintenanceKindForEquipment(equipment: EquipmentLike): 'SSA' | 'CEBADO' {
  return isSsaEquipment(equipment) ? 'SSA' : 'CEBADO';
}
