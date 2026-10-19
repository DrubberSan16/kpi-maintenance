export type LocationSnapshot = {
  location_id?: string | null;
  location_code?: string | null;
  location_name?: string | null;
  sucursal_id?: string | null;
  sucursal_code?: string | null;
  sucursal_name?: string | null;
  captured_at?: string;
  basis?: string;
};

/** A frozen empty location is still historical; never replace it with today's location. */
export function withWorkOrderLocation<T>(
  equipment: T,
  order?: { equipment_location_snapshot?: LocationSnapshot | null } | null,
): T {
  const snapshot = order?.equipment_location_snapshot;
  if (!equipment || !snapshot) return equipment;
  return {
    ...equipment,
    location_id: snapshot.location_id ?? null,
    equipment_location_label:
      [snapshot.location_code, snapshot.location_name, snapshot.sucursal_name]
        .filter(Boolean)
        .filter((v, i, a) => a.indexOf(v) === i)
        .join(' - ') || 'Sin ubicación registrada',
    location_nombre: snapshot.location_name ?? null,
    location_codigo: snapshot.location_code ?? null,
    sucursal_id: snapshot.sucursal_id ?? null,
    sucursal_nombre: snapshot.sucursal_name ?? null,
  };
}
