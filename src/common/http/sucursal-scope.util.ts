export const SUCURSAL_SCOPE_HEADER = 'x-sucursal-id';

export function getSucursalScopeId(req?: {
  headers?: Record<string, string | string[] | undefined>;
}) {
  const raw = req?.headers?.[SUCURSAL_SCOPE_HEADER];
  if (Array.isArray(raw)) {
    return String(raw[0] || '').trim() || null;
  }
  return String(raw || '').trim() || null;
}
