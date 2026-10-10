import { EntityManager } from 'typeorm';

/** Stock registrado en origen que todavía espera aprobación en destino. */
export async function getPendingTransferStock(
  executor: Pick<EntityManager, 'query'>,
  bodegaId: string,
  productoId: string,
) {
  const rows = await executor.query(
    `SELECT cantidad_nuevo AS nuevo, cantidad_usado AS usado,
            cantidad_critico AS critico, cantidad_total AS total
       FROM kpi_inventory.v_transferencia_stock_pendiente
      WHERE bodega_id = $1 AND producto_id = $2`,
    [bodegaId, productoId],
  );
  const row = rows?.[0] ?? {};
  return {
    nuevo: Number(row.nuevo ?? 0), usado: Number(row.usado ?? 0),
    critico: Number(row.critico ?? 0), total: Number(row.total ?? 0),
  };
}
