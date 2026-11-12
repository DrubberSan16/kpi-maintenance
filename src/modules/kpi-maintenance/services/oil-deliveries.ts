import { aceiteEnGalonesSql } from './material-cost-category';

/**
 * Aceite que salió de bodega para cada OT, asiento por asiento del kardex.
 *
 * Todos los indicadores de cebado y aceite cuentan esto y no lo que se tecleó
 * en los materiales de la OT, por dos razones que reclamó bodega:
 *  - La fecha es la de la salida (EB). Las OT se registran a veces semanas
 *    después del trabajo con `hora_inicio` retroactiva, y con esa fecha el
 *    aceite caía en agosto mientras el kardex lo descargaba en septiembre.
 *  - La cantidad es la entregada. Hay OT con la línea de aceite duplicada o
 *    sin egreso de bodega, y sumar los materiales de la OT no cuadraba.
 *
 * Una entrada ligada a la OT es aceite devuelto y resta. Las salidas anuladas
 * quedan con el asiento borrado (`is_deleted`), así que no cuentan.
 */
export const ACEITE_ENTREGADO_SQL = `
  SELECT mi.work_order_id, k.fecha, k.producto_id, k.bodega_id,
    p.codigo AS material_codigo, CONCAT_WS(' - ', p.codigo, p.nombre) AS material_label,
    CONCAT_WS(' - ', b.codigo, b.nombre) AS bodega_label,
    COALESCE(k.salida_cantidad, 0) - COALESCE(k.entrada_cantidad, 0) AS galones,
    CASE WHEN COALESCE(k.entrada_cantidad, 0) > 0
      THEN -COALESCE(k.costo_total, 0) ELSE COALESCE(k.costo_total, 0) END AS costo
  FROM kpi_inventory.tb_kardex k
  JOIN kpi_inventory.tb_movimiento_inventario mi
    ON mi.id = k.movimiento_id AND mi.work_order_id IS NOT NULL
      AND NOT COALESCE(mi.is_deleted, false)
  JOIN kpi_inventory.tb_producto p ON p.id = k.producto_id AND ${aceiteEnGalonesSql('p')}
  LEFT JOIN kpi_inventory.tb_bodega b ON b.id = k.bodega_id
  WHERE NOT COALESCE(k.is_deleted, false)`;

/** Agrega solo Kardex del período; los consumos digitados de aceite se omiten antes. */
export function addOilDeliveryCosts(orders: Map<string, any>, deliveries: any[]) {
  const round = (n: number) => Number(n.toFixed(4));
  for (const delivery of deliveries) {
    const row = orders.get(delivery.work_order_id);
    if (!row) continue;
    const quantity = Number(delivery.galones) || 0;
    const cost = Number(delivery.costo) || 0;
    row.costo_aceite = round(row.costo_aceite + cost);
    row.total_costo = round(row.total_costo + cost);
    row.total_cantidad = round(row.total_cantidad + quantity);
    row.total_items += 1;
    row._materiales.add(delivery.material_label);
    // Fecha local producida por SQL: no convertir el último día del mes a UTC.
    const date = String(delivery.fecha_referencia).slice(0, 10);
    const key = `aceite|${delivery.producto_id}|${delivery.bodega_id}|${date}`;
    const detail = row._detalle.get(key) ?? {
      producto_id: delivery.producto_id, material_codigo: delivery.material_codigo,
      material_label: delivery.material_label, bodega_label: delivery.bodega_label,
      categoria_costo: 'ACEITE', fecha_referencia: date, fuente: 'KARDEX',
      cantidad: 0, costo_total: 0, costo_unitario: 0,
    };
    detail.cantidad = round(detail.cantidad + quantity);
    detail.costo_total = round(detail.costo_total + cost);
    detail.costo_unitario = detail.cantidad ? round(detail.costo_total / detail.cantidad) : 0;
    row._detalle.set(key, detail);
  }
}

/** Distribución mensual de costos: aceite por Kardex, otras partidas por OT. */
export function splitOilCostPeriods(row: any): any[] {
  if (row.aceite_fuente !== 'KARDEX') return [row];
  const oil = (row.materiales_detalle ?? []).filter((item: any) => item.categoria_costo === 'ACEITE');
  const periods = new Map<string, any>();
  if (!row.solo_aceite_periodo) periods.set(row.period_key, {
    ...row, total_costo: Number(row.total_costo) - Number(row.costo_aceite),
    total_cantidad: Number(row.total_cantidad) - oil.reduce((sum: number, item: any) => sum + Number(item.cantidad), 0),
    total_items: Math.max(0, Number(row.total_items) - oil.length),
  });
  for (const item of oil) {
    const key = String(item.fecha_referencia).slice(0, 7);
    const period = periods.get(key) ?? { ...row, period_key: key,
      periodo: new Intl.DateTimeFormat('es-EC', { month: 'long', year: 'numeric', timeZone: 'UTC' }).format(new Date(`${key}-01T12:00:00Z`)),
      total_costo: 0, total_cantidad: 0, total_items: 0 };
    period.total_costo += Number(item.costo_total);
    period.total_cantidad += Number(item.cantidad);
    period.total_items += 1;
    periods.set(key, period);
  }
  return [...periods.values()];
}
