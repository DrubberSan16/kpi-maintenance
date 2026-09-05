-- ---------------------------------------------------------------------------
-- Recostea el material de órdenes de trabajo que quedó registrado en cero.
--
-- Antes del 2026-09-05 la salida de materiales de una OT se valorizaba con
-- `producto.ultimo_costo` a secas. Cuando ese campo estaba vacío la OT
-- consumía el repuesto a cero, aunque la bodega de la que salía sí tuviera
-- precio. El código ya aplica la estructura correcta (manda el costo de la
-- bodega, el del material es el respaldo); esto arregla lo ya registrado.
--
-- Solo toca las líneas que están en CERO. Una línea con un importe real se
-- registró con el precio que regía entonces y no se reescribe: no existe una
-- serie histórica de costos por bodega con la que rehacerla honestamente.
--
-- Las líneas en cero cuyo material sigue sin precio en ningún lado se quedan
-- como están: no hay de dónde sacarlo.
--
-- Deja rastro en kpi_inventory.tb_recosteo_ot_2026_09: qué valía cada línea
-- antes y después.
--
-- Idempotente: al reejecutarse ya no quedan líneas en cero recuperables.
-- ---------------------------------------------------------------------------

BEGIN;

-- ------------------------------------------------------------ 0. Rastro
CREATE TABLE IF NOT EXISTS kpi_inventory.tb_recosteo_ot_2026_09 (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tabla         text NOT NULL,
  fila_id       uuid NOT NULL,
  producto_id   uuid,
  bodega_id     uuid,
  cantidad      numeric(18,6),
  costo_antes   numeric(14,4),
  costo_despues numeric(14,4),
  ejecutado_at  timestamp without time zone NOT NULL DEFAULT now()
);

-- ------------------------------ 1. El precio que manda la estructura
-- Costo de la bodega; si no tiene, el del material (el primero que sea un
-- importe de verdad, no el primero no nulo).
CREATE TEMP TABLE tmp_precio ON COMMIT DROP AS
SELECT
  s.producto_id,
  s.bodega_id,
  CASE
    WHEN COALESCE(s.costo_promedio_bodega, 0) > 0 THEN s.costo_promedio_bodega::numeric
    WHEN COALESCE(p.costo_promedio, 0)       > 0 THEN p.costo_promedio::numeric
    ELSE COALESCE(p.ultimo_costo, 0)::numeric
  END AS precio
FROM kpi_inventory.tb_stock_bodega s
JOIN kpi_inventory.tb_producto p ON p.id = s.producto_id
WHERE COALESCE(s.is_deleted, false) = false;

CREATE INDEX ON tmp_precio (producto_id, bodega_id);

-- --------------------- 2. Detalle del egreso: la bodega la sabe el kardex
CREATE TEMP TABLE tmp_det ON COMMIT DROP AS
SELECT
  d.id            AS det_id,
  d.producto_id,
  k.bodega_id,
  d.cantidad::numeric AS cantidad,
  v.precio
FROM kpi_inventory.tb_movimiento_inventario_det d
JOIN kpi_inventory.tb_movimiento_inventario m ON m.id = d.movimiento_id
JOIN LATERAL (
  SELECT kx.bodega_id
  FROM kpi_inventory.tb_kardex kx
  WHERE kx.movimiento_det_id = d.id
  LIMIT 1
) k ON true
JOIN tmp_precio v
  ON v.producto_id = d.producto_id AND v.bodega_id = k.bodega_id
WHERE m.tipo_documento = 'EGRESO_BODEGA'
  AND m.work_order_id IS NOT NULL
  AND COALESCE(m.is_deleted, false) = false
  AND COALESCE(d.is_deleted, false) = false
  AND COALESCE(d.costo_unitario, 0) = 0
  AND v.precio > 0;

INSERT INTO kpi_inventory.tb_recosteo_ot_2026_09
  (tabla, fila_id, producto_id, bodega_id, cantidad, costo_antes, costo_despues)
SELECT 'tb_movimiento_inventario_det', det_id, producto_id, bodega_id, cantidad, 0, precio
FROM tmp_det;

UPDATE kpi_inventory.tb_movimiento_inventario_det AS d
SET costo_unitario = t.precio,
    subtotal_costo = round(t.cantidad * t.precio, 4),
    updated_at = now(),
    updated_by = 'RECOSTEO_OT'
FROM tmp_det AS t
WHERE d.id = t.det_id;

-- ------------------------------------- 3. El kardex de esas mismas líneas
UPDATE kpi_inventory.tb_kardex AS k
SET costo_unitario = t.precio,
    costo_total = round(COALESCE(k.salida_cantidad, 0)::numeric * t.precio, 4),
    saldo_costo_promedio = t.precio,
    saldo_valorizado = round(COALESCE(k.saldo_cantidad, 0)::numeric * t.precio, 4),
    updated_at = now(),
    updated_by = 'RECOSTEO_OT'
FROM tmp_det AS t
WHERE k.movimiento_det_id = t.det_id;

-- --------------------- 4. La cabecera vale lo que suma su detalle vigente
UPDATE kpi_inventory.tb_movimiento_inventario AS mov
SET total_costos = totales.total,
    updated_at = now(),
    updated_by = 'RECOSTEO_OT'
FROM (
  SELECT d.movimiento_id, COALESCE(SUM(d.subtotal_costo), 0) AS total
  FROM kpi_inventory.tb_movimiento_inventario_det d
  WHERE COALESCE(d.is_deleted, false) = false
    AND d.movimiento_id IN (
      SELECT DISTINCT d2.movimiento_id
      FROM kpi_inventory.tb_movimiento_inventario_det d2
      JOIN tmp_det t ON t.det_id = d2.id
    )
  GROUP BY d.movimiento_id
) AS totales
WHERE mov.id = totales.movimiento_id;

-- ------------------------ 5. La entrega que ve la OT en su pantalla
-- Aquí la bodega está en la propia línea.
CREATE TEMP TABLE tmp_entrega ON COMMIT DROP AS
SELECT ed.id AS det_id, ed.producto_id, ed.bodega_id,
       ed.cantidad::numeric AS cantidad, v.precio
FROM kpi_inventory.tb_entrega_material_det ed
JOIN kpi_inventory.tb_entrega_material e
  ON e.id = ed.entrega_id AND COALESCE(e.is_deleted, false) = false
JOIN tmp_precio v
  ON v.producto_id = ed.producto_id AND v.bodega_id = ed.bodega_id
WHERE COALESCE(ed.costo_unitario, 0) = 0
  AND v.precio > 0;

INSERT INTO kpi_inventory.tb_recosteo_ot_2026_09
  (tabla, fila_id, producto_id, bodega_id, cantidad, costo_antes, costo_despues)
SELECT 'tb_entrega_material_det', det_id, producto_id, bodega_id, cantidad, 0, precio
FROM tmp_entrega;

UPDATE kpi_inventory.tb_entrega_material_det AS ed
SET costo_unitario = t.precio
FROM tmp_entrega AS t
WHERE ed.id = t.det_id;

-- ---------------------------- 6. Los consumos reservados de la OT
CREATE TEMP TABLE tmp_consumo ON COMMIT DROP AS
SELECT c.id AS consumo_id, c.producto_id, c.bodega_id,
       c.cantidad::numeric AS cantidad, v.precio
FROM kpi_maintenance.tb_consumo_repuesto c
JOIN tmp_precio v
  ON v.producto_id = c.producto_id AND v.bodega_id = c.bodega_id
WHERE COALESCE(c.is_deleted, false) = false
  AND COALESCE(c.costo_unitario, 0) = 0
  AND v.precio > 0;

INSERT INTO kpi_inventory.tb_recosteo_ot_2026_09
  (tabla, fila_id, producto_id, bodega_id, cantidad, costo_antes, costo_despues)
SELECT 'tb_consumo_repuesto', consumo_id, producto_id, bodega_id, cantidad, 0, precio
FROM tmp_consumo;

UPDATE kpi_maintenance.tb_consumo_repuesto AS c
SET costo_unitario = t.precio,
    subtotal = round(t.cantidad * t.precio, 4)
FROM tmp_consumo AS t
WHERE c.id = t.consumo_id;

COMMIT;

-- ---------------------------------------------------------------------------
-- Verificación: no debe quedar ninguna línea en cero que tenga precio.
--
--   SELECT count(*) FROM kpi_inventory.tb_movimiento_inventario_det d
--     JOIN kpi_inventory.tb_movimiento_inventario m ON m.id = d.movimiento_id
--    WHERE m.tipo_documento='EGRESO_BODEGA' AND m.work_order_id IS NOT NULL
--      AND COALESCE(d.is_deleted,false)=false AND COALESCE(d.costo_unitario,0)=0;
--
--   SELECT tabla, count(*), round(sum(cantidad*costo_despues),2)
--     FROM kpi_inventory.tb_recosteo_ot_2026_09 GROUP BY tabla;
-- ---------------------------------------------------------------------------
