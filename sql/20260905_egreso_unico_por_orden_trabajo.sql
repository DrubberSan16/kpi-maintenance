-- ---------------------------------------------------------------------------
-- Un solo egreso de bodega (EB) por orden de trabajo.
--
-- Hasta ahora cada "registrar salida real" abría su propio EB, así que una OT
-- que sacaba material en varias tandas terminaba con varios documentos. La
-- regla de negocio es uno por orden: bodega firma un documento que crece.
--
-- Esta migración consolida lo ya existente. Por cada OT sobrevive el egreso más
-- antiguo (el que abrió el documento) y absorbe el detalle y el kardex de los
-- demás; los absorbidos quedan fuera de circulación conservando su número, que
-- ya fue emitido y no debe reutilizarse.
--
-- El código nuevo (resolveWorkOrderIssueMovement en kpi-maintenance) ya no abre
-- un EB por salida: reutiliza el de la orden. Sin esta migración el histórico
-- seguiría partido; sin el código, la migración volvería a desordenarse.
--
-- Deja rastro en kpi_inventory.tb_consolidacion_egreso_ot: qué se movió, hacia
-- dónde y cuánto, para poder auditar o revertir.
--
-- Idempotente: al reejecutarse ya no encuentra duplicados activos.
-- ---------------------------------------------------------------------------

BEGIN;

-- ------------------------------------------------------------ 0. Rastro
CREATE TABLE IF NOT EXISTS kpi_inventory.tb_consolidacion_egreso_ot (
  id                      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  work_order_id           uuid NOT NULL,
  movimiento_destino_id   uuid NOT NULL,
  numero_documento_destino varchar(60),
  movimiento_origen_id    uuid NOT NULL,
  numero_documento_origen varchar(60),
  detalles_movidos        integer NOT NULL DEFAULT 0,
  kardex_movidos          integer NOT NULL DEFAULT 0,
  ejecutado_at            timestamp without time zone NOT NULL DEFAULT now()
);

-- --------------------------------------- 1. Qué absorbe a qué, por OT
-- El superviviente es el más antiguo: es el que abrió el documento y el que
-- lleva el número que bodega ya conoce.
--
-- Quedan fuera los egresos que pertenecen a una transferencia de bodega: el
-- traslado a chatarra de una OT abre su propio egreso, que también lleva el
-- work_order_id pero es otro documento y se anula con la transferencia.
CREATE TEMP TABLE tmp_eb_consolidacion ON COMMIT DROP AS
WITH activos AS (
  SELECT
    id,
    work_order_id,
    numero_documento,
    ROW_NUMBER() OVER (
      PARTITION BY work_order_id
      ORDER BY fecha_movimiento ASC, created_at ASC, id ASC
    ) AS orden
  FROM kpi_inventory.tb_movimiento_inventario AS mov
  WHERE tipo_documento = 'EGRESO_BODEGA'
    AND work_order_id IS NOT NULL
    AND COALESCE(is_deleted, false) = false
    AND NOT EXISTS (
      SELECT 1
      FROM kpi_inventory.tb_transferencia_bodega AS tra
      WHERE tra.movimiento_salida_id = mov.id
         OR tra.movimiento_ingreso_id = mov.id
    )
)
SELECT
  origen.id               AS origen_id,
  origen.numero_documento AS origen_numero,
  origen.work_order_id    AS work_order_id,
  destino.id              AS destino_id,
  destino.numero_documento AS destino_numero
FROM activos AS origen
JOIN activos AS destino
  ON destino.work_order_id = origen.work_order_id
 AND destino.orden = 1
WHERE origen.orden > 1;

INSERT INTO kpi_inventory.tb_consolidacion_egreso_ot (
  work_order_id, movimiento_destino_id, numero_documento_destino,
  movimiento_origen_id, numero_documento_origen,
  detalles_movidos, kardex_movidos
)
SELECT
  mapa.work_order_id,
  mapa.destino_id,
  mapa.destino_numero,
  mapa.origen_id,
  mapa.origen_numero,
  (SELECT count(*) FROM kpi_inventory.tb_movimiento_inventario_det det
    WHERE det.movimiento_id = mapa.origen_id),
  (SELECT count(*) FROM kpi_inventory.tb_kardex kdx
    WHERE kdx.movimiento_id = mapa.origen_id)
FROM tmp_eb_consolidacion AS mapa;

-- ------------------------------------- 2. El detalle cambia de documento
UPDATE kpi_inventory.tb_movimiento_inventario_det AS det
SET movimiento_id = mapa.destino_id,
    updated_at = now(),
    updated_by = 'CONSOLIDACION_EB'
FROM tmp_eb_consolidacion AS mapa
WHERE det.movimiento_id = mapa.origen_id;

-- El kardex apunta al documento por movimiento_id; movimiento_det_id no cambia
-- porque el detalle conserva su fila, solo cambia de padre.
UPDATE kpi_inventory.tb_kardex AS kdx
SET movimiento_id = mapa.destino_id,
    updated_at = now(),
    updated_by = 'CONSOLIDACION_EB'
FROM tmp_eb_consolidacion AS mapa
WHERE kdx.movimiento_id = mapa.origen_id;

-- --------------------------- 3. El absorbido sale de circulación vacío
UPDATE kpi_inventory.tb_movimiento_inventario AS mov
SET is_deleted = true,
    status = 'INACTIVE',
    deleted_at = now(),
    deleted_by = 'CONSOLIDACION_EB',
    updated_at = now(),
    updated_by = 'CONSOLIDACION_EB',
    observacion = COALESCE(mov.observacion, '')
                  || ' [Consolidado en ' || COALESCE(mapa.destino_numero, 'EB') || ']'
FROM tmp_eb_consolidacion AS mapa
WHERE mov.id = mapa.origen_id;

-- -------------------- 4. El superviviente recoge todo lo que absorbió
UPDATE kpi_inventory.tb_movimiento_inventario AS mov
SET observacion = COALESCE(mov.observacion, '') || absorbidos.marca,
    updated_at = now(),
    updated_by = 'CONSOLIDACION_EB'
FROM (
  SELECT
    destino_id,
    ' [Incluye ' || string_agg(origen_numero, ', ' ORDER BY origen_numero) || ']' AS marca
  FROM tmp_eb_consolidacion
  GROUP BY destino_id
) AS absorbidos
WHERE mov.id = absorbidos.destino_id;

-- El total del documento es el de todo su detalle vigente.
UPDATE kpi_inventory.tb_movimiento_inventario AS mov
SET total_costos = totales.total,
    updated_at = now(),
    updated_by = 'CONSOLIDACION_EB'
FROM (
  SELECT det.movimiento_id, COALESCE(SUM(det.subtotal_costo), 0) AS total
  FROM kpi_inventory.tb_movimiento_inventario_det AS det
  WHERE COALESCE(det.is_deleted, false) = false
  GROUP BY det.movimiento_id
) AS totales
WHERE mov.id = totales.movimiento_id
  AND mov.id IN (SELECT DISTINCT destino_id FROM tmp_eb_consolidacion);

-- Una bodega única solo se sostiene si todo el egreso salió de la misma; en
-- cuanto el documento mezcla bodegas, la resuelve el kardex línea a línea.
UPDATE kpi_inventory.tb_movimiento_inventario AS mov
SET bodega_origen_id = bodegas.bodega_id,
    updated_at = now(),
    updated_by = 'CONSOLIDACION_EB'
FROM (
  SELECT
    kdx.movimiento_id,
    CASE WHEN count(DISTINCT kdx.bodega_id) = 1
         THEN min(kdx.bodega_id::text)::uuid
         ELSE NULL
    END AS bodega_id
  FROM kpi_inventory.tb_kardex AS kdx
  WHERE COALESCE(kdx.is_deleted, false) = false
  GROUP BY kdx.movimiento_id
) AS bodegas
WHERE mov.id = bodegas.movimiento_id
  AND mov.id IN (SELECT DISTINCT destino_id FROM tmp_eb_consolidacion);

COMMIT;

-- ---------------------------------------------------------------------------
-- Verificación: ninguna OT debe quedar con más de un egreso activo.
--
--   SELECT work_order_id, count(*) AS egresos
--     FROM kpi_inventory.tb_movimiento_inventario AS mov
--    WHERE tipo_documento = 'EGRESO_BODEGA'
--      AND work_order_id IS NOT NULL
--      AND COALESCE(is_deleted, false) = false
--      AND NOT EXISTS (
--            SELECT 1 FROM kpi_inventory.tb_transferencia_bodega AS tra
--             WHERE tra.movimiento_salida_id = mov.id
--                OR tra.movimiento_ingreso_id = mov.id)
--    GROUP BY work_order_id
--   HAVING count(*) > 1;
-- ---------------------------------------------------------------------------
