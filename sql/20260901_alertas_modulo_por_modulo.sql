-- ---------------------------------------------------------------------------
-- Reestructuración de alertas por módulo.
--
--  * Añade los tipos del ciclo de vida de la orden de trabajo y el aviso de
--    "evento a realizar" del cronograma semanal.
--  * Regulariza PROGRAMACION_REPROGRAMADA, que el código ya emitía pero el
--    CHECK no admitía (se guardaba degradado como MANTENIMIENTO_PROXIMO).
--  * Cierra las alertas vivas de los generadores retirados sin borrar filas:
--    el histórico y su payload de entregas quedan intactos.
--
-- Idempotente: puede reejecutarse sin efectos secundarios.
-- ---------------------------------------------------------------------------

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'ck_tb_alerta_mant_tipo'
      AND conrelid = 'kpi_maintenance.tb_alerta_mantenimiento'::regclass
  ) THEN
    ALTER TABLE kpi_maintenance.tb_alerta_mantenimiento
      DROP CONSTRAINT ck_tb_alerta_mant_tipo;
  END IF;
END $$;

ALTER TABLE kpi_maintenance.tb_alerta_mantenimiento
  ADD CONSTRAINT ck_tb_alerta_mant_tipo
  CHECK (
    tipo_alerta IN (
      -- Vigentes: equipos con mantenimiento por tiempo y programaciones.
      'MANTENIMIENTO_VENCIDO',
      'MANTENIMIENTO_PROXIMO',
      'EVENTO_A_REALIZAR',
      'PROGRAMACION_REPROGRAMADA',
      -- Vigentes: ciclo de vida de la orden de trabajo.
      'ORDEN_TRABAJO_GENERADA',
      'ORDEN_TRABAJO_CONSUMOS',
      'ORDEN_TRABAJO_FINALIZADA',
      'ORDEN_TRABAJO_BLOQUEADA',
      'ORDEN_TRABAJO_DESBLOQUEADA',
      -- Retirados: se conservan para no invalidar el histórico existente.
      'OVERDUE',
      'MPG_325',
      'MPG_650',
      'MPG_975',
      'MPG_1300',
      'ANOMALIA_HOROMETRO',
      'REPORTE_DIARIO_VENCIDO',
      'REPORTE_DIARIO_PROXIMO',
      'LUBRICANTE_CRITICO',
      'LUBRICANTE_ALERTA',
      'COMBUSTIBLE_BAJO',
      'COMBUSTIBLE_PROXIMO_MINIMO',
      'SIN_STOCK',
      'STOCK_BAJO_BODEGA'
    )
  );

-- Cierre del histórico de generadores retirados. No se borra ninguna fila:
-- solo dejan de figurar como pendientes.
UPDATE kpi_maintenance.tb_alerta_mantenimiento
SET estado = 'CERRADA',
    nivel = 'INFO',
    resolved_at = COALESCE(resolved_at, now()),
    ultima_evaluacion_at = now(),
    payload_json = COALESCE(payload_json, '{}'::jsonb)
      || jsonb_build_object(
        'retired_at', to_jsonb(now()),
        'retired_reason', 'GENERADOR_DE_ALERTA_RETIRADO'
      ),
    updated_at = now()
WHERE COALESCE(is_deleted, false) = false
  AND estado IN ('ABIERTA', 'EN_PROCESO')
  AND (
    origen IN (
      'REPORTE_DIARIO',
      'ANALISIS_LUBRICANTE',
      'COMBUSTIBLE',
      'INVENTARIO',
      'BITACORA'
    )
    OR tipo_alerta IN (
      'ANOMALIA_HOROMETRO',
      'LUBRICANTE_CRITICO',
      'LUBRICANTE_ALERTA',
      'COMBUSTIBLE_BAJO',
      'COMBUSTIBLE_PROXIMO_MINIMO',
      'SIN_STOCK',
      'STOCK_BAJO_BODEGA',
      'REPORTE_DIARIO_VENCIDO',
      'REPORTE_DIARIO_PROXIMO',
      'OVERDUE',
      'MPG_325',
      'MPG_650',
      'MPG_975',
      'MPG_1300'
    )
  );

-- Índice de apoyo para el cierre de alertas de bloqueo por OT.
CREATE INDEX IF NOT EXISTS idx_tb_alerta_mantenimiento_work_order_estado
  ON kpi_maintenance.tb_alerta_mantenimiento (work_order_id, estado)
  WHERE is_deleted = false;

COMMIT;
