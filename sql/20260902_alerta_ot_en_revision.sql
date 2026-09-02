-- ---------------------------------------------------------------------------
-- Tipo de alerta para la orden que pasa a revision.
--
-- Al entrar en revision se avisa a supervision con el consumo de aceite de esa
-- orden y su nivel (normal, seguimiento o critico). La alerta queda registrada
-- para poder auditar a quien se notifico y con que cifra.
--
-- Idempotente.
-- ---------------------------------------------------------------------------

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
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
      'MANTENIMIENTO_VENCIDO',
      'MANTENIMIENTO_PROXIMO',
      'EVENTO_A_REALIZAR',
      'PROGRAMACION_REPROGRAMADA',
      'ORDEN_TRABAJO_GENERADA',
      'ORDEN_TRABAJO_CONSUMOS',
      'ORDEN_TRABAJO_FINALIZADA',
      'ORDEN_TRABAJO_BLOQUEADA',
      'ORDEN_TRABAJO_DESBLOQUEADA',
      'ORDEN_TRABAJO_EN_REVISION',
      -- Retirados: se conservan para no invalidar el historico.
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

COMMIT;
