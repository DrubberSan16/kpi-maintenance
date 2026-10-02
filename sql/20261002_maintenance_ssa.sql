-- SSA en OT y planes. No modifica datos ni reclasifica registros existentes.
BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

ALTER TABLE kpi_process.tb_work_order
  DROP CONSTRAINT IF EXISTS ck_tb_work_order_maintenance_kind;
ALTER TABLE kpi_process.tb_work_order
  ADD CONSTRAINT ck_tb_work_order_maintenance_kind CHECK (
    maintenance_kind IN ('CORRECTIVO', 'PREVENTIVO', 'PREDICTIVO', 'CEBADO', 'SSA', 'INSPECCION', 'PROYECTO')
  );

ALTER TABLE kpi_maintenance.tb_plan_mantenimiento
  DROP CONSTRAINT IF EXISTS ck_tb_plan_tipo;
ALTER TABLE kpi_maintenance.tb_plan_mantenimiento
  ADD CONSTRAINT ck_tb_plan_tipo CHECK (
    UPPER(COALESCE(TRIM(tipo), '')) IN ('PREVENTIVO', 'CORRECTIVO', 'PREDICTIVO', 'CEBADO', 'SSA')
  );
COMMIT;
