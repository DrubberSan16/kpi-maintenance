-- Requiere desplegar tambien la version anterior de la aplicacion.
-- Si existen datos SSA, se detiene y conserva todos los registros.
BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM kpi_process.tb_work_order WHERE maintenance_kind = 'SSA')
     OR EXISTS (SELECT 1 FROM kpi_maintenance.tb_plan_mantenimiento WHERE UPPER(TRIM(tipo)) = 'SSA')
     OR EXISTS (SELECT 1 FROM kpi_maintenance.tb_procedimiento_plantilla
       WHERE UPPER(TRIM(tipo_proceso)) = 'SSA' OR UPPER(TRIM(clase_mantenimiento)) = 'SSA') THEN
    RAISE EXCEPTION 'Existen registros SSA. No se puede retirar el tipo sin resolverlos primero.';
  END IF;
END $$;

ALTER TABLE kpi_process.tb_work_order
  DROP CONSTRAINT IF EXISTS ck_tb_work_order_maintenance_kind;
ALTER TABLE kpi_process.tb_work_order
  ADD CONSTRAINT ck_tb_work_order_maintenance_kind CHECK (
    maintenance_kind IN ('CORRECTIVO', 'PREVENTIVO', 'PREDICTIVO', 'CEBADO', 'INSPECCION', 'PROYECTO')
  );

ALTER TABLE kpi_maintenance.tb_plan_mantenimiento
  DROP CONSTRAINT IF EXISTS ck_tb_plan_tipo;
ALTER TABLE kpi_maintenance.tb_plan_mantenimiento
  ADD CONSTRAINT ck_tb_plan_tipo CHECK (
    UPPER(COALESCE(TRIM(tipo), '')) IN ('PREVENTIVO', 'CORRECTIVO', 'PREDICTIVO', 'CEBADO')
  );
COMMIT;
