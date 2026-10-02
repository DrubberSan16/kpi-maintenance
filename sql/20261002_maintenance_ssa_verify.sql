-- Prueba las restricciones usando filas existentes y revierte todo al terminar.
BEGIN;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';
DO $$
DECLARE
  order_id uuid;
  plan_id uuid;
BEGIN
  SELECT id INTO STRICT order_id FROM kpi_process.tb_work_order
    WHERE status_workflow IN ('PLANNED', 'IN_PROGRESS') AND NOT is_deleted
    ORDER BY created_at DESC LIMIT 1;
  SELECT id INTO STRICT plan_id FROM kpi_maintenance.tb_plan_mantenimiento
    WHERE NOT is_deleted ORDER BY created_at DESC LIMIT 1;

  UPDATE kpi_process.tb_work_order SET maintenance_kind = 'SSA' WHERE id = order_id;
  UPDATE kpi_maintenance.tb_plan_mantenimiento SET tipo = 'SSA' WHERE id = plan_id;
  IF NOT EXISTS (SELECT 1 FROM kpi_process.tb_work_order WHERE id = order_id AND maintenance_kind = 'SSA')
    OR NOT EXISTS (SELECT 1 FROM kpi_maintenance.tb_plan_mantenimiento WHERE id = plan_id AND tipo = 'SSA') THEN
    RAISE EXCEPTION 'SSA no se conserva en OT o plan.';
  END IF;

  BEGIN
    UPDATE kpi_process.tb_work_order SET maintenance_kind = 'TIPO_NO_VALIDO' WHERE id = order_id;
    RAISE EXCEPTION 'La OT admitio un tipo desconocido.';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  BEGIN
    UPDATE kpi_maintenance.tb_plan_mantenimiento SET tipo = 'TIPO_NO_VALIDO' WHERE id = plan_id;
    RAISE EXCEPTION 'El plan admitio un tipo desconocido.';
  EXCEPTION WHEN check_violation THEN NULL;
  END;
  RAISE NOTICE 'PASS: OT y plan aceptan SSA y rechazan tipos desconocidos. Todos los cambios se revierten.';
END $$;
ROLLBACK;
