BEGIN;
SET LOCAL lock_timeout = '10s';
DROP TRIGGER IF EXISTS trg_operational_horometer ON kpi_maintenance.tb_equipo;
DROP TRIGGER IF EXISTS trg_freeze_closed_order_horometer ON kpi_process.tb_work_order;
DROP FUNCTION IF EXISTS kpi_maintenance.track_operational_horometer();
DROP FUNCTION IF EXISTS kpi_maintenance.freeze_closed_order_horometer();
-- Conservar la columna y todas las lecturas registradas; permite revertir el código.
COMMIT;
