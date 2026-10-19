BEGIN;
-- Existing manufacturer serials in the header stay in numero_serie.
ALTER TABLE kpi_maintenance.tb_equipo ADD COLUMN IF NOT EXISTS numero_serie text;
ALTER TABLE kpi_maintenance.tb_equipo_componente ADD COLUMN IF NOT EXISTS serie text;
COMMIT;
