-- Clase independiente de emergencia. Las OT existentes conservan Normal.
-- No modifica documentos, cantidades, Kardex ni costos históricos.
BEGIN;
ALTER TABLE kpi_process.tb_work_order
  ADD COLUMN IF NOT EXISTS is_overhaul boolean NOT NULL DEFAULT false;
COMMIT;
