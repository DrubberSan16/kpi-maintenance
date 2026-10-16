BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

-- La migración no debe cambiar ningún dato operativo existente.
LOCK TABLE kpi_maintenance.tb_equipo IN ACCESS EXCLUSIVE MODE;
CREATE TEMP TABLE equipo_antes_reserva ON COMMIT DROP AS
  SELECT id, to_jsonb(e) - 'es_reserva' AS datos
  FROM kpi_maintenance.tb_equipo e;

ALTER TABLE kpi_maintenance.tb_equipo
  ADD COLUMN IF NOT EXISTS es_reserva boolean NOT NULL DEFAULT false;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'kpi_maintenance' AND table_name = 'tb_equipo'
      AND column_name = 'es_reserva' AND data_type = 'boolean'
      AND is_nullable = 'NO' AND column_default = 'false'
  ) THEN
    RAISE EXCEPTION 'es_reserva debe ser boolean NOT NULL DEFAULT false';
  END IF;
  IF EXISTS (
    SELECT 1 FROM equipo_antes_reserva a
    FULL JOIN kpi_maintenance.tb_equipo e ON e.id = a.id
    WHERE a.id IS NULL OR e.id IS NULL
      OR a.datos IS DISTINCT FROM (to_jsonb(e) - 'es_reserva')
  ) THEN
    RAISE EXCEPTION 'La migración alteró datos operativos del equipo';
  END IF;
END $$;

SELECT count(*) AS equipos, count(*) FILTER (WHERE es_reserva) AS en_reserva,
  count(*) FILTER (WHERE es_reserva IS NULL) AS reservas_nulas
FROM kpi_maintenance.tb_equipo;
COMMIT;
