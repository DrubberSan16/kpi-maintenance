BEGIN;
SET LOCAL lock_timeout = '10s';
CREATE TABLE IF NOT EXISTS kpi_maintenance.tb_equipo_ubicacion_historial (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  equipo_id uuid NOT NULL REFERENCES kpi_maintenance.tb_equipo(id) ON DELETE CASCADE,
  location_id uuid,
  location_codigo text, location_nombre text,
  sucursal_id uuid, sucursal_codigo text, sucursal_nombre text,
  valid_from timestamptz NOT NULL DEFAULT clock_timestamp(),
  valid_to timestamptz, recorded_by text, source text NOT NULL,
  CHECK (valid_to IS NULL OR valid_to >= valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_equipo_ubicacion_actual
  ON kpi_maintenance.tb_equipo_ubicacion_historial(equipo_id) WHERE valid_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_equipo_ubicacion_historial
  ON kpi_maintenance.tb_equipo_ubicacion_historial(equipo_id,valid_from DESC);
GRANT SELECT, INSERT, UPDATE, DELETE ON kpi_maintenance.tb_equipo_ubicacion_historial TO justice_app;
ALTER TABLE kpi_process.tb_work_order ADD COLUMN IF NOT EXISTS equipment_location_snapshot jsonb;

CREATE OR REPLACE FUNCTION kpi_maintenance.equipment_location_snapshot(p_equipo uuid)
RETURNS jsonb LANGUAGE sql VOLATILE AS $$
  SELECT jsonb_build_object('equipment_id',e.id,'location_id',e.location_id,
    'location_code',l.codigo,'location_name',l.nombre,
    'sucursal_id',l.sucursal_id,'sucursal_code',s.codigo,'sucursal_name',s.nombre)
  FROM kpi_maintenance.tb_equipo e
  LEFT JOIN kpi_maintenance.tb_location l ON l.id=e.location_id
  LEFT JOIN kpi_inventory.tb_sucursal s ON s.id=l.sucursal_id
  WHERE e.id=p_equipo
$$;

CREATE OR REPLACE FUNCTION kpi_maintenance.record_equipment_location(p_equipo uuid,p_actor text,p_source text)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE snap jsonb; prior kpi_maintenance.tb_equipo_ubicacion_historial%ROWTYPE; moment timestamptz:=clock_timestamp();
BEGIN
  PERFORM id FROM kpi_maintenance.tb_equipo WHERE id=p_equipo FOR UPDATE;
  snap:=kpi_maintenance.equipment_location_snapshot(p_equipo);
  SELECT * INTO prior FROM kpi_maintenance.tb_equipo_ubicacion_historial
    WHERE equipo_id=p_equipo AND valid_to IS NULL FOR UPDATE;
  IF FOUND AND prior.location_id IS NOT DISTINCT FROM (snap->>'location_id')::uuid
    AND prior.sucursal_id IS NOT DISTINCT FROM (snap->>'sucursal_id')::uuid
    AND prior.location_nombre IS NOT DISTINCT FROM snap->>'location_name'
    AND prior.sucursal_nombre IS NOT DISTINCT FROM snap->>'sucursal_name'
    AND prior.location_codigo IS NOT DISTINCT FROM snap->>'location_code'
    AND prior.sucursal_codigo IS NOT DISTINCT FROM snap->>'sucursal_code' THEN RETURN; END IF;
  UPDATE kpi_maintenance.tb_equipo_ubicacion_historial SET valid_to=moment WHERE equipo_id=p_equipo AND valid_to IS NULL;
  INSERT INTO kpi_maintenance.tb_equipo_ubicacion_historial
    (equipo_id,location_id,location_codigo,location_nombre,sucursal_id,sucursal_codigo,sucursal_nombre,valid_from,recorded_by,source)
  VALUES (p_equipo,(snap->>'location_id')::uuid,snap->>'location_code',snap->>'location_name',
    (snap->>'sucursal_id')::uuid,snap->>'sucursal_code',snap->>'sucursal_name',moment,p_actor,p_source);
END $$;

CREATE OR REPLACE FUNCTION kpi_maintenance.track_equipment_location()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='INSERT' THEN
    PERFORM kpi_maintenance.record_equipment_location(NEW.id,NEW.created_by,'EQUIPMENT_CREATED');
  ELSIF NEW.location_id IS DISTINCT FROM OLD.location_id THEN
    PERFORM kpi_maintenance.record_equipment_location(NEW.id,NEW.updated_by,'EQUIPMENT_MOVED');
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_equipment_location ON kpi_maintenance.tb_equipo;
CREATE TRIGGER trg_equipment_location AFTER INSERT OR UPDATE OF location_id ON kpi_maintenance.tb_equipo
  FOR EACH ROW EXECUTE FUNCTION kpi_maintenance.track_equipment_location();

CREATE OR REPLACE FUNCTION kpi_maintenance.track_location_branch()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE asset record;
BEGIN
  IF NEW.sucursal_id IS DISTINCT FROM OLD.sucursal_id OR NEW.nombre IS DISTINCT FROM OLD.nombre OR NEW.codigo IS DISTINCT FROM OLD.codigo THEN
    FOR asset IN SELECT id FROM kpi_maintenance.tb_equipo WHERE location_id=NEW.id ORDER BY id LOOP
      PERFORM kpi_maintenance.record_equipment_location(asset.id,NEW.updated_by,'LOCATION_CHANGED');
    END LOOP;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_location_branch ON kpi_maintenance.tb_location;
CREATE TRIGGER trg_location_branch AFTER UPDATE OF sucursal_id,nombre,codigo ON kpi_maintenance.tb_location
  FOR EACH ROW EXECUTE FUNCTION kpi_maintenance.track_location_branch();

CREATE OR REPLACE FUNCTION kpi_maintenance.track_branch_identity()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE asset record;
BEGIN
  IF NEW.nombre IS DISTINCT FROM OLD.nombre OR NEW.codigo IS DISTINCT FROM OLD.codigo THEN
    FOR asset IN SELECT e.id FROM kpi_maintenance.tb_equipo e JOIN kpi_maintenance.tb_location l ON l.id=e.location_id
      WHERE l.sucursal_id=NEW.id ORDER BY e.id LOOP
      PERFORM kpi_maintenance.record_equipment_location(asset.id,NEW.updated_by,'BRANCH_RENAMED');
    END LOOP;
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_branch_identity ON kpi_inventory.tb_sucursal;
CREATE TRIGGER trg_branch_identity AFTER UPDATE OF nombre,codigo ON kpi_inventory.tb_sucursal
  FOR EACH ROW EXECUTE FUNCTION kpi_maintenance.track_branch_identity();

-- Establish an observed baseline; never fabricate an arrival date for old assets.
SELECT kpi_maintenance.record_equipment_location(e.id,'migration-20261019','BASELINE_OBSERVED')
  FROM kpi_maintenance.tb_equipo e WHERE NOT e.is_deleted
  AND NOT EXISTS (SELECT 1 FROM kpi_maintenance.tb_equipo_ubicacion_historial h WHERE h.equipo_id=e.id);
UPDATE kpi_process.tb_work_order wo
  SET equipment_location_snapshot=kpi_maintenance.equipment_location_snapshot(wo.equipment_id)
    || jsonb_build_object('captured_at',clock_timestamp(),'basis','BASELINE_OBSERVED')
  WHERE wo.equipment_id IS NOT NULL AND wo.equipment_location_snapshot IS NULL
    AND (wo.started_at IS NOT NULL OR wo.closed_at IS NOT NULL OR
      upper(wo.status_workflow) IN ('IN_PROGRESS','EN_PROCESO','REVIEW','CLOSED','CERRADA','CERRADO','DONE','COMPLETED'));

CREATE OR REPLACE FUNCTION kpi_maintenance.freeze_work_order_location()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP='UPDATE' AND OLD.equipment_location_snapshot IS NOT NULL THEN
    IF NEW.equipment_id IS DISTINCT FROM OLD.equipment_id THEN
      RAISE EXCEPTION 'No se puede cambiar el equipo de una OT que inició su ejecución';
    END IF;
    NEW.equipment_location_snapshot:=OLD.equipment_location_snapshot;
    RETURN NEW;
  END IF;
  -- Ignore client-provided snapshots. Freeze on the first execution, including direct closure.
  NEW.equipment_location_snapshot:=NULL;
  IF NEW.equipment_id IS NOT NULL AND (NEW.started_at IS NOT NULL OR NEW.closed_at IS NOT NULL OR
    upper(NEW.status_workflow) IN ('IN_PROGRESS','EN_PROCESO','REVIEW','CLOSED','CERRADA','CERRADO','DONE','COMPLETED')) THEN
    PERFORM id FROM kpi_maintenance.tb_equipo WHERE id=NEW.equipment_id FOR SHARE;
    NEW.equipment_location_snapshot:=kpi_maintenance.equipment_location_snapshot(NEW.equipment_id)
      || jsonb_build_object('captured_at',clock_timestamp(),'basis','EXECUTION_STARTED');
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_work_order_location ON kpi_process.tb_work_order;
CREATE TRIGGER trg_work_order_location BEFORE INSERT OR UPDATE ON kpi_process.tb_work_order
  FOR EACH ROW EXECUTE FUNCTION kpi_maintenance.freeze_work_order_location();
COMMIT;
