BEGIN;
SET LOCAL lock_timeout = '10s';
ALTER TABLE kpi_maintenance.tb_equipo
  ADD COLUMN IF NOT EXISTS horometro_operativo_desde timestamp without time zone;

-- Activación prospectiva: no inventar horas de funcionamiento anteriores.
UPDATE kpi_maintenance.tb_equipo
SET horometro_operativo_desde = clock_timestamp() AT TIME ZONE 'America/Guayaquil'
WHERE estado_funcionamiento = 'FUNCIONAMIENTO' AND horometro_operativo_desde IS NULL;

CREATE OR REPLACE FUNCTION kpi_maintenance.track_operational_horometer()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE moment timestamp := clock_timestamp() AT TIME ZONE 'America/Guayaquil';
BEGIN
  IF TG_OP = 'INSERT' THEN
    NEW.horometro_operativo_desde := CASE WHEN NEW.estado_funcionamiento = 'FUNCIONAMIENTO' THEN moment ELSE NULL END;
    RETURN NEW;
  END IF;
  IF NEW.horometro_actual IS DISTINCT FROM OLD.horometro_actual THEN
    -- Una lectura física sustituye la estimación y establece una nueva referencia.
    NEW.horometro_operativo_desde := CASE WHEN NEW.estado_funcionamiento = 'FUNCIONAMIENTO' THEN moment ELSE NULL END;
  ELSIF NEW.estado_funcionamiento IS DISTINCT FROM OLD.estado_funcionamiento THEN
    IF OLD.estado_funcionamiento = 'FUNCIONAMIENTO' AND OLD.horometro_operativo_desde IS NOT NULL THEN
      NEW.horometro_actual := round((OLD.horometro_actual + greatest(0, extract(epoch FROM (moment - OLD.horometro_operativo_desde))) / 3600)::numeric, 2);
      NEW.fecha_ultima_lectura := moment;
    END IF;
    NEW.horometro_operativo_desde := CASE WHEN NEW.estado_funcionamiento = 'FUNCIONAMIENTO' THEN moment ELSE NULL END;
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_operational_horometer ON kpi_maintenance.tb_equipo;
CREATE TRIGGER trg_operational_horometer
BEFORE INSERT OR UPDATE ON kpi_maintenance.tb_equipo
FOR EACH ROW EXECUTE FUNCTION kpi_maintenance.track_operational_horometer();

CREATE OR REPLACE FUNCTION kpi_maintenance.freeze_closed_order_horometer()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reading numeric;
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.status_workflow = 'CLOSED' THEN
    -- La lectura de cierre permanece histórica, también al editar otros campos.
    NEW.valor_json := COALESCE(NEW.valor_json, '{}'::jsonb) - 'horometro_anterior' - 'horometro_actual';
    IF COALESCE(OLD.valor_json, '{}'::jsonb) ? 'horometro_anterior' THEN
      NEW.valor_json := NEW.valor_json || jsonb_build_object('horometro_anterior', OLD.valor_json -> 'horometro_anterior');
    END IF;
    IF COALESCE(OLD.valor_json, '{}'::jsonb) ? 'horometro_actual' THEN
      NEW.valor_json := NEW.valor_json || jsonb_build_object('horometro_actual', OLD.valor_json -> 'horometro_actual');
    END IF;
  ELSIF NEW.status_workflow = 'CLOSED' AND upper(COALESCE(NEW.maintenance_kind, '')) <> 'PROYECTO' AND NEW.equipment_id IS NOT NULL THEN
    SELECT horometro_actual + CASE
      WHEN estado_funcionamiento = 'FUNCIONAMIENTO' AND horometro_operativo_desde IS NOT NULL
      THEN greatest(0, extract(epoch FROM ((clock_timestamp() AT TIME ZONE 'America/Guayaquil') - horometro_operativo_desde))) / 3600
      ELSE 0 END INTO reading
    FROM kpi_maintenance.tb_equipo WHERE id = NEW.equipment_id;
    IF reading IS NOT NULL THEN
      NEW.valor_json := COALESCE(NEW.valor_json, '{}'::jsonb) || jsonb_build_object('horometro_actual', round(reading, 2), 'horometro_cerrado_en', clock_timestamp() AT TIME ZONE 'America/Guayaquil');
    END IF;
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_freeze_closed_order_horometer ON kpi_process.tb_work_order;
CREATE TRIGGER trg_freeze_closed_order_horometer
BEFORE INSERT OR UPDATE ON kpi_process.tb_work_order
FOR EACH ROW EXECUTE FUNCTION kpi_maintenance.freeze_closed_order_horometer();
COMMIT;
