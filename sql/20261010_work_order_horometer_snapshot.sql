-- Prospective OT snapshots. Existing started/closed readings are preserved.
-- The equipment's operational clock remains independent of the OT workflow.
BEGIN;
SET LOCAL lock_timeout = '10s';
CREATE OR REPLACE FUNCTION kpi_maintenance.freeze_closed_order_horometer()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reading numeric; moment timestamp := clock_timestamp() AT TIME ZONE 'America/Guayaquil';
BEGIN
  IF TG_OP = 'UPDATE' AND (
    OLD.started_at IS NOT NULL OR OLD.status_workflow IN ('IN_PROGRESS','REVIEW','CLOSED')
  ) THEN
    -- Preserve all snapshot fields once execution has started, including closure.
    NEW.valor_json := COALESCE(NEW.valor_json, '{}'::jsonb)
      - 'horometro_actual' - 'horometro_anterior' - 'horometro_inicio_ejecucion'
      - 'horometro_capturado_en' - 'horometro_cerrado_en' - 'horometro_detenido_en';
    NEW.valor_json := NEW.valor_json || (
      SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb)
      FROM jsonb_each(COALESCE(OLD.valor_json, '{}'::jsonb))
      WHERE key IN ('horometro_actual','horometro_anterior','horometro_inicio_ejecucion',
                    'horometro_capturado_en','horometro_cerrado_en','horometro_detenido_en')
    );
  ELSIF NEW.status_workflow = 'IN_PROGRESS'
    AND upper(COALESCE(NEW.maintenance_kind,'')) <> 'PROYECTO'
    AND NEW.equipment_id IS NOT NULL THEN
    SELECT horometro_actual + CASE
      WHEN estado_funcionamiento = 'FUNCIONAMIENTO' AND horometro_operativo_desde IS NOT NULL
      THEN greatest(0, extract(epoch FROM (moment - horometro_operativo_desde))) / 3600
      ELSE 0 END INTO reading
    FROM kpi_maintenance.tb_equipo WHERE id = NEW.equipment_id;
    IF reading IS NOT NULL THEN
      NEW.valor_json := COALESCE(NEW.valor_json, '{}'::jsonb) || jsonb_build_object(
        'horometro_automatico',true, 'horometro_actual',round(reading,6),
        'horometro_inicio_ejecucion',round(reading,6), 'horometro_capturado_en',moment);
    END IF;
  ELSIF NEW.status_workflow = 'PLANNED' THEN
    -- Saving planning data never constitutes the execution snapshot.
    NEW.valor_json := COALESCE(NEW.valor_json, '{}'::jsonb)
      || jsonb_build_object('horometro_actual',NULL,'horometro_capturado_en',NULL);
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_freeze_closed_order_horometer ON kpi_process.tb_work_order;
CREATE TRIGGER trg_freeze_closed_order_horometer
BEFORE INSERT OR UPDATE ON kpi_process.tb_work_order
FOR EACH ROW EXECUTE FUNCTION kpi_maintenance.freeze_closed_order_horometer();
COMMIT;
