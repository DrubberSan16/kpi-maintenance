-- Functional verification only: all fixtures and mutations are rolled back.
BEGIN;
DO $$
DECLARE kind_id uuid := gen_random_uuid(); equipment_id uuid := gen_random_uuid(); order_id uuid := gen_random_uuid();
  snapshot jsonb; payload jsonb; base numeric; state text; clock_start timestamp; live_reading numeric;
BEGIN
  INSERT INTO kpi_maintenance.tb_equipo_tipo(id,codigo,nombre)
    VALUES(kind_id,'VERIFY-'||left(kind_id::text,8),'Verificación temporal horómetro');
  INSERT INTO kpi_maintenance.tb_equipo(id,codigo,nombre,equipo_tipo_id,horometro_actual,estado_funcionamiento)
    VALUES(equipment_id,'VERIFY-'||left(equipment_id::text,8),'Verificación temporal horómetro',kind_id,1000,'FUNCIONAMIENTO');
  UPDATE kpi_maintenance.tb_equipo SET horometro_operativo_desde=(clock_timestamp() AT TIME ZONE 'America/Guayaquil')-interval '2 hours'
    WHERE id=equipment_id;
  SELECT horometro_actual,estado_funcionamiento,horometro_operativo_desde INTO base,state,clock_start
    FROM kpi_maintenance.tb_equipo WHERE id=equipment_id;
  INSERT INTO kpi_process.tb_work_order(id,code,type,title,equipment_id,status_workflow,valor_json)
    VALUES(order_id,'VERIFY-'||left(order_id::text,8),'MANTENIMIENTO','Verificación temporal',equipment_id,'PLANNED',
      '{"horometro_actual":99999,"horometro_anterior":900,"horometro_capturado_en":"2099-01-01"}'::jsonb);
  SELECT valor_json INTO payload FROM kpi_process.tb_work_order WHERE id=order_id;
  IF payload->>'horometro_actual' IS NOT NULL OR payload->>'horometro_capturado_en' IS NOT NULL THEN
    RAISE EXCEPTION 'PLANNED captured a horometer';
  END IF;
  UPDATE kpi_process.tb_work_order SET status_workflow='IN_PROGRESS',started_at=clock_timestamp() WHERE id=order_id;
  SELECT valor_json INTO snapshot FROM kpi_process.tb_work_order WHERE id=order_id;
  IF abs((snapshot->>'horometro_actual')::numeric-1002)>0.001 OR snapshot->>'horometro_capturado_en' IS NULL THEN
    RAISE EXCEPTION 'Execution did not capture the operational counter: %',snapshot;
  END IF;
  IF (snapshot->>'horometro_anterior')::numeric<>900 THEN RAISE EXCEPTION 'Historical previous reading changed'; END IF;
  UPDATE kpi_process.tb_work_order SET title='Editado',valor_json=valor_json||
    '{"horometro_actual":55555,"horometro_anterior":1,"horometro_capturado_en":"2099-01-01"}'::jsonb WHERE id=order_id;
  SELECT valor_json INTO payload FROM kpi_process.tb_work_order WHERE id=order_id;
  IF payload IS DISTINCT FROM snapshot THEN RAISE EXCEPTION 'Editing changed the execution snapshot'; END IF;
  UPDATE kpi_process.tb_work_order SET status_workflow='REVIEW' WHERE id=order_id;
  UPDATE kpi_process.tb_work_order SET status_workflow='IN_PROGRESS' WHERE id=order_id;
  UPDATE kpi_process.tb_work_order SET status_workflow='CLOSED',closed_at=clock_timestamp(),valor_json=valor_json||'{"horometro_actual":88888}'::jsonb WHERE id=order_id;
  SELECT valor_json INTO payload FROM kpi_process.tb_work_order WHERE id=order_id;
  IF payload IS DISTINCT FROM snapshot THEN RAISE EXCEPTION 'Resuming or closure recaptured the snapshot'; END IF;
  IF EXISTS(SELECT 1 FROM kpi_maintenance.tb_equipo WHERE id=equipment_id AND
    (horometro_actual IS DISTINCT FROM base OR estado_funcionamiento IS DISTINCT FROM state
     OR horometro_operativo_desde IS DISTINCT FROM clock_start)) THEN
    RAISE EXCEPTION 'OT lifecycle changed the equipment clock';
  END IF;
  SELECT horometro_actual+greatest(0,extract(epoch FROM ((clock_timestamp() AT TIME ZONE 'America/Guayaquil')-horometro_operativo_desde)))/3600
    INTO live_reading FROM kpi_maintenance.tb_equipo WHERE id=equipment_id;
  IF live_reading<1002 THEN RAISE EXCEPTION 'The independent equipment counter stopped'; END IF;
  RAISE NOTICE 'PASS: PLANNED empty; first IN_PROGRESS captures; edits/resume/closure freeze; equipment clock unchanged and advancing';
END;
$$;
ROLLBACK;
