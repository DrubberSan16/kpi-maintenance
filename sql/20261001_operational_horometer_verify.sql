-- Integration checks use existing rows only inside a transaction that rolls back.
BEGIN;
SET LOCAL lock_timeout = '5s';
DO $$
DECLARE equipment_id uuid; order_id uuid; reading numeric; snapshot numeric;
BEGIN
  SELECT wo.id, wo.equipment_id INTO order_id, equipment_id
  FROM kpi_process.tb_work_order wo
  WHERE NOT wo.is_deleted AND wo.equipment_id IS NOT NULL
    AND wo.status_workflow IN ('IN_PROGRESS', 'REVIEW')
    AND upper(COALESCE(wo.maintenance_kind, '')) <> 'PROYECTO'
  ORDER BY wo.created_at DESC LIMIT 1;
  IF order_id IS NULL THEN RAISE EXCEPTION 'No active order available for transactional verification'; END IF;
  UPDATE kpi_maintenance.tb_equipo SET horometro_actual = 1000, estado_funcionamiento = 'FUNCIONAMIENTO'
    WHERE id = equipment_id;
  UPDATE kpi_maintenance.tb_equipo SET horometro_operativo_desde = (clock_timestamp() AT TIME ZONE 'America/Guayaquil') - interval '2 hours'
    WHERE id = equipment_id;
  UPDATE kpi_maintenance.tb_equipo SET estado_funcionamiento = 'PARADO' WHERE id = equipment_id;
  SELECT horometro_actual INTO reading FROM kpi_maintenance.tb_equipo WHERE id = equipment_id;
  IF reading <> 1002 THEN RAISE EXCEPTION 'Stop did not accumulate two hours: %', reading; END IF;
  UPDATE kpi_process.tb_work_order SET status_workflow = 'CLOSED', valor_json = COALESCE(valor_json, '{}'::jsonb) || '{"horometro_anterior":1000,"horometro_actual":9999}'::jsonb WHERE id = order_id;
  SELECT (valor_json->>'horometro_actual')::numeric INTO snapshot FROM kpi_process.tb_work_order WHERE id = order_id;
  IF snapshot <> 1002 THEN RAISE EXCEPTION 'Closure did not capture equipment reading: %', snapshot; END IF;
  UPDATE kpi_process.tb_work_order SET valor_json = valor_json || '{"horometro_anterior":0,"horometro_actual":9999}'::jsonb WHERE id = order_id;
  SELECT (valor_json->>'horometro_actual')::numeric INTO snapshot FROM kpi_process.tb_work_order WHERE id = order_id;
  IF snapshot <> 1002 THEN RAISE EXCEPTION 'Closed reading was overwritten: %', snapshot; END IF;
  IF (SELECT (valor_json->>'horometro_anterior')::numeric FROM kpi_process.tb_work_order WHERE id = order_id) <> 1000 THEN RAISE EXCEPTION 'Initial reading changed after closure'; END IF;
  UPDATE kpi_maintenance.tb_equipo SET estado_funcionamiento = 'FUNCIONAMIENTO' WHERE id = equipment_id;
  IF (SELECT horometro_operativo_desde IS NULL FROM kpi_maintenance.tb_equipo WHERE id = equipment_id) THEN RAISE EXCEPTION 'Restart did not create time anchor'; END IF;
  UPDATE kpi_maintenance.tb_equipo SET horometro_actual = 2000 WHERE id = equipment_id;
  IF (SELECT horometro_actual FROM kpi_maintenance.tb_equipo WHERE id = equipment_id) <> 2000 THEN RAISE EXCEPTION 'Physical reading not accepted'; END IF;
  RAISE NOTICE 'PASS: accrual, stop, restart, physical reading, frozen initial/final OT readings';
END;
$$;
ROLLBACK;
