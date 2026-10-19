-- Run after the migration inside a transaction which MUST be rolled back.
DO $$
DECLARE asset uuid; branch_a uuid; branch_b uuid; loc_a uuid:=gen_random_uuid(); loc_b uuid:=gen_random_uuid();
  old_ot uuid:=gen_random_uuid(); pending_ot uuid:=gen_random_uuid(); third_ot uuid:=gen_random_uuid();
  snap_a jsonb; snap_b jsonb; history_count integer;
BEGIN
  SELECT id INTO asset FROM kpi_maintenance.tb_equipo WHERE NOT is_deleted ORDER BY id LIMIT 1;
  SELECT id INTO branch_a FROM kpi_inventory.tb_sucursal WHERE NOT is_deleted ORDER BY id LIMIT 1;
  SELECT id INTO branch_b FROM kpi_inventory.tb_sucursal WHERE NOT is_deleted AND id<>branch_a ORDER BY id LIMIT 1;
  IF asset IS NULL OR branch_b IS NULL THEN RAISE EXCEPTION 'Se necesita un equipo y dos sucursales para verificar'; END IF;
  INSERT INTO kpi_maintenance.tb_location(id,codigo,nombre,sucursal_id,status)
    VALUES(loc_a,'QA-'||loc_a,'QA Ubicación A',branch_a,'ACTIVE'),(loc_b,'QA-'||loc_b,'QA Ubicación B',branch_b,'ACTIVE');
  UPDATE kpi_maintenance.tb_equipo SET location_id=loc_a WHERE id=asset;
  INSERT INTO kpi_process.tb_work_order(id,code,type,title,equipment_id,status_workflow,equipment_location_snapshot)
    VALUES(old_ot,'QA-'||old_ot,'MANTENIMIENTO','QA temporal',asset,'PLANNED','{"location_name":"FAKE"}'::jsonb),
      (pending_ot,'QA-'||pending_ot,'MANTENIMIENTO','QA planificada antes del traslado',asset,'PLANNED',NULL);
  IF EXISTS(SELECT 1 FROM kpi_process.tb_work_order WHERE id=old_ot AND equipment_location_snapshot IS NOT NULL) THEN
    RAISE EXCEPTION 'Una OT planificada aceptó ubicación suministrada por el cliente'; END IF;
  UPDATE kpi_process.tb_work_order SET status_workflow='IN_PROGRESS',started_at=now() WHERE id=old_ot;
  SELECT equipment_location_snapshot INTO snap_a FROM kpi_process.tb_work_order WHERE id=old_ot;
  IF (snap_a->>'location_id')::uuid<>loc_a OR (snap_a->>'sucursal_id')::uuid<>branch_a THEN RAISE EXCEPTION 'OT inicial sin ubicación A'; END IF;
  UPDATE kpi_maintenance.tb_equipo SET location_id=loc_b WHERE id=asset;
  UPDATE kpi_process.tb_work_order SET title='QA edición posterior',equipment_location_snapshot='{}'::jsonb WHERE id=old_ot;
  IF (SELECT equipment_location_snapshot FROM kpi_process.tb_work_order WHERE id=old_ot)<>snap_a THEN RAISE EXCEPTION 'La OT anterior cambió después del traslado o de una edición'; END IF;
  UPDATE kpi_process.tb_work_order SET status_workflow='IN_PROGRESS',started_at=now() WHERE id=pending_ot;
  SELECT equipment_location_snapshot INTO snap_b FROM kpi_process.tb_work_order WHERE id=pending_ot;
  IF (snap_b->>'location_id')::uuid<>loc_b OR (snap_b->>'sucursal_id')::uuid<>branch_b THEN RAISE EXCEPTION 'La OT pendiente no tomó la nueva ubicación al ejecutar'; END IF;
  UPDATE kpi_maintenance.tb_location SET sucursal_id=branch_a,nombre='QA ubicación renombrada' WHERE id=loc_b;
  INSERT INTO kpi_process.tb_work_order(id,code,type,title,equipment_id,status_workflow)
    VALUES(third_ot,'QA-'||third_ot,'MANTENIMIENTO','QA cierre directo',asset,'CLOSED');
  IF (SELECT equipment_location_snapshot->>'sucursal_id' FROM kpi_process.tb_work_order WHERE id=third_ot)<>branch_a::text THEN RAISE EXCEPTION 'El cambio de sucursal de ubicación no se refleja en nuevas ejecuciones'; END IF;
  IF (SELECT equipment_location_snapshot FROM kpi_process.tb_work_order WHERE id=pending_ot)<>snap_b THEN RAISE EXCEPTION 'Cambiar sucursal o nombre alteró la OT previa'; END IF;
  SELECT count(*) INTO history_count FROM kpi_maintenance.tb_equipo_ubicacion_historial WHERE equipo_id=asset AND location_id IN(loc_a,loc_b);
  IF history_count<>3 THEN RAISE EXCEPTION 'Se esperaban tres permanencias nuevas, se obtuvieron %',history_count; END IF;
  IF (SELECT count(*) FROM kpi_maintenance.tb_equipo_ubicacion_historial WHERE equipo_id=asset AND valid_to IS NULL)<>1 THEN RAISE EXCEPTION 'No hay una única ubicación vigente'; END IF;
  RAISE NOTICE 'PASS: ejecución, traslado, OT pendiente, sucursal, renombre, inmutabilidad y tres permanencias';
END $$;
