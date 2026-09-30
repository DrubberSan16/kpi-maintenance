-- ---------------------------------------------------------------------------
-- Responsables de OT y plantillas: de usuarios a empleados
--
-- Hasta hoy los responsables de una tarea de OT (tb_work_order_tarea.responsables)
-- y de una plantilla (tb_procedimiento_plantilla.responsabilidades) se elegian
-- de Usuarios. Desde este cambio se eligen de Empleados (tb_empleado): asi
-- puede trabajar en una OT quien no tiene usuario, y cada hora trabajada se
-- costea con el valor por hora de la persona.
--
-- Este script deja lo anterior en la forma nueva, sin perder nada:
--
--   1. Vincula con su empleado a cuatro usuarios que no lo tenian (acordado con
--      quien lo pidio; la cedula es la de PERSONAL JUSTICE.xlsx):
--        jean.cedeno   -> 1205870940  CEDENO TRIVINO JEAN BYRON
--        felipe.loaiza -> 1716621311  LOAIZA SILVA FELIPE
--        jean.aguirre  -> 0802531988  AGUIRRE ZAMORA JEAN PIERRE
--        jose.cortez   -> 1250347281  CORTEZ EZETA JOSE MANUEL
--      priscila.alarcon no esta en el Excel: se queda sin empleado y sus horas
--      siguen contando, sin costo.
--
--   2. A cada responsable de tarea guardado por usuario cuyo usuario tiene
--      empleado le agrega `empleado_id`, el nombre del empleado y `costo_hora`:
--      el valor por hora que el empleado tiene hoy, congelado. Antes no se
--      guardaba ningun costo, asi que hoy es lo unico que se puede congelar; los
--      cambios de valor por hora de aqui en adelante no tocan lo ya registrado.
--      `user_id` y `username` se dejan tal cual. Las tareas de OT Proyecto usan
--      la misma tabla, asi que quedan cubiertas.
--
--   3. En cada plantilla cambia el id de usuario por el de su empleado. Un usuario
--      sin empleado se deja: la aplicacion lo sigue mostrando por su usuario.
--
-- No cambia `updated_at` de las tareas: esto es una normalizacion, no una
-- edicion de la tarea (la tabla tiene un disparador que lo actualiza, y se
-- apaga mientras corre el script, dentro de la misma transaccion).
--
-- Antes de tocar nada se guarda una copia de lo que se va a cambiar en
-- kpi_maintenance.bk_20260930_* (si ya existe no se pisa, para no perder el
-- original al volver a ejecutar).
--
-- Idempotente: lo ya convertido no se vuelve a tocar. Como deshacerlo:
--   ALTER TABLE kpi_maintenance.tb_work_order_tarea
--     DISABLE TRIGGER trg_tb_work_order_tarea_updated_at;
--   UPDATE kpi_maintenance.tb_work_order_tarea t SET responsables = b.responsables
--     FROM kpi_maintenance.bk_20260930_tarea_responsables b WHERE b.id = t.id;
--   ALTER TABLE kpi_maintenance.tb_work_order_tarea
--     ENABLE TRIGGER trg_tb_work_order_tarea_updated_at;
--   UPDATE kpi_maintenance.tb_procedimiento_plantilla p
--     SET responsabilidades = b.responsabilidades
--     FROM kpi_maintenance.bk_20260930_plantilla_responsabilidades b WHERE b.id = p.id;
-- (lo agregado despues de este script, con costos ya congelados, se pierde.)
-- ---------------------------------------------------------------------------

BEGIN;

-- Copia de lo que se va a cambiar.
CREATE TABLE IF NOT EXISTS kpi_maintenance.bk_20260930_tarea_responsables AS
  SELECT id, responsables, updated_at
  FROM kpi_maintenance.tb_work_order_tarea;

CREATE TABLE IF NOT EXISTS kpi_maintenance.bk_20260930_plantilla_responsabilidades AS
  SELECT id, responsabilidades, updated_at
  FROM kpi_maintenance.tb_procedimiento_plantilla;

-- 1. Vinculo usuario - empleado. Solo donde el empleado no tiene usuario y el
-- usuario no esta ya en otro empleado.
UPDATE kpi_maintenance.tb_empleado emp
SET user_id = u.id,
    updated_at = now(),
    updated_by = 'migracion-responsables'
FROM (VALUES
  ('1205870940', 'jean.cedeno'),
  ('1716621311', 'felipe.loaiza'),
  ('0802531988', 'jean.aguirre'),
  ('1250347281', 'jose.cortez')
) AS v (cedula, name_user)
JOIN kpi_security.tb_user u
  ON u.name_user = v.name_user AND COALESCE(u.is_deleted, false) = false
WHERE emp.cedula = v.cedula
  AND emp.is_deleted = false
  AND emp.user_id IS NULL
  AND NOT EXISTS (
    SELECT 1 FROM kpi_maintenance.tb_empleado otro
    WHERE otro.user_id = u.id AND otro.is_deleted = false
  );

-- 2. Responsables de las tareas.
ALTER TABLE kpi_maintenance.tb_work_order_tarea
  DISABLE TRIGGER trg_tb_work_order_tarea_updated_at;

UPDATE kpi_maintenance.tb_work_order_tarea t
SET responsables = nuevo.responsables
FROM (
  SELECT
    tarea.id,
    jsonb_agg(
      CASE
        WHEN e.value ? 'empleado_id' THEN e.value
        WHEN emp.id IS NOT NULL THEN
          e.value || jsonb_build_object(
            'empleado_id', emp.id::text,
            'display_name', emp.nombres_apellidos,
            'costo_hora', to_jsonb(emp.valor_hora)
          )
        ELSE e.value
      END
      ORDER BY e.ord
    ) AS responsables
  FROM kpi_maintenance.tb_work_order_tarea tarea
  CROSS JOIN LATERAL jsonb_array_elements(tarea.responsables)
    WITH ORDINALITY AS e (value, ord)
  LEFT JOIN kpi_maintenance.tb_empleado emp
    ON emp.is_deleted = false
   AND emp.user_id::text = e.value ->> 'user_id'
  WHERE jsonb_typeof(tarea.responsables) = 'array'
  GROUP BY tarea.id
) nuevo
WHERE t.id = nuevo.id
  AND t.responsables IS DISTINCT FROM nuevo.responsables;

ALTER TABLE kpi_maintenance.tb_work_order_tarea
  ENABLE TRIGGER trg_tb_work_order_tarea_updated_at;

-- 3. Responsables de las plantillas: el id del empleado en lugar del del usuario,
-- en el mismo orden y sin repetir.
UPDATE kpi_maintenance.tb_procedimiento_plantilla p
SET responsabilidades = nuevo.ids
FROM (
  SELECT id, jsonb_agg(destino ORDER BY ord) AS ids
  FROM (
    SELECT DISTINCT ON (plantilla.id, COALESCE(emp.id::text, e.value))
      plantilla.id,
      e.ord,
      COALESCE(emp.id::text, e.value) AS destino
    FROM kpi_maintenance.tb_procedimiento_plantilla plantilla
    CROSS JOIN LATERAL jsonb_array_elements_text(plantilla.responsabilidades)
      WITH ORDINALITY AS e (value, ord)
    LEFT JOIN kpi_maintenance.tb_empleado emp
      ON emp.is_deleted = false
     AND emp.user_id::text = e.value
    WHERE jsonb_typeof(plantilla.responsabilidades) = 'array'
    ORDER BY plantilla.id, COALESCE(emp.id::text, e.value), e.ord
  ) unicos
  GROUP BY id
) nuevo
WHERE p.id = nuevo.id
  AND p.responsabilidades IS DISTINCT FROM nuevo.ids;

-- Resumen: lo que queda por usuario sin empleado se ve aqui.
DO $$
DECLARE
  con_empleado integer;
  sin_empleado integer;
  sin_costo integer;
  plantillas_por_usuario integer;
BEGIN
  SELECT
    count(*) FILTER (WHERE e.value ? 'empleado_id'),
    count(*) FILTER (WHERE NOT (e.value ? 'empleado_id')),
    count(*) FILTER (WHERE e.value ? 'empleado_id' AND NOT (e.value ? 'costo_hora'))
  INTO con_empleado, sin_empleado, sin_costo
  FROM kpi_maintenance.tb_work_order_tarea t
  CROSS JOIN LATERAL jsonb_array_elements(t.responsables) e
  WHERE jsonb_typeof(t.responsables) = 'array';

  SELECT count(*)
  INTO plantillas_por_usuario
  FROM kpi_maintenance.tb_procedimiento_plantilla p
  CROSS JOIN LATERAL jsonb_array_elements_text(p.responsabilidades) e
  WHERE jsonb_typeof(p.responsabilidades) = 'array'
    AND NOT EXISTS (
      SELECT 1 FROM kpi_maintenance.tb_empleado emp WHERE emp.id::text = e.value
    );

  RAISE NOTICE 'Responsables de tareas: % con empleado (% sin costo congelado), % solo por usuario.',
    con_empleado, sin_costo, sin_empleado;
  RAISE NOTICE 'Plantillas: % responsables siguen por usuario (sin empleado).',
    plantillas_por_usuario;
END $$;

COMMIT;
