-- ---------------------------------------------------------------------------
-- OT Proyecto
--
-- Una OT de Proyecto es una orden de trabajo normal cuyo tipo de mantenimiento
-- es PROYECTO. Se diferencia en dos cosas:
--
--   1. No se ejecuta sobre un equipo: se ejecuta en una o varias ubicaciones y
--      bodegas, que se guardan en tablas hijas propias.
--   2. Contrata personal eventual (soldador, esmerilador, ...) que se liquida
--      por dia trabajado. Ese detalle vive en tb_work_order_proyecto_personal.
--
-- La plantilla (tb_procedimiento_plantilla) de tipo PROYECTO aporta los campos
-- de cabecera por defecto —empresa, objetivo general, objetivos especificos,
-- metodologia, alcance y los roles a contratar—. Los materiales NO se piden en
-- la plantilla porque en un proyecto son variables; se cargan en la OT.
--
-- Idempotente: puede reejecutarse sin efectos.
-- ---------------------------------------------------------------------------

BEGIN;

-- --------------------------------------------- 1. maintenance_kind PROYECTO
--
-- Se reemplaza el CHECK para admitir PROYECTO. Se incluye tambien INSPECCION:
-- el servicio ya lo aceptaba y la pantalla ya lo ofrecia, pero el CHECK lo
-- rechazaba, de modo que una OT de inspeccion reventaba al guardar.
DO $$
DECLARE
  constraint_row record;
  unsupported_values text;
BEGIN
  IF to_regclass('kpi_process.tb_work_order') IS NULL THEN
    RAISE NOTICE 'La tabla kpi_process.tb_work_order no existe. Se omite migracion.';
    RETURN;
  END IF;

  FOR constraint_row IN
    SELECT conname
    FROM pg_constraint
    WHERE conrelid = 'kpi_process.tb_work_order'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) ILIKE '%maintenance_kind%'
  LOOP
    EXECUTE format(
      'ALTER TABLE kpi_process.tb_work_order DROP CONSTRAINT %I',
      constraint_row.conname
    );
  END LOOP;

  SELECT string_agg(DISTINCT maintenance_kind, ', ' ORDER BY maintenance_kind)
  INTO unsupported_values
  FROM kpi_process.tb_work_order
  WHERE coalesce(is_deleted, false) = false
    AND maintenance_kind NOT IN (
      'CORRECTIVO', 'PREVENTIVO', 'PREDICTIVO', 'CEBADO', 'INSPECCION', 'PROYECTO'
    );

  IF unsupported_values IS NOT NULL THEN
    RAISE EXCEPTION
      'Existen tipos de mantenimiento no soportados en tb_work_order: %',
      unsupported_values;
  END IF;

  ALTER TABLE kpi_process.tb_work_order
    ADD CONSTRAINT ck_tb_work_order_maintenance_kind
    CHECK (
      maintenance_kind IN (
        'CORRECTIVO',
        'PREVENTIVO',
        'PREDICTIVO',
        'CEBADO',
        'INSPECCION',
        'PROYECTO'
      )
    );
END $$;

-- ------------------------------------- 2. Ubicaciones donde corre el proyecto
CREATE TABLE IF NOT EXISTS kpi_maintenance.tb_work_order_proyecto_ubicacion (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  work_order_id uuid NOT NULL,
  location_id   uuid NOT NULL,
  orden         integer NOT NULL DEFAULT 1,
  created_at    timestamp without time zone NOT NULL DEFAULT now(),
  updated_at    timestamp without time zone NOT NULL DEFAULT now(),
  created_by    text,
  updated_by    text,
  is_deleted    boolean NOT NULL DEFAULT false,
  CONSTRAINT fk_wopu_work_order FOREIGN KEY (work_order_id)
    REFERENCES kpi_process.tb_work_order (id) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_wopu_location FOREIGN KEY (location_id)
    REFERENCES kpi_maintenance.tb_location (id) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_wopu_work_order_location
  ON kpi_maintenance.tb_work_order_proyecto_ubicacion (work_order_id, location_id)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS idx_wopu_work_order
  ON kpi_maintenance.tb_work_order_proyecto_ubicacion (work_order_id)
  WHERE is_deleted = false;

-- ---------------------------------------- 3. Bodegas donde corre el proyecto
CREATE TABLE IF NOT EXISTS kpi_maintenance.tb_work_order_proyecto_bodega (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  work_order_id uuid NOT NULL,
  bodega_id     uuid NOT NULL,
  orden         integer NOT NULL DEFAULT 1,
  created_at    timestamp without time zone NOT NULL DEFAULT now(),
  updated_at    timestamp without time zone NOT NULL DEFAULT now(),
  created_by    text,
  updated_by    text,
  is_deleted    boolean NOT NULL DEFAULT false,
  CONSTRAINT fk_wopb_work_order FOREIGN KEY (work_order_id)
    REFERENCES kpi_process.tb_work_order (id) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_wopb_bodega FOREIGN KEY (bodega_id)
    REFERENCES kpi_inventory.tb_bodega (id) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_wopb_work_order_bodega
  ON kpi_maintenance.tb_work_order_proyecto_bodega (work_order_id, bodega_id)
  WHERE is_deleted = false;

CREATE INDEX IF NOT EXISTS idx_wopb_work_order
  ON kpi_maintenance.tb_work_order_proyecto_bodega (work_order_id)
  WHERE is_deleted = false;

-- ------------------------------------------------- 4. Personal contratado
--
-- Una fila por persona contratada. `rol` es el cargo del documento (Soldador
-- estructural, Esmerilador, ...) y las columnas siguientes son las columnas de
-- la tabla "Contratacion de personal": nombre, dias laborados, ubicacion,
-- valor dia, fecha y observacion.
CREATE TABLE IF NOT EXISTS kpi_maintenance.tb_work_order_proyecto_personal (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  work_order_id   uuid NOT NULL,
  orden           integer NOT NULL DEFAULT 1,
  rol             text NOT NULL,
  nombre          text,
  dias_laborados  numeric(10, 2) NOT NULL DEFAULT 0,
  location_id     uuid,
  ubicacion_texto text,
  valor_dia       numeric(18, 2) NOT NULL DEFAULT 0,
  fecha           date,
  observacion     text,
  created_at      timestamp without time zone NOT NULL DEFAULT now(),
  updated_at      timestamp without time zone NOT NULL DEFAULT now(),
  created_by      text,
  updated_by      text,
  is_deleted      boolean NOT NULL DEFAULT false,
  CONSTRAINT fk_wopp_work_order FOREIGN KEY (work_order_id)
    REFERENCES kpi_process.tb_work_order (id) ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT fk_wopp_location FOREIGN KEY (location_id)
    REFERENCES kpi_maintenance.tb_location (id) ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT ck_wopp_dias_laborados CHECK (dias_laborados >= 0),
  CONSTRAINT ck_wopp_valor_dia CHECK (valor_dia >= 0)
);

CREATE INDEX IF NOT EXISTS idx_wopp_work_order
  ON kpi_maintenance.tb_work_order_proyecto_personal (work_order_id, orden)
  WHERE is_deleted = false;

-- ------------------------------------------- 4.b Dueño de las tablas nuevas
--
-- La migracion se aplica con `sudo -u postgres`, asi que las tablas nacen a
-- nombre de postgres. El servicio entra como justice_app: sin este traspaso
-- TypeORM lee y escribe contra tablas que no puede tocar.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'justice_app') THEN
    ALTER TABLE kpi_maintenance.tb_work_order_proyecto_ubicacion OWNER TO justice_app;
    ALTER TABLE kpi_maintenance.tb_work_order_proyecto_bodega OWNER TO justice_app;
    ALTER TABLE kpi_maintenance.tb_work_order_proyecto_personal OWNER TO justice_app;
  END IF;
END $$;

-- --------------------------------------- 5. Campos de plantilla de proyecto
ALTER TABLE kpi_maintenance.tb_procedimiento_plantilla
  ADD COLUMN IF NOT EXISTS empresa text,
  ADD COLUMN IF NOT EXISTS objetivos_especificos jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS metodologia text,
  ADD COLUMN IF NOT EXISTS alcance jsonb NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS personal_requerido jsonb NOT NULL DEFAULT '[]'::jsonb;

COMMENT ON COLUMN kpi_maintenance.tb_procedimiento_plantilla.empresa
  IS 'Solo plantillas PROYECTO: empresa que ejecuta el proyecto.';
COMMENT ON COLUMN kpi_maintenance.tb_procedimiento_plantilla.objetivos_especificos
  IS 'Solo plantillas PROYECTO: lista de objetivos especificos.';
COMMENT ON COLUMN kpi_maintenance.tb_procedimiento_plantilla.metodologia
  IS 'Solo plantillas PROYECTO: metodologia aplicable.';
COMMENT ON COLUMN kpi_maintenance.tb_procedimiento_plantilla.alcance
  IS 'Solo plantillas PROYECTO: lista de actividades de alcance del proyecto.';
COMMENT ON COLUMN kpi_maintenance.tb_procedimiento_plantilla.personal_requerido
  IS 'Solo plantillas PROYECTO: [{rol, cantidad, valor_dia}] a contratar. Los materiales NO se definen aqui: en un proyecto son variables.';

-- --------------------------------------------- 6. Menu "OT. Proyecto"
--
-- Se crea colgando de Mantenimiento, justo debajo de "Ordenes de Trabajo".
-- No se asigna a ningun rol: el Super Administrador ve todo el arbol de menus
-- sin pasar por tb_menu_role, asi que queda visible solo para el mientras se
-- revisa. Para habilitarlo a otro rol basta con asignarlo desde Roles.
INSERT INTO kpi_security.tb_menu (
  id, nombre, descripcion, menu_id, url_component, menu_position, icon,
  status, created_at, updated_at, created_by, is_deleted
)
SELECT
  gen_random_uuid(),
  'OT. Proyecto',
  'Ordenes de trabajo de tipo Proyecto: se ejecutan sobre ubicaciones y bodegas.',
  padre.id,
  'work-orders-proyecto',
  6::bigint,
  'mdi-clipboard-text-multiple-outline',
  'ACTIVE', now(), now(), 'SYSTEM', false
FROM kpi_security.tb_menu padre
WHERE padre.nombre = 'Mantenimiento'
  AND padre.menu_id IS NULL
  AND COALESCE(padre.is_deleted, false) = false
  AND NOT EXISTS (
    SELECT 1 FROM kpi_security.tb_menu m
    WHERE m.url_component = 'work-orders-proyecto'
      AND COALESCE(m.is_deleted, false) = false
  );

-- "Terceros" ocupaba la posicion 6 dentro de Mantenimiento. Se corre a la 7
-- para que "OT. Proyecto" quede inmediatamente despues de Ordenes de Trabajo.
UPDATE kpi_security.tb_menu hijo
SET menu_position = 7,
    updated_at = now(),
    updated_by = 'SYSTEM'
FROM kpi_security.tb_menu padre
WHERE hijo.menu_id = padre.id
  AND padre.nombre = 'Mantenimiento'
  AND padre.menu_id IS NULL
  AND hijo.url_component = 'terceros'
  AND COALESCE(hijo.is_deleted, false) = false
  AND hijo.menu_position <> 7;

COMMIT;
