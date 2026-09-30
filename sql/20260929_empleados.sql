-- ---------------------------------------------------------------------------
-- Empleados
--
-- Catalogo del personal de la empresa: nombres y apellidos, cedula, sueldo,
-- valor por hora y cargo. Un empleado puede o no tener usuario en el sistema:
-- `user_id` enlaza con kpi_security.tb_user y queda nulo cuando no lo tiene.
--
-- valor_hora
--   Se calcula con el sueldo (sueldo / 240: 30 dias x 8 horas, la base del
--   Codigo del Trabajo para las horas suplementarias) y se guarda con cuatro
--   decimales, porque un costo unitario que nace de una division no se redondea
--   en el origen: la pantalla lo muestra con dos. Un administrador puede fijarlo
--   a mano si a la persona se le paga otro valor; en ese caso
--   `valor_hora_manual` es true y un cambio de sueldo posterior o una
--   importacion de Excel ya no lo pisan.
--
-- cargo
--   Texto libre. La aplicacion sugiere los cargos ya registrados y reutiliza la
--   escritura existente cuando alguien teclea el mismo cargo con otras
--   mayusculas, tildes o espacios, asi que no se repite.
--
-- Unicidad
--   Una cedula y un usuario pertenecen a un solo empleado vivo. Los indices son
--   parciales (is_deleted = false): dar de baja a alguien libera su cedula y su
--   usuario.
--
-- Idempotente: puede reejecutarse sin efectos. Como deshacerlo:
--   DROP TABLE kpi_maintenance.tb_empleado;
-- ---------------------------------------------------------------------------

BEGIN;

CREATE TABLE IF NOT EXISTS kpi_maintenance.tb_empleado (
  id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id            uuid,
  nombres_apellidos  varchar(200) NOT NULL,
  cedula             varchar(10) NOT NULL,
  sueldo             numeric(12, 2) NOT NULL,
  valor_hora         numeric(14, 4) NOT NULL,
  valor_hora_manual  boolean NOT NULL DEFAULT false,
  cargo              varchar(150) NOT NULL,
  status             text NOT NULL DEFAULT 'ACTIVE',
  created_at         timestamp without time zone NOT NULL DEFAULT now(),
  updated_at         timestamp without time zone NOT NULL DEFAULT now(),
  created_by         text,
  updated_by         text,
  is_deleted         boolean NOT NULL DEFAULT false,
  deleted_at         timestamp without time zone,
  deleted_by         text,
  CONSTRAINT fk_empleado_user FOREIGN KEY (user_id)
    REFERENCES kpi_security.tb_user (id) ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT ck_empleado_cedula CHECK (cedula ~ '^[0-9]{10}$'),
  CONSTRAINT ck_empleado_sueldo CHECK (sueldo > 0),
  CONSTRAINT ck_empleado_valor_hora CHECK (valor_hora >= 0),
  CONSTRAINT ck_empleado_status CHECK (status IN ('ACTIVE', 'INACTIVE'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_empleado_cedula
  ON kpi_maintenance.tb_empleado (cedula)
  WHERE is_deleted = false;

CREATE UNIQUE INDEX IF NOT EXISTS uq_empleado_user
  ON kpi_maintenance.tb_empleado (user_id)
  WHERE is_deleted = false AND user_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_empleado_nombres
  ON kpi_maintenance.tb_empleado (nombres_apellidos)
  WHERE is_deleted = false;

COMMENT ON TABLE kpi_maintenance.tb_empleado IS
  'Personal de la empresa con su sueldo y valor por hora. user_id enlaza con kpi_security.tb_user cuando el empleado tiene usuario.';
COMMENT ON COLUMN kpi_maintenance.tb_empleado.valor_hora IS
  'Valor de la hora ordinaria en USD, con cuatro decimales. Por defecto sueldo / 240; ver valor_hora_manual.';
COMMENT ON COLUMN kpi_maintenance.tb_empleado.valor_hora_manual IS
  'true cuando un administrador fijo el valor por hora a mano: no se recalcula al cambiar el sueldo ni al importar.';

-- Las tablas de la aplicacion pertenecen al rol justice_app; ejecutado como
-- postgres la nueva nace con otro dueno y la aplicacion no podria leerla.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'justice_app')
     AND EXISTS (
       SELECT 1 FROM pg_tables
       WHERE schemaname = 'kpi_maintenance'
         AND tablename = 'tb_empleado'
         AND tableowner <> 'justice_app'
     ) THEN
    ALTER TABLE kpi_maintenance.tb_empleado OWNER TO justice_app;
  END IF;
END $$;

COMMIT;
