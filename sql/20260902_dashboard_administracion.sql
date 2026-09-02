-- ---------------------------------------------------------------------------
-- Dashboard Administración: campos base de los KPI de mantenimiento.
--
-- 1) Horas reales de la orden de trabajo.
--    Hasta ahora las horas de una OT salían de `horas_a_realizar`, un número
--    tecleado a mano dentro de `valor_json`. Se añaden marcas de inicio y fin
--    reales para que la duración se mida y no se declare; a partir de aquí los
--    reportes que midan horas de OT deben leer estos campos.
--
--    Se guardan como timestamp y no como `time` a propósito: una intervención
--    puede cruzar la medianoche, y con solo la hora del día la duración saldría
--    negativa.
--
-- 2) Márgenes del semáforo de mantenimiento por horómetro, configurables por
--    equipo. Por defecto: amarillo al 10% antes de cumplir la frecuencia y rojo
--    en cuanto se pasa (tolerancia 0%). Cada unidad puede ajustarlos, que es lo
--    que pedía el punto 6.
--
-- Idempotente: puede reejecutarse sin efectos.
-- ---------------------------------------------------------------------------

BEGIN;

-- --------------------------------------------------------------- 1. Work order
ALTER TABLE kpi_process.tb_work_order
  ADD COLUMN IF NOT EXISTS hora_inicio timestamp without time zone,
  ADD COLUMN IF NOT EXISTS hora_fin timestamp without time zone;

COMMENT ON COLUMN kpi_process.tb_work_order.hora_inicio IS
  'Inicio real de la intervención. Fuente de verdad para las horas de la OT.';
COMMENT ON COLUMN kpi_process.tb_work_order.hora_fin IS
  'Fin real de la intervención. Fuente de verdad para las horas de la OT.';

-- La duración no puede ser negativa.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_tb_work_order_horas_intervencion'
      AND conrelid = 'kpi_process.tb_work_order'::regclass
  ) THEN
    ALTER TABLE kpi_process.tb_work_order
      ADD CONSTRAINT chk_tb_work_order_horas_intervencion
      CHECK (hora_inicio IS NULL OR hora_fin IS NULL OR hora_fin >= hora_inicio);
  END IF;
END $$;

-- Semilla para las OT ya cerradas: se aprovechan las marcas de flujo existentes
-- para que los KPI tengan historia desde el primer día. Solo donde ambas
-- existen y son coherentes; el resto queda en NULL y se irá completando a mano.
UPDATE kpi_process.tb_work_order
SET hora_inicio = started_at,
    hora_fin = closed_at
WHERE COALESCE(is_deleted, false) = false
  AND hora_inicio IS NULL
  AND hora_fin IS NULL
  AND started_at IS NOT NULL
  AND closed_at IS NOT NULL
  AND closed_at >= started_at;

CREATE INDEX IF NOT EXISTS idx_tb_work_order_intervencion
  ON kpi_process.tb_work_order (equipment_id, maintenance_kind, hora_inicio)
  WHERE is_deleted = false;

-- ------------------------------------------------------------------ 2. Equipo
ALTER TABLE kpi_maintenance.tb_equipo
  ADD COLUMN IF NOT EXISTS margen_anticipacion_pct numeric(6, 2) NOT NULL DEFAULT 10,
  ADD COLUMN IF NOT EXISTS margen_tolerancia_pct numeric(6, 2) NOT NULL DEFAULT 0;

COMMENT ON COLUMN kpi_maintenance.tb_equipo.margen_anticipacion_pct IS
  'Porcentaje de la frecuencia antes del objetivo en que la alerta pasa a amarillo. 10 = avisa al 90% del intervalo.';
COMMENT ON COLUMN kpi_maintenance.tb_equipo.margen_tolerancia_pct IS
  'Porcentaje de la frecuencia que se tolera por encima del objetivo antes de pasar a rojo. 0 = rojo al superarlo.';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'chk_tb_equipo_margenes_semaforo'
      AND conrelid = 'kpi_maintenance.tb_equipo'::regclass
  ) THEN
    ALTER TABLE kpi_maintenance.tb_equipo
      ADD CONSTRAINT chk_tb_equipo_margenes_semaforo
      CHECK (
        margen_anticipacion_pct >= 0 AND margen_anticipacion_pct <= 100
        AND margen_tolerancia_pct >= 0 AND margen_tolerancia_pct <= 100
      );
  END IF;
END $$;

COMMIT;

-- ---------------------------------------------------------------------------
-- Registro del modulo en el menu.
--
-- El menu vive en `kpi_security.tb_menu` y `url_component` es la ruta del SPA.
-- Se cuelga de "Administracion", que es donde ya estan los reportes.
--
-- Nota: esto solo crea la entrada. El acceso se concede aparte, asignando el
-- permiso al rol correspondiente desde la pantalla de Roles.
-- ---------------------------------------------------------------------------

BEGIN;

INSERT INTO kpi_security.tb_menu (
  id, nombre, descripcion, menu_id, url_component, menu_position, icon,
  status, created_at, updated_at, created_by, is_deleted
)
SELECT
  gen_random_uuid(),
  'Dashboard Administración',
  'KPI de mantenimiento: disponibilidad, correctivos, cebado, repuestos, proyección y confiabilidad',
  padre.id,
  'dashboard-administracion',
  COALESCE((
    SELECT MAX(menu_position) + 1
    FROM kpi_security.tb_menu
    WHERE menu_id = padre.id AND COALESCE(is_deleted, false) = false
  ), 1),
  'mdi-view-dashboard-outline',
  'ACTIVE', now(), now(), 'SYSTEM', false
FROM kpi_security.tb_menu padre
WHERE padre.nombre = 'Administración'
  AND padre.menu_id IS NULL
  AND COALESCE(padre.is_deleted, false) = false
  -- Idempotente: no duplica si ya existe.
  AND NOT EXISTS (
    SELECT 1 FROM kpi_security.tb_menu m
    WHERE m.url_component = 'dashboard-administracion'
      AND COALESCE(m.is_deleted, false) = false
  );

COMMIT;
