-- ---------------------------------------------------------------------------
-- Los cuatro tableros como módulos de menú asignables por rol.
--
--   Dashboard Administración   dashboard-administracion   (ya existe)
--   Dashboard Gerencia         dashboard-gerencia         (renombra "Reporte detallado")
--   Dashboard Operativo        dashboard-operativo        (nuevo, pantalla en desarrollo)
--   Dashboard Supervisores     dashboard-supervisores     (nuevo, pantalla en desarrollo)
--
-- Cada uno es una entrada propia para poder concederlo por separado desde la
-- pantalla de Roles. La pantalla de inicio de cada usuario es el primero de
-- estos tableros que tenga asignado; sin ninguno, aterriza en Bienvenid@.
--
-- El renombrado conserva la fila (mismo id), así que las asignaciones de rol ya
-- existentes sobre "Reporte detallado" siguen siendo válidas: el permiso vive
-- en el id del menú, no en su nombre ni en su ruta.
--
-- Idempotente: puede reejecutarse sin efectos.
-- ---------------------------------------------------------------------------

BEGIN;

-- ------------------------------------------------- 1. Reporte detallado -> Gerencia
UPDATE kpi_security.tb_menu
SET nombre = 'Dashboard Gerencia',
    url_component = 'dashboard-gerencia',
    descripcion = 'Tablero de gerencia con el reporte detallado de operación',
    icon = COALESCE(NULLIF(icon, ''), 'mdi-chart-box-outline'),
    updated_at = now(),
    updated_by = 'SYSTEM'
WHERE COALESCE(is_deleted, false) = false
  AND url_component = 'reporte-detallado';

-- --------------------------------------------------- 2. Tableros nuevos
INSERT INTO kpi_security.tb_menu (
  id, nombre, descripcion, menu_id, url_component, menu_position, icon,
  status, created_at, updated_at, created_by, is_deleted
)
SELECT
  gen_random_uuid(),
  nuevo.nombre,
  nuevo.descripcion,
  padre.id,
  nuevo.url_component,
  nuevo.posicion,
  nuevo.icon,
  'ACTIVE', now(), now(), 'SYSTEM', false
FROM kpi_security.tb_menu padre
CROSS JOIN (
  VALUES
    ('Dashboard Operativo', 'Tablero operativo. Pendiente de definir indicadores.',
     'dashboard-operativo', 101::bigint, 'mdi-cog-play-outline'),
    ('Dashboard Supervisores', 'Tablero de supervisión. Pendiente de definir indicadores.',
     'dashboard-supervisores', 102::bigint, 'mdi-account-hard-hat-outline')
) AS nuevo(nombre, descripcion, url_component, posicion, icon)
WHERE padre.nombre = 'Administración'
  AND padre.menu_id IS NULL
  AND COALESCE(padre.is_deleted, false) = false
  AND NOT EXISTS (
    SELECT 1 FROM kpi_security.tb_menu m
    WHERE m.url_component = nuevo.url_component
      AND COALESCE(m.is_deleted, false) = false
  );

COMMIT;
