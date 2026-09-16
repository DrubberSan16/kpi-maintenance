-- ---------------------------------------------------------------------------
-- Ajuste directo de horómetro
--
-- El horómetro es un contador físico: solo avanza. Bajarlo a mano es una
-- corrección administrativa, no trabajo de la máquina, y por eso pasa a
-- distinguirse en el historial con `fuente = 'AJUSTE_DIRECTO'`: es lo que
-- permite listar esas correcciones por separado en un informe.
--
-- Se reetiquetan las correcciones descendentes que ya existían y que se
-- hicieron desde el módulo de Equipos. Las que vienen de una OT se quedan como
-- `ORDEN_TRABAJO`: esa lectura pertenece a la orden que la registró, no es un
-- ajuste suelto.
--
-- Las filas antiguas no llevan el motivo escrito por nadie -antes no se pedía-,
-- así que conservan la observación automática que tenían.
--
-- Idempotente: puede reejecutarse sin efectos.
-- ---------------------------------------------------------------------------

BEGIN;

UPDATE kpi_maintenance.tb_equipo_horometro_historial
SET fuente = 'AJUSTE_DIRECTO'
WHERE fuente = 'MANUAL_EQUIPOS'
  AND observacion ILIKE '%descendente%';

COMMIT;
