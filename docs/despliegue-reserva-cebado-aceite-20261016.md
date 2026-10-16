# Reserva de equipos y control de cebado y aceite

## Alcance de esta publicación

- `es_reserva` es un booleano independiente del estado operativo. Al detener desde el control se confirma si la parada es por reserva; al encender se retira la reserva.
- Gerencia usa un control unificado con todas las OT, incluso las de consumo cero, filtros por tipo y equipo, y las mismas filas para tablas, gráficos y PDF/Excel.
- Las OT de cebado presentan horas trabajadas desde el inicio de ejecución hasta el cierre, descontando los intervalos en revisión. Las otras OT conservan sus horómetros.
- Los ajustes locales previos de Kardex y del formato de texto de Excel no forman parte de estos commits.

## Scripts ejecutados

El SQL completo está en [`sql/20261016_equipo_reserva.sql`](../sql/20261016_equipo_reserva.sql).
El ejecutor con respaldo está en [`scripts/apply-equipo-reserva-ovh.sh`](../scripts/apply-equipo-reserva-ovh.sh).

El ejecutor comprueba la base `JUSTICE_KPI`, respalda `kpi_maintenance.tb_equipo` con `pg_dump` y aplica el SQL con `ON_ERROR_STOP=1`. La migración usa una transacción, espera de bloqueo máxima de 5 segundos y tiempo máximo de 30 segundos. Antes de añadir la columna guarda una fotografía temporal y comprueba que las demás columnas y filas no hayan cambiado. Una falla aborta la transacción. Repetirla conserva los valores de reserva existentes.

Comandos desde PowerShell en la carpeta que contiene `front-justice` y `kpi-maintenance`:

```powershell
ssh -o BatchMode=yes ovh-serverPostgres 'mkdir -p /tmp/kpi-reserva-control-20261016'
scp kpi-maintenance/sql/20261016_equipo_reserva.sql kpi-maintenance/scripts/apply-equipo-reserva-ovh.sh ovh-serverPostgres:/tmp/kpi-reserva-control-20261016/
ssh -o BatchMode=yes ovh-serverPostgres 'bash -n /tmp/kpi-reserva-control-20261016/apply-equipo-reserva-ovh.sh'
ssh -o BatchMode=yes ovh-serverPostgres 'bash /tmp/kpi-reserva-control-20261016/apply-equipo-reserva-ovh.sh'
```

No se incluyen contraseñas ni tokens. El servidor usa `sudo -n -u postgres` y autenticación local de PostgreSQL.

## Ejecución real de la base

| Verificación | Resultado |
| --- | --- |
| Servidor SSH | `ovh-serverPostgres` (`vps-49ac4f33`) |
| Base y versión | `JUSTICE_KPI`, PostgreSQL 18.6 |
| Inicio UTC | `2026-10-16 14:14:08` |
| SHA-256 del SQL ejecutado | `4e6b57fcf72132016442c2bff7de6a59646d53a209ef4c4b8498df0a7b446d6f` |
| Respaldo privado en servidor | `/var/lib/postgresql/reserva-control-20261016/tb_equipo-before-20261016T141408Z.dump` |
| Filas comprobadas sin modificar datos operativos | 37 |
| Nueva columna | `es_reserva boolean NOT NULL DEFAULT false` |
| Reservas iniciales / valores nulos | 0 / 0 |
| Transacción | `COMMIT`, comprobación de integridad aprobada |

El primer intento se detuvo durante el respaldo, antes de ejecutar el SQL: el directorio intermedio creado por `install -d` era de `root` y tenía permisos `0700`. El ejecutor final guarda el respaldo directamente bajo `/var/lib/postgresql/reserva-control-20261016`, con propietario `postgres` y permisos `0700`.

La migración no asigna reserva a equipos concretos ni cambia sus estados de funcionamiento o lecturas. La selección se registra mediante el control de equipos.

## Publicación y comprobaciones

1. Publicar por SSH en `main` de `DrubberSan16/kpi-maintenance` y `DrubberSan16/front-justice`.
2. Esperar `completed/success` del workflow `Deploy production` asociado al SHA de cada commit. Ambos invocan por SSH `/usr/local/sbin/deploy-kpi-component` en `ovh-serverJus`; el servidor valida el SHA exacto antes de compilar.
3. Comprobar `HEAD` en `/opt/kpi/kpi_maintenance` y `/var/www/front`, y el proceso `kpi_maintenance` con el usuario `justice` y `HOME=/var/lib/justice`.
4. Verificar por API la columna de reserva, el nuevo endpoint `/kpi_maintenance/dashboard-administracion/control-cebado-aceite`, filtros combinados, OT sin aceite y horas de cebado sin revisión. Comparar el resultado con los datos de la base.
5. Verificar que el `index.html` público referencia los artefactos compilados de esta publicación y que contienen la modal, los chips y el control unificado.

Antes de publicar se validaron 26 suites / 600 pruebas de backend, builds de ambos proyectos y pruebas de PDF/Excel y horómetro. El SQL se ensayó en PostgreSQL 17 aislado, incluyendo repetición e integridad de reservas existentes. El frontend se vuelve a comprobar desde una copia del índice de Git para excluir los cambios locales ajenos a esta tarea.

Los SHAs, enlaces de Actions y resultados finales de producción quedan en el informe de la conversación y en `outputs/20261016-reserva-control/` de la carpeta de trabajo. El respaldo contiene datos privados y permanece exclusivamente en el servidor PostgreSQL.

## Recuperación

Ante un error durante la migración, PostgreSQL revierte la transacción automáticamente. Conservar el `.dump` anterior para una recuperación dirigida; no restaurarlo sobre una tabla activa sin revisar las escrituras posteriores. Una reversión del código debe usar los SHAs previos: backend `c7841b5cee4188f7d88569ce5d76a48ac94492ad` y frontend `a1f79384f97f493925b9d82dc5c3ff7693ba26b6`. La columna adicional es compatible con esos binarios y puede conservarse.
