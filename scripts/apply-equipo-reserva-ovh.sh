#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

# Ejecutar como ubuntu en ovh-serverPostgres, desde el directorio que contiene
# también una copia de sql/20261016_equipo_reserva.sql. No requiere contraseñas.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/20261016_equipo_reserva.sql"
DATABASE="JUSTICE_KPI"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_DIR="/var/lib/postgresql/reserva-control-20261016"
BACKUP_FILE="$BACKUP_DIR/tb_equipo-before-$STAMP.dump"

[[ -r "$SQL_FILE" ]] || { printf 'No se encuentra %s\n' "$SQL_FILE" >&2; exit 1; }
DB_ACTUAL="$(sudo -n -u postgres psql -X -At -v ON_ERROR_STOP=1 -d "$DATABASE" -c 'SELECT current_database()')"
[[ "$DB_ACTUAL" == "$DATABASE" ]] || { printf 'Base de datos inesperada\n' >&2; exit 1; }

printf 'Servidor: %s\nBase: %s\nInicio UTC: %s\n' "$(hostname)" "$DB_ACTUAL" "$STAMP"
sha256sum "$SQL_FILE"
sudo -n install -d -m 0700 -o postgres -g postgres "$BACKUP_DIR"
sudo -n -u postgres pg_dump -d "$DATABASE" --format=custom --no-owner --no-acl \
  --table=kpi_maintenance.tb_equipo --file="$BACKUP_FILE"
sudo -n -u postgres test -s "$BACKUP_FILE"
printf 'Respaldo: %s\n' "$BACKUP_FILE"

sudo -n -u postgres psql -X -v ON_ERROR_STOP=1 -d "$DATABASE" -f "$SQL_FILE"
sudo -n -u postgres psql -X -v ON_ERROR_STOP=1 -d "$DATABASE" -c \
  "SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_schema='kpi_maintenance' AND table_name='tb_equipo' AND column_name='es_reserva';"
printf 'Migración y verificación completadas.\n'
