#!/bin/bash
# Tworzy dump bazy danych z działającego kontenera PostgreSQL.
# Użycie: ./scripts/backup.sh
# Wynik:  backups/weather_YYYY-MM-DD_HH-MM-SS.sql

set -e

# Wczytaj zmienne z .env (jeśli plik istnieje)
if [ -f "$(dirname "$0")/../.env" ]; then
    export $(grep -v '^#' "$(dirname "$0")/../.env" | xargs)
fi

CONTAINER="postgres"
BACKUP_DIR="$(dirname "$0")/../backups"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
BACKUP_FILE="$BACKUP_DIR/weather_${TIMESTAMP}.sql"

mkdir -p "$BACKUP_DIR"

echo ">>> Tworzę backup: $BACKUP_FILE"

docker exec "$CONTAINER" pg_dump \
    -U "${POSTGRES_USER:-weather_user}" \
    -d "${POSTGRES_DB:-weather}" \
    --no-password \
    > "$BACKUP_FILE"

echo ">>> Gotowe! Rozmiar: $(du -sh "$BACKUP_FILE" | cut -f1)"
