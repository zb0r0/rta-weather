#!/bin/bash
# Przywraca bazę danych z pliku .sql do działającego kontenera PostgreSQL.
# Użycie: ./scripts/restore.sh backups/weather_2024-01-01_12-00-00.sql

set -e

if [ -z "$1" ]; then
    echo "Podaj ścieżkę do pliku backupu."
    echo "Użycie: ./scripts/restore.sh backups/weather_YYYY-MM-DD_HH-MM-SS.sql"
    exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "$BACKUP_FILE" ]; then
    echo "Plik nie istnieje: $BACKUP_FILE"
    exit 1
fi

# Wczytaj zmienne z .env (jeśli plik istnieje)
if [ -f "$(dirname "$0")/../.env" ]; then
    export $(grep -v '^#' "$(dirname "$0")/../.env" | xargs)
fi

CONTAINER="postgres"

echo ">>> Przywracam z: $BACKUP_FILE"
echo ">>> Uwaga: istniejące dane w tabeli weather_raw zostaną nadpisane."
read -p "Kontynuować? (t/N): " confirm
if [[ "$confirm" != "t" && "$confirm" != "T" ]]; then
    echo "Anulowano."
    exit 0
fi

docker exec -i "$CONTAINER" psql \
    -U "${POSTGRES_USER:-weather_user}" \
    -d "${POSTGRES_DB:-weather}" \
    --no-password \
    < "$BACKUP_FILE"

echo ">>> Przywracanie zakończone."
