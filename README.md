# Weather Pipeline — SGH Warszawa

Monitorowanie pogody w czasie rzeczywistym na koordynatach budynku SGH (52.25°N, 21.0°E).
Dane na żywo pobierane są z **Open-Meteo** co 15 minut i zapisywane do PostgreSQL przez Apache Kafka.
Dane historyczne również z Open-Meteo — godzinowa rozdzielczość, dostępne od 1940 roku.
Oba źródła są darmowe i nie wymagają klucza API.

---

## Wymagania wstępne

Zainstaluj przed startem:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) — jedyna wymagana aplikacja
- [Git](https://git-scm.com/) — do sklonowania repozytorium
- Git Bash (instaluje się razem z Gitem) — do uruchamiania skryptów `.sh` na Windowsie

---

## Pierwsze uruchomienie

### 1. Uruchom Docker Desktop

Otwórz aplikację Docker Desktop i poczekaj aż w lewym dolnym rogu pojawi się zielony napis **"Engine running"**. Bez tego żadna komenda docker nie zadziała.

### 2. Sklonuj repozytorium

```bash
git clone https://github.com/zb0r0/rta-weather.git
cd weather-pipeline
```

### 3. Skonfiguruj zmienne środowiskowe

Plik `.env` jest już w repo (prywatne repozytorium). Jeśli go nie ma — skopiuj z szablonu:

```bash
cp .env.example .env
```

Żadnego klucza API nie trzeba uzupełniać — projekt używa wyłącznie Open-Meteo, które jest w pełni darmowe.

### 4. Uruchom cały stack

```bash
docker compose up --build -d
```

Pierwsze uruchomienie pobiera obrazy Dockera (~500 MB) i buduje kontenery — może potrwać 2-3 minuty. Przy kolejnych uruchomieniach jest znacznie szybciej.

### 5. Sprawdź czy wszystko działa

```bash
docker compose ps
```

Wszystkie serwisy powinny mieć status `healthy` lub `running`. Kafka potrzebuje ~30 sekund na pełny start.

---

## Baza danych

### Jak działa inicjalizacja

Schemat bazy (tabele, indeksy, widoki) tworzony jest **automatycznie** przy pierwszym uruchomieniu PostgreSQL. Plik `db/init.sql` jest wykonywany przez kontener przy starcie — nie musisz nic robić ręcznie.

### Weryfikacja — czy baza ma dane

Po ~60 sekundach od uruchomienia stacku powinny pojawić się pierwsze rekordy. Sprawdź przez pgAdmin lub przez terminal:

**Opcja A — pgAdmin (przeglądarka)**

1. Wejdź na [http://localhost:5050](http://localhost:5050)
2. Zaloguj się: `admin@sgh.waw.pl` / `admin`
3. Kliknij **Add New Server**:
   - Name: `weather`
   - Host: `postgres`
   - Port: `5432`
   - Username: `weather_user`
   - Password: `weather_pass`
4. Przejdź do: Servers → weather → Databases → weather → Schemas → public → Tables
5. Kliknij prawym na `weather_raw` → **View/Edit Data → All Rows**

**Opcja B — terminal**

```bash
docker exec -it postgres psql -U weather_user -d weather -c "SELECT measured_at, temp_c, pressure_hpa, weather_description FROM weather_raw ORDER BY measured_at DESC LIMIT 5;"
```

Jeśli tabela jest pusta — sprawdź logi konsumenta:

```bash
docker logs weather_consumer
docker logs weather_producer
```

### Przydatne zapytania SQL

```sql
-- Ostatnie 10 pomiarów
SELECT measured_at, temp_c, feels_like_c, pressure_hpa, humidity_pct, weather_description
FROM weather_raw
ORDER BY measured_at DESC
LIMIT 10;

-- Dane z ostatnich 24 godzin (gotowy widok)
SELECT * FROM v_weather_last_24h;

-- Statystyki dzienne
SELECT
    DATE(measured_at) AS dzien,
    ROUND(AVG(temp_c)::numeric, 2) AS srednia_temp,
    ROUND(MIN(temp_c)::numeric, 2) AS min_temp,
    ROUND(MAX(temp_c)::numeric, 2) AS max_temp,
    ROUND(AVG(pressure_hpa)::numeric, 1) AS srednie_cisnienie,
    COUNT(*) AS liczba_pomiarow
FROM weather_raw
GROUP BY DATE(measured_at)
ORDER BY dzien DESC;

-- Sprawdź ile rekordów historycznych vs na żywo
SELECT
    CASE WHEN raw_payload IS NULL THEN 'historyczne (Open-Meteo)'
         ELSE 'na żywo (OpenWeatherMap)' END AS zrodlo,
    COUNT(*) AS liczba,
    MIN(measured_at) AS od,
    MAX(measured_at) AS do
FROM weather_raw
GROUP BY 1;
```

---

## Dane historyczne

### Skąd pochodzą

Dane historyczne pobierane są z **Open-Meteo** (https://open-meteo.com/) — darmowe API bez klucza, godzinowa rozdzielczość, dane od 1940 roku. Trafiają do tej samej tabeli `weather_raw` co dane na żywo.

### Konfiguracja zakresu dat

W pliku `.env` ustaw zakres przed uruchomieniem:

```env
HISTORY_START_DATE=2026-01-01   # data początkowa
HISTORY_END_DATE=2026-05-27     # data końcowa (domyślnie: wczoraj)
```

Żeby pobrać dane do budowy dobrego modelu ML — ustaw szerszy zakres, np.:

```env
HISTORY_START_DATE=2020-01-01   # 5+ lat danych
```

### Uruchomienie pobierania historii

Stack musi być uruchomiony (baza musi działać). Uruchamiasz jednorazowo:

```bash
docker compose --profile historical run --rm historical-fetch
```

Skrypt wypisze postęp i ile rekordów wstawił. Możesz go uruchamiać wielokrotnie — duplikaty są pomijane automatycznie (`WHERE NOT EXISTS`).

### Ile danych dostaniesz

| Zakres | Liczba rekordów (godzinowe) |
|--------|----------------------------|
| Od 2026-01-01 | ~3 500 |
| Od 2024-01-01 | ~12 000 |
| Od 2020-01-01 | ~47 000 |
| Od 2010-01-01 | ~134 000 |

---

## Stream Processor & Alerty

Serwis `stream-processor` uruchamia się automatycznie razem z całym stackiem
i wykrywa anomalie pogodowe w czasie rzeczywistym.

### Wykrywane anomalie

| Typ alertu       | Warunek                                                   | Severity           |
|------------------|-----------------------------------------------------------|--------------------|
| `TEMP_ANOMALY`   | Temperatura odbiega od średniej kroczącej o więcej niż 2σ | WARNING            |
| `PRESSURE_DROP`  | Skok/spadek ciśnienia > 5 hPa między pomiarami            | WARNING            |
| `HIGH_WIND`      | Prędkość wiatru > 15 m/s (> 25 m/s → CRITICAL)           | WARNING / CRITICAL |
| `LOW_VISIBILITY` | Widoczność < 1000 m (< 200 m → CRITICAL)                  | WARNING / CRITICAL |

### Sprawdzenie alertów w bazie

```sql
SELECT alert_type, severity, message, measured_at, trigger_value
FROM weather_alerts
ORDER BY created_at DESC
LIMIT 20;
```

### Skan anomalii na danych historycznych

Dane historyczne omijają Kafkę i trafiają bezpośrednio do bazy — stream processor
ich nie widzi. Żeby wykryć anomalie też na danych historycznych, uruchom jednorazowo:

```bash
docker compose run --rm stream-processor python historical_anomaly_scan.py
```

Skrypt można uruchamiać wielokrotnie — duplikaty są pomijane automatycznie.

### Podgląd alertów na topiku Kafka

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic weather-alerts \
  --from-beginning
```

---

## Codzienna praca

### Uruchomienie (każdy dzień)

```bash
# 1. Upewnij się że Docker Desktop działa (zielony "Engine running")
# 2. W katalogu projektu:
docker compose up -d
```

Dane z poprzednich sesji zostają — volume PostgreSQL persystuje między uruchomieniami.

### Zatrzymanie

```bash
docker compose down        # zatrzymuje kontenery, dane zostają
docker compose down -v     # zatrzymuje kontenery I usuwa dane (ostrożnie!)
```

### Podgląd logów na żywo

```bash
docker logs -f weather_producer   # pobrania z Open-Meteo (co 15 min)
docker logs -f weather_consumer          # zapisy do bazy
docker logs -f weather_stream_processor  # wykrywanie anomalii i alerty
docker logs -f kafka                     # logi Kafki
```

### Aktualizacja kodu (po git pull)

```bash
git pull
docker compose up --build -d      # przebudowuje obrazy producenta i konsumenta
```

---

## Backup i przywracanie danych

### Tworzenie backupu

Wymaga działającego kontenera `postgres`. Uruchom przez Git Bash:

```bash
bash scripts/backup.sh
```

Tworzy plik `backups/weather_YYYY-MM-DD_HH-MM-SS.sql`. Katalog `backups/` jest w `.gitignore` — pliki zostają tylko lokalnie.

### Przywracanie backupu

```bash
# Uruchom stack (baza musi działać)
docker compose up -d

# Przywróć dane
bash scripts/restore.sh backups/weather_2026-01-01_12-00-00.sql
```

### Przesyłanie danych między członkami zespołu

```bash
# Osoba A robi backup i wysyła plik .sql (np. przez Messenger/Dysk)
bash scripts/backup.sh

# Osoba B klonuje repo, uruchamia stack i przywraca dane
docker compose up -d
bash scripts/restore.sh backups/weather_YYYY-MM-DD_HH-MM-SS.sql
```

---

## Struktura projektu

```
weather-pipeline/
├── docker-compose.yml          ← definicja wszystkich serwisów
├── .env                        ← zmienne środowiskowe
├── .env.example                ← szablon .env
├── .gitignore
├── db/
│   └── init.sql                ← schemat bazy (auto-wykonywany przy starcie)
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py             ← Open-Meteo current → Kafka (co 15 min)
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── consumer.py             ← Kafka → PostgreSQL
├── historical/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── historical_fetch.py     ← Open-Meteo → PostgreSQL (jednorazowo)
├── stream_processor/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── stream_processor.py         ← Kafka weather-raw → anomaly detection → weather-alerts
│   └── historical_anomaly_scan.py  ← jednorazowy skan anomalii na danych historycznych
├── scripts/
│   ├── backup.sh               ← pg_dump z działającego kontenera
│   └── restore.sh              ← przywracanie z pliku .sql
└── backups/                    ← pliki .sql (lokalnie, nie na GitHubie)
```

---

## Adresy serwisów

| Serwis     | Adres                     | Login / Hasło               |
|------------|---------------------------|-----------------------------|
| pgAdmin    | http://localhost:5050     | admin@sgh.waw.pl / admin    |
| PostgreSQL | localhost:5432            | weather_user / weather_pass |
| Kafka      | localhost:9092            | —                           |

---

## Tabele bazy danych

| Tabela                | Właściciel | Opis                                                        |
|-----------------------|------------|-------------------------------------------------------------|
| `weather_raw`         | Krystian   | Dane na żywo + historyczne — oba z Open-Meteo               |
| `weather_alerts`      | Maks       | Alerty ze stream processora i skanu historycznego           |
| `weather_predictions` | Osoba 4    | Predykcje modelu ML                                         |

Kolumna `raw_payload` zawiera pełny JSON dla danych na żywo, a `NULL` dla danych historycznych — po tym można odróżnić źródło.

---

## Rozwiązywanie problemów

**Docker nie startuje / "Engine running" nie pojawia się**
Zrestartuj Docker Desktop. Jeśli problem persystuje — zrestartuj komputer.

**Kontenery nie startują (`docker compose up` zawiesza się)**
```bash
docker compose down
docker compose up -d
```

**Baza jest pusta po kilku minutach**
```bash
docker logs weather_producer   # sprawdź czy API odpowiada
docker logs weather_consumer   # sprawdź czy consumer łączy się z bazą
```

**Port zajęty (np. 5432 używany przez lokalnego Postgresa)**
Zmień port w `docker-compose.yml`, np. `"5433:5432"` dla PostgreSQL.

**Błąd przy historical-fetch: "connection refused"**
Upewnij się że stack jest uruchomiony przed odpaleniem historical-fetch:
```bash
docker compose up -d
# poczekaj ~30 sekund
docker compose --profile historical run --rm historical-fetch
```
