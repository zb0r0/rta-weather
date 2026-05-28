# Weather Pipeline — SGH Warszawa

Monitorowanie pogody w czasie rzeczywistym na koordynatach budynku SGH (52.25°N, 21.0°E).

## Architektura

```
OpenWeatherMap API
       ↓
   [Producer]  ─── co 60s ───→  Kafka: weather-raw
                                       ↓
                               [Consumer/Sink]
                                       ↓
                               PostgreSQL: weather_raw
                                       ↓
                    ┌──────────────────┴──────────────────┐
             [Stream Processor]                   [Batch ML Training]
             Kafka: weather-alerts                      ↓
                    ↓                         [Inference Service]
             PostgreSQL: weather_alerts        PostgreSQL: weather_predictions
                                                         ↓
                                                    [Grafana Dashboard]
```

## Struktura projektu

```
weather-pipeline/
├── docker-compose.yml
├── .env
├── db/
│   └── init.sql          ← schemat bazy danych (uruchamiany automatycznie)
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── producer.py       ← OWM API → Kafka
└── consumer/
    ├── Dockerfile
    ├── requirements.txt
    └── consumer.py       ← Kafka → PostgreSQL
```

## Uruchomienie

```bash
# 1. Sklonuj repo i wejdź do katalogu
cd weather-pipeline

# 2. Uruchom cały stack
docker compose up --build -d

# 3. Sprawdź logi producenta
docker logs -f weather_producer

# 4. Sprawdź logi konsumenta
docker logs -f weather_consumer

# 5. Zatrzymaj stack
docker compose down
```

## Dostęp do usług

| Usługa    | Adres                     | Dane logowania          |
|-----------|---------------------------|-------------------------|
| pgAdmin   | http://localhost:5050     | admin@sgh.waw.pl / admin |
| PostgreSQL| localhost:5432            | weather_user / weather_pass |
| Kafka     | localhost:9092            | —                       |

### Szybka weryfikacja danych w bazie

```sql
-- Ostatnie 10 rekordów
SELECT measured_at, temp_c, pressure_hpa, humidity_pct, weather_description
FROM weather_raw
ORDER BY measured_at DESC
LIMIT 10;

-- Widok ostatnich 24h
SELECT * FROM v_weather_last_24h LIMIT 50;
```

## Tabele bazy danych

| Tabela                  | Właściciel    | Opis                                        |
|-------------------------|---------------|---------------------------------------------|
| `weather_raw`           | Ty            | Surowe dane z OWM, zapis co minutę          |
| `weather_alerts`        | Osoba 2       | Alerty ze stream processora                 |
| `weather_predictions`   | Osoba 4       | Predykcje modelu ML                         |

## Zmienne środowiskowe (.env)

| Zmienna                  | Opis                              |
|--------------------------|-----------------------------------|
| `OWM_API_KEY`            | Klucz API OpenWeatherMap          |
| `OWM_LAT` / `OWM_LON`   | Koordynaty SGH                    |
| `FETCH_INTERVAL_SECONDS` | Częstotliwość odpytywania API (s) |
| `POSTGRES_*`             | Dane połączenia z bazą            |
| `PGADMIN_*`              | Dane logowania do pgAdmin         |
