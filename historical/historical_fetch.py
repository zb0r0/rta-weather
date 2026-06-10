"""
Historical Weather Fetch — Open-Meteo → PostgreSQL
----------------------------------------------------
Pobiera godzinowe dane historyczne z Open-Meteo (darmowe, bez API key)
i wstawia je do tabeli weather_raw.

Konfiguracja przez zmienne środowiskowe:
  HISTORY_START_DATE  — data początkowa, np. "2026-01-01"
  HISTORY_END_DATE    — data końcowa,    np. "2026-05-27" (domyślnie: wczoraj)

Uruchomienie:
  docker compose --profile historical run --rm historical-fetch
"""

import logging
import os
from datetime import date, timedelta, timezone, datetime

import psycopg2
import psycopg2.extras
import requests

# ── Konfiguracja ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Zakres dat — zmień te wartości żeby pobrać szerszy zakres ────────────────
HISTORY_START_DATE = os.environ.get("HISTORY_START_DATE", "2026-01-01")
HISTORY_END_DATE   = os.environ.get("HISTORY_END_DATE",
                                    str(date.today() - timedelta(days=1)))

LAT = os.environ.get("OWM_LAT", "52.25")
LON = os.environ.get("OWM_LON", "21.0")

POSTGRES_HOST     = os.environ["POSTGRES_HOST"]
POSTGRES_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.environ["POSTGRES_DB"]
POSTGRES_USER     = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# Open-Meteo archive API — darmowe, bez klucza, dane od 1940
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"

# ── Open-Meteo: kody WMO → opis (podzbiór) ───────────────────────────────────
WMO_DESCRIPTIONS = {
    0:  ("Clear",       "clear sky"),
    1:  ("Clear",       "mainly clear"),
    2:  ("Clouds",      "partly cloudy"),
    3:  ("Clouds",      "overcast"),
    45: ("Fog",         "fog"),
    48: ("Fog",         "rime fog"),
    51: ("Drizzle",     "light drizzle"),
    53: ("Drizzle",     "moderate drizzle"),
    55: ("Drizzle",     "dense drizzle"),
    61: ("Rain",        "slight rain"),
    63: ("Rain",        "moderate rain"),
    65: ("Rain",        "heavy rain"),
    71: ("Snow",        "slight snow"),
    73: ("Snow",        "moderate snow"),
    75: ("Snow",        "heavy snow"),
    77: ("Snow",        "snow grains"),
    80: ("Rain",        "slight showers"),
    81: ("Rain",        "moderate showers"),
    82: ("Rain",        "violent showers"),
    85: ("Snow",        "slight snow showers"),
    86: ("Snow",        "heavy snow showers"),
    95: ("Thunderstorm","thunderstorm"),
    96: ("Thunderstorm","thunderstorm with hail"),
    99: ("Thunderstorm","thunderstorm with heavy hail"),
}

# ── INSERT SQL ────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO weather_raw (
    measured_at,
    temp_k, feels_like_k, temp_min_k, temp_max_k,
    pressure_hpa, humidity_pct, visibility_m,
    wind_speed_ms, wind_deg, wind_gust_ms,
    clouds_pct,
    rain_1h_mm, snow_1h_mm,
    weather_id, weather_main, weather_description,
    lat, lon,
    raw_payload
)
SELECT
    %(measured_at)s,
    %(temp_k)s, %(feels_like_k)s, %(temp_k)s, %(temp_k)s,
    %(pressure_hpa)s, %(humidity_pct)s, %(visibility_m)s,
    %(wind_speed_ms)s, %(wind_deg)s, %(wind_gust_ms)s,
    %(clouds_pct)s,
    %(rain_1h_mm)s, %(snow_1h_mm)s,
    %(weather_id)s, %(weather_main)s, %(weather_description)s,
    %(lat)s, %(lon)s,
    %(raw_payload)s
WHERE NOT EXISTS (
    SELECT 1 FROM weather_raw
    WHERE measured_at = %(measured_at)s
)
"""

# ── Pobieranie danych z Open-Meteo ───────────────────────────────────────────

def fetch_historical(start_date: str, end_date: str) -> dict:
    params = {
        "latitude":        LAT,
        "longitude":       LON,
        "start_date":      start_date,
        "end_date":        end_date,
        "hourly": ",".join([
            "temperature_2m",
            "apparent_temperature",
            "relative_humidity_2m",
            "pressure_msl",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "cloud_cover",
            "precipitation",
            "snowfall",
            "visibility",
            "weather_code",
        ]),
        "wind_speed_unit": "ms",       # m/s zamiast domyślnych km/h
        # UTC, nie Europe/Warsaw — measured_at zapisywany jako timestamptz w UTC
        "timezone":        "UTC",
    }
    log.info("Pobieram dane z Open-Meteo: %s → %s", start_date, end_date)
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_records(data: dict) -> list[dict]:
    """Przekształca odpowiedź Open-Meteo w listę rekordów gotowych do INSERT."""
    hourly = data["hourly"]
    times  = hourly["time"]
    n      = len(times)

    records = []
    for i in range(n):
        wmo_code = hourly["weather_code"][i]
        main_desc, desc = WMO_DESCRIPTIONS.get(wmo_code, ("Unknown", "unknown"))

        temp_c       = hourly["temperature_2m"][i]
        feels_like_c = hourly["apparent_temperature"][i]

        # Konwersja °C → K (NULL-safe)
        temp_k       = (temp_c + 273.15)       if temp_c       is not None else None
        feels_like_k = (feels_like_c + 273.15) if feels_like_c is not None else None

        # Open-Meteo zwraca czas jako string "2026-01-01T00:00" bez strefy;
        # przy timezone=UTC w zapytaniu jest to czas UTC
        measured_at = datetime.fromisoformat(times[i]).replace(
            tzinfo=timezone.utc
        )

        snow_mm = hourly["snowfall"][i]   # Open-Meteo: cm, przeliczamy na mm
        if snow_mm is not None:
            snow_mm = snow_mm * 10

        records.append({
            "measured_at":        measured_at,
            "temp_k":             temp_k,
            "feels_like_k":       feels_like_k,
            "pressure_hpa":       hourly["pressure_msl"][i],
            "humidity_pct":       hourly["relative_humidity_2m"][i],
            "visibility_m":       hourly["visibility"][i],
            "wind_speed_ms":      hourly["wind_speed_10m"][i],
            "wind_deg":           hourly["wind_direction_10m"][i],
            "wind_gust_ms":       hourly["wind_gusts_10m"][i],
            "clouds_pct":         hourly["cloud_cover"][i],
            "rain_1h_mm":         hourly["precipitation"][i],
            "snow_1h_mm":         snow_mm,
            "weather_id":         wmo_code,
            "weather_main":       main_desc,
            "weather_description": desc,
            "lat":                float(LAT),
            "lon":                float(LON),
            "raw_payload":        None,   # brak surowego JSON (dane zagregowane)
        })

    return records

# ── Zapis do PostgreSQL ───────────────────────────────────────────────────────

def insert_records(conn, records: list[dict]) -> int:
    inserted = 0
    with conn.cursor() as cur:
        for rec in records:
            cur.execute(INSERT_SQL, rec)
            inserted += cur.rowcount
    conn.commit()
    return inserted


def main():
    log.info("=" * 60)
    log.info("Historical fetch | %s → %s | lat=%s, lon=%s",
             HISTORY_START_DATE, HISTORY_END_DATE, LAT, LON)
    log.info("=" * 60)

    # Połączenie z bazą
    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )

    try:
        data    = fetch_historical(HISTORY_START_DATE, HISTORY_END_DATE)
        records = parse_records(data)
        log.info("Pobrano %d rekordów z Open-Meteo", len(records))

        inserted = insert_records(conn, records)
        skipped  = len(records) - inserted
        log.info("Wstawiono: %d | Pominięto (duplikaty): %d", inserted, skipped)

    finally:
        conn.close()

    log.info("Gotowe.")


if __name__ == "__main__":
    main()
