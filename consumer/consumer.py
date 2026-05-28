"""
Weather Consumer — Kafka → PostgreSQL
--------------------------------------
Konsumuje wiadomości z topiku 'weather-raw' (format Open-Meteo current),
parsuje payload i zapisuje do tabeli weather_raw w PostgreSQL.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ── Konfiguracja ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_RAW         = os.environ.get("KAFKA_TOPIC_RAW", "weather-raw")
KAFKA_GROUP_ID          = os.environ.get("KAFKA_GROUP_ID", "weather-consumer-group")

POSTGRES_HOST           = os.environ["POSTGRES_HOST"]
POSTGRES_PORT           = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB             = os.environ["POSTGRES_DB"]
POSTGRES_USER           = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD       = os.environ["POSTGRES_PASSWORD"]

# ── Kody WMO → opis (taki sam słownik jak w historical_fetch.py) ──────────────

WMO_DESCRIPTIONS = {
    0:  ("Clear",        "clear sky"),
    1:  ("Clear",        "mainly clear"),
    2:  ("Clouds",       "partly cloudy"),
    3:  ("Clouds",       "overcast"),
    45: ("Fog",          "fog"),
    48: ("Fog",          "rime fog"),
    51: ("Drizzle",      "light drizzle"),
    53: ("Drizzle",      "moderate drizzle"),
    55: ("Drizzle",      "dense drizzle"),
    61: ("Rain",         "slight rain"),
    63: ("Rain",         "moderate rain"),
    65: ("Rain",         "heavy rain"),
    71: ("Snow",         "slight snow"),
    73: ("Snow",         "moderate snow"),
    75: ("Snow",         "heavy snow"),
    77: ("Snow",         "snow grains"),
    80: ("Rain",         "slight showers"),
    81: ("Rain",         "moderate showers"),
    82: ("Rain",         "violent showers"),
    85: ("Snow",         "slight snow showers"),
    86: ("Snow",         "heavy snow showers"),
    95: ("Thunderstorm", "thunderstorm"),
    96: ("Thunderstorm", "thunderstorm with hail"),
    99: ("Thunderstorm", "thunderstorm with heavy hail"),
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
    sunrise_at, sunset_at,
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
    %(sunrise_at)s, %(sunset_at)s,
    %(lat)s, %(lon)s,
    %(raw_payload)s
WHERE NOT EXISTS (
    SELECT 1 FROM weather_raw
    WHERE measured_at = %(measured_at)s
      AND raw_payload IS NOT NULL
)
"""

# ── Parser payloadu Open-Meteo ────────────────────────────────────────────────

def parse_open_meteo_payload(data: dict) -> dict:
    """Przekształca JSON Open-Meteo current na słownik gotowy do INSERT."""
    current = data.get("current", {})
    daily   = data.get("daily", {})

    wmo_code = current.get("weather_code")
    main_desc, desc = WMO_DESCRIPTIONS.get(wmo_code, ("Unknown", "unknown"))

    temp_c       = current.get("temperature_2m")
    feels_like_c = current.get("apparent_temperature")
    temp_k       = (temp_c + 273.15)       if temp_c       is not None else None
    feels_like_k = (feels_like_c + 273.15) if feels_like_c is not None else None

    # Open-Meteo zwraca czas jako "2026-05-28T20:00" (czas lokalny strefy timezone)
    # Parsujemy bez strefy i traktujemy jako UTC (Open-Meteo daje czas lokalny Warszawy)
    measured_at_str = current.get("time", "")
    try:
        measured_at = datetime.fromisoformat(measured_at_str).replace(tzinfo=timezone.utc)
    except ValueError:
        measured_at = datetime.now(timezone.utc)
        log.warning("Nie można sparsować czasu: %s — używam NOW()", measured_at_str)

    # Sunrise/sunset — Open-Meteo zwraca listę (jeden element na dziś)
    sunrise_str = (daily.get("sunrise") or [None])[0]
    sunset_str  = (daily.get("sunset")  or [None])[0]

    def parse_dt(s):
        if not s:
            return None
        try:
            return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
        except ValueError:
            return None

    return {
        "measured_at":         measured_at,
        "temp_k":              temp_k,
        "feels_like_k":        feels_like_k,
        "pressure_hpa":        current.get("pressure_msl"),
        "humidity_pct":        current.get("relative_humidity_2m"),
        "visibility_m":        current.get("visibility"),
        "wind_speed_ms":       current.get("wind_speed_10m"),
        "wind_deg":            current.get("wind_direction_10m"),
        "wind_gust_ms":        current.get("wind_gusts_10m"),
        "clouds_pct":          current.get("cloud_cover"),
        "rain_1h_mm":          current.get("precipitation"),
        "snow_1h_mm":          None,   # Open-Meteo current nie rozdziela deszczu od śniegu
        "weather_id":          wmo_code,
        "weather_main":        main_desc,
        "weather_description": desc,
        "sunrise_at":          parse_dt(sunrise_str),
        "sunset_at":           parse_dt(sunset_str),
        "lat":                 data.get("latitude"),
        "lon":                 data.get("longitude"),
        "raw_payload":         json.dumps(data),   # pełny JSON — odróżnia od danych historycznych
    }

# ── Połączenia ────────────────────────────────────────────────────────────────

def build_db_connection():
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False
            log.info("Połączono z PostgreSQL: %s/%s", POSTGRES_HOST, POSTGRES_DB)
            return conn
        except psycopg2.OperationalError as e:
            log.warning("Próba %d/10 połączenia z PostgreSQL nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć się z PostgreSQL po 10 próbach.")


def build_consumer() -> KafkaConsumer:
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_RAW,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            log.info("Połączono z Kafka, subskrypcja: %s", KAFKA_TOPIC_RAW)
            return consumer
        except KafkaError as e:
            log.warning("Próba %d/10 połączenia z Kafka nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć się z Kafka po 10 próbach.")

# ── Główna pętla ──────────────────────────────────────────────────────────────

def main():
    log.info("Uruchamiam konsumenta | topic=%s, group=%s", KAFKA_TOPIC_RAW, KAFKA_GROUP_ID)

    conn     = build_db_connection()
    consumer = build_consumer()

    for message in consumer:
        try:
            raw_data = message.value
            log.debug("Odebrano wiadomość: offset=%d", message.offset)

            record = parse_open_meteo_payload(raw_data)

            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, record)
                inserted = cur.rowcount

            conn.commit()
            consumer.commit()

            if inserted:
                log.info(
                    "Zapisano: measured_at=%s, temp=%.1f°C, pressure=%.1f hPa, %s",
                    record["measured_at"],
                    (record["temp_k"] or 273.15) - 273.15,
                    record["pressure_hpa"] or 0,
                    record["weather_description"],
                )
            else:
                log.info("Pominięto duplikat: measured_at=%s", record["measured_at"])

        except (psycopg2.Error, psycopg2.OperationalError) as e:
            log.error("Błąd PostgreSQL: %s — próba ponownego połączenia", e)
            conn.rollback()
            try:
                conn.close()
            except Exception:
                pass
            conn = build_db_connection()

        except Exception as e:
            log.exception("Nieoczekiwany błąd przy przetwarzaniu wiadomości: %s", e)
            conn.rollback()


if __name__ == "__main__":
    main()
