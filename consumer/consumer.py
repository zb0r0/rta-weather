"""
Weather Consumer (Kafka → PostgreSQL)
--------------------------------------
Konsumuje wiadomości z topiku 'weather-raw', parsuje payload OWM
i zapisuje do tabeli weather_raw w PostgreSQL.
"""

import json
import logging
import os
import time

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

# ── INSERT SQL ────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO weather_raw (
    measured_at,
    temp_k, feels_like_k, temp_min_k, temp_max_k,
    pressure_hpa, humidity_pct, visibility_m,
    wind_speed_ms, wind_deg, wind_gust_ms,
    clouds_pct,
    rain_1h_mm, snow_1h_mm,
    weather_id, weather_main, weather_description, weather_icon,
    sunrise_at, sunset_at,
    lat, lon,
    raw_payload
) VALUES (
    to_timestamp(%(measured_at)s),
    %(temp_k)s, %(feels_like_k)s, %(temp_min_k)s, %(temp_max_k)s,
    %(pressure_hpa)s, %(humidity_pct)s, %(visibility_m)s,
    %(wind_speed_ms)s, %(wind_deg)s, %(wind_gust_ms)s,
    %(clouds_pct)s,
    %(rain_1h_mm)s, %(snow_1h_mm)s,
    %(weather_id)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s,
    to_timestamp(%(sunrise_at)s), to_timestamp(%(sunset_at)s),
    %(lat)s, %(lon)s,
    %(raw_payload)s
)
"""

# ── Parsowanie payloadu OWM ───────────────────────────────────────────────────

def parse_owm_payload(data: dict) -> dict:
    """Przekształca surowy JSON z OWM na słownik gotowy do INSERT."""
    main      = data.get("main", {})
    wind      = data.get("wind", {})
    clouds    = data.get("clouds", {})
    rain      = data.get("rain", {})
    snow      = data.get("snow", {})
    sys       = data.get("sys", {})
    coord     = data.get("coord", {})
    weather   = data.get("weather", [{}])[0]

    return {
        "measured_at":       data.get("dt"),
        # temperatura
        "temp_k":            main.get("temp"),
        "feels_like_k":      main.get("feels_like"),
        "temp_min_k":        main.get("temp_min"),
        "temp_max_k":        main.get("temp_max"),
        # atmosfera
        "pressure_hpa":      main.get("pressure"),
        "humidity_pct":      main.get("humidity"),
        "visibility_m":      data.get("visibility"),
        # wiatr
        "wind_speed_ms":     wind.get("speed"),
        "wind_deg":          wind.get("deg"),
        "wind_gust_ms":      wind.get("gust"),
        # zachmurzenie i opady
        "clouds_pct":        clouds.get("all"),
        "rain_1h_mm":        rain.get("1h"),
        "snow_1h_mm":        snow.get("1h"),
        # opis
        "weather_id":        weather.get("id"),
        "weather_main":      weather.get("main"),
        "weather_description": weather.get("description"),
        "weather_icon":      weather.get("icon"),
        # słońce
        "sunrise_at":        sys.get("sunrise"),
        "sunset_at":         sys.get("sunset"),
        # lokalizacja
        "lat":               coord.get("lat"),
        "lon":               coord.get("lon"),
        # surowy payload jako JSONB
        "raw_payload":       json.dumps(data),
    }

# ── Połączenie z bazą ─────────────────────────────────────────────────────────

def build_db_connection():
    """Łączy się z PostgreSQL z retry logic."""
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False
            log.info("Połączono z PostgreSQL: %s/%s", POSTGRES_HOST, POSTGRES_DB)
            return conn
        except psycopg2.OperationalError as e:
            log.warning("Próba %d/10 połączenia z PostgreSQL nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć się z PostgreSQL po 10 próbach.")


def build_consumer() -> KafkaConsumer:
    """Tworzy KafkaConsumer z retry logic."""
    for attempt in range(1, 11):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_RAW,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=False,       # ręczny commit po zapisie do DB
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            log.info("Połączono z Kafka, subskrypcja: %s", KAFKA_TOPIC_RAW)
            return consumer
        except KafkaError as e:
            log.warning("Próba %d/10 połączenia z Kafka nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć się z Kafka po 10 próbach.")

# ── Główna pętla ─────────────────────────────────────────────────────────────

def main():
    log.info("Uruchamiam konsumenta | topic=%s, group=%s", KAFKA_TOPIC_RAW, KAFKA_GROUP_ID)

    conn     = build_db_connection()
    consumer = build_consumer()

    for message in consumer:
        try:
            raw_data = message.value
            log.debug("Odebrano wiadomość: offset=%d", message.offset)

            record = parse_owm_payload(raw_data)

            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, record)
            conn.commit()

            # Commit offsetu do Kafka dopiero po pomyślnym zapisie do DB
            consumer.commit()

            log.info(
                "Zapisano rekord: measured_at=%s, temp_c=%.2f°C, pressure=%s hPa",
                record["measured_at"],
                (record["temp_k"] or 0) - 273.15,
                record["pressure_hpa"],
            )

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
            # Nie commitujemy offsetu — wiadomość zostanie przetworzona ponownie
            conn.rollback()


if __name__ == "__main__":
    main()
