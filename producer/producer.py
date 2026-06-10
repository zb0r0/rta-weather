"""
Weather Producer — Open-Meteo current API → Kafka
--------------------------------------------------
Co FETCH_INTERVAL_SECONDS sekund pobiera aktualne dane pogodowe
z Open-Meteo (darmowe, bez klucza API, aktualizacja co ~15 min)
i publikuje surowy JSON do topiku Kafka 'weather-raw'.

Endpoint aktualizuje dane co 900 sekund (15 min), dlatego domyślny
interwał pobierania ustawiony jest na 900s — odpytywanie częściej
nie przynosi nowych danych.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Konfiguracja ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_RAW         = os.environ.get("KAFKA_TOPIC_RAW", "weather-raw")

LAT                     = os.environ.get("OWM_LAT", "52.25")
LON                     = os.environ.get("OWM_LON", "21.0")
FETCH_INTERVAL_SECONDS  = int(os.environ.get("FETCH_INTERVAL_SECONDS", "900"))

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# ── Funkcje pomocnicze ────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Połączono z Kafka: %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaError as e:
            log.warning("Próba %d/10 połączenia z Kafka nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć się z Kafka po 10 próbach.")


def fetch_weather() -> dict:
    """Pobiera aktualne dane pogodowe z Open-Meteo."""
    params = {
        "latitude":  LAT,
        "longitude": LON,
        "current": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "apparent_temperature",
            "precipitation",
            "weather_code",
            "pressure_msl",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "cloud_cover",
            "visibility",
        ]),
        "daily":           "sunrise,sunset",
        "wind_speed_unit": "ms",
        # UTC, nie Europe/Warsaw — konsumenci parsują "time" jako UTC
        # (.replace(tzinfo=timezone.utc)); czas lokalny przesuwałby pomiary o 1-2h
        "timezone":        "UTC",
        "forecast_days":   1,
    }
    response = requests.get(OPEN_METEO_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Dodajemy znacznik czasu pobrania po stronie producenta
    data["_producer_ts"] = datetime.now(timezone.utc).isoformat()
    data["_source"]      = "open-meteo-current"

    return data


def on_send_success(metadata):
    log.info(
        "Opublikowano → topic=%s, partition=%d, offset=%d",
        metadata.topic, metadata.partition, metadata.offset,
    )


def on_send_error(exc):
    log.error("Błąd publikowania do Kafka: %s", exc)


# ── Główna pętla ──────────────────────────────────────────────────────────────

def main():
    log.info(
        "Uruchamiam producenta | lat=%s, lon=%s, interval=%ds, topic=%s",
        LAT, LON, FETCH_INTERVAL_SECONDS, KAFKA_TOPIC_RAW,
    )
    log.info("Źródło: Open-Meteo (aktualizacja co ~15 min)")

    producer = build_producer()

    while True:
        start = time.monotonic()

        try:
            data    = fetch_weather()
            current = data.get("current", {})
            log.info(
                "Pobrano: time=%s, temp=%.1f°C, pressure=%.1f hPa, humidity=%s%%",
                current.get("time"),
                current.get("temperature_2m", 0),
                current.get("pressure_msl", 0),
                current.get("relative_humidity_2m"),
            )

            producer.send(KAFKA_TOPIC_RAW, value=data) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            producer.flush()

        except requests.exceptions.RequestException as e:
            log.error("Błąd HTTP przy pobieraniu pogody: %s", e)
        except Exception as e:
            log.exception("Nieoczekiwany błąd: %s", e)

        elapsed   = time.monotonic() - start
        sleep_for = max(0, FETCH_INTERVAL_SECONDS - elapsed)
        log.debug("Następne pobranie za %.0fs", sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
