"""
Weather Producer
----------------
Co FETCH_INTERVAL_SECONDS sekund odpytuje OpenWeatherMap API
i publikuje surowy JSON do topiku Kafka 'weather-raw'.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Konfiguracja ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_RAW         = os.environ.get("KAFKA_TOPIC_RAW", "weather-raw")

OWM_API_KEY             = os.environ["OWM_API_KEY"]
OWM_LAT                 = os.environ.get("OWM_LAT", "52.25")
OWM_LON                 = os.environ.get("OWM_LON", "21.0")
OWM_URL                 = "https://api.openweathermap.org/data/2.5/weather"

FETCH_INTERVAL_SECONDS  = int(os.environ.get("FETCH_INTERVAL_SECONDS", "60"))

# ── Funkcje pomocnicze ───────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    """Tworzy KafkaProducer z retry logic."""
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Gwarancja dostarczenia: czekaj na potwierdzenie od lidera
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
    """Pobiera aktualne dane pogodowe z OWM API."""
    params = {
        "lat":   OWM_LAT,
        "lon":   OWM_LON,
        "appid": OWM_API_KEY,
    }
    response = requests.get(OWM_URL, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()

    # Dodajemy znacznik czasu pobrania (po stronie producenta)
    data["_producer_ts"] = datetime.now(timezone.utc).isoformat()

    return data


def on_send_success(metadata):
    log.info(
        "Opublikowano → topic=%s, partition=%d, offset=%d",
        metadata.topic, metadata.partition, metadata.offset,
    )


def on_send_error(exc):
    log.error("Błąd publikowania do Kafka: %s", exc)


# ── Główna pętla ─────────────────────────────────────────────────────────────

def main():
    log.info(
        "Uruchamiam producenta | lat=%s, lon=%s, interval=%ds, topic=%s",
        OWM_LAT, OWM_LON, FETCH_INTERVAL_SECONDS, KAFKA_TOPIC_RAW,
    )

    producer = build_producer()

    while True:
        start = time.monotonic()

        try:
            data = fetch_weather()
            log.info(
                "Pobrano dane: temp=%.2fK, pressure=%s hPa, humidity=%s%%",
                data.get("main", {}).get("temp", 0),
                data.get("main", {}).get("pressure"),
                data.get("main", {}).get("humidity"),
            )

            producer.send(KAFKA_TOPIC_RAW, value=data) \
                    .add_callback(on_send_success) \
                    .add_errback(on_send_error)

            # Flush żeby nie trzymać w buforze przy małym ruchu
            producer.flush()

        except requests.exceptions.RequestException as e:
            log.error("Błąd HTTP przy pobieraniu pogody: %s", e)
        except Exception as e:
            log.exception("Nieoczekiwany błąd: %s", e)

        # Precyzyjne czekanie: odejmujemy czas wykonania
        elapsed = time.monotonic() - start
        sleep_for = max(0, FETCH_INTERVAL_SECONDS - elapsed)
        log.debug("Następne pobranie za %.1fs", sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
