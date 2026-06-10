"""
Stream Processor — Kafka weather-raw → anomaly detection → weather-alerts + PostgreSQL
---------------------------------------------------------------------------------------
Subskrybuje topik 'weather-raw', analizuje strumień danych w czasie rzeczywistym
i wykrywa anomalie pogodowe. Wykryte alerty trafiają jednocześnie do:
  - topiku Kafka 'weather-alerts' (dla innych konsumentów w przyszłości)
  - tabeli 'weather_alerts' w PostgreSQL (dla Grafany i dashboardu)

Wykrywane anomalie:
  1. TEMP_ANOMALY    — temperatura odbiega od średniej kroczącej o więcej niż 2σ
  2. PRESSURE_DROP   — nagły skok/spadek ciśnienia > 5 hPa względem poprzedniego pomiaru
  3. HIGH_WIND       — prędkość wiatru przekracza 15 m/s
  4. LOW_VISIBILITY  — widoczność spada poniżej 1000 m (mgła)
"""

import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from statistics import mean, stdev

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

# ── Konfiguracja logowania ─────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Zmienne środowiskowe ───────────────────────────────────────────────────────
# Wszystkie parametry są przekazywane przez Docker Compose z .env

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_RAW         = os.environ.get("KAFKA_TOPIC_RAW",    "weather-raw")
KAFKA_TOPIC_ALERTS      = os.environ.get("KAFKA_TOPIC_ALERTS", "weather-alerts")
# Osobna grupa konsumentów — niezależna od consumer.py, przetwarza od początku
KAFKA_GROUP_ID          = os.environ.get("KAFKA_GROUP_ID",     "weather-stream-processor-group")

POSTGRES_HOST     = os.environ["POSTGRES_HOST"]
POSTGRES_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.environ["POSTGRES_DB"]
POSTGRES_USER     = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# ── Progi dla reguł anomalii ───────────────────────────────────────────────────

WINDOW_SIZE          = 20    # ile ostatnich pomiarów trzymamy w oknie
TEMP_SIGMA_THRESHOLD = 2.0   # odchylenia standardowe dla anomalii temperatury
PRESSURE_DELTA_HPA   = 5.0   # max dopuszczalny skok ciśnienia [hPa] między pomiarami
WIND_SPEED_THRESHOLD = 15.0  # próg prędkości wiatru [m/s]
VISIBILITY_THRESHOLD = 1000  # próg widoczności [m]

# ── Słownik kodów WMO (taki sam jak w consumer.py) ────────────────────────────

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

# ── Parser payloadu Open-Meteo ─────────────────────────────────────────────────

def parse_payload(data: dict) -> dict:
    """
    Wyciąga interesujące nas pola z surowego JSON Open-Meteo.
    Zwraca płaski słownik z wartościami liczbowymi i znacznikiem czasu.
    """
    current = data.get("current", {})

    # Parsowanie czasu pomiaru — producer odpytuje Open-Meteo z timezone=UTC
    measured_at_str = current.get("time", "")
    try:
        measured_at = datetime.fromisoformat(measured_at_str).replace(tzinfo=timezone.utc)
    except ValueError:
        measured_at = datetime.now(timezone.utc)
        log.warning("Nie można sparsować czasu '%s' — używam NOW()", measured_at_str)

    temp_c = current.get("temperature_2m")

    return {
        "measured_at":   measured_at,
        "temp_c":        temp_c,
        "pressure_hpa":  current.get("pressure_msl"),
        "wind_speed_ms": current.get("wind_speed_10m"),
        "visibility_m":  current.get("visibility"),
    }

# ── Reguły wykrywania anomalii ─────────────────────────────────────────────────
# Każda funkcja zwraca słownik alertu lub None jeśli nie ma anomalii.
# Słownik alertu odpowiada kolumnom tabeli weather_alerts.

def check_temp_anomaly(temp_window: deque, current_temp: float) -> dict | None:
    """
    Reguła 1: temperatura odbiega od średniej kroczącej o więcej niż 2σ.
    Wymaga co najmniej 3 pomiarów w oknie żeby odchylenie miało sens.
    """
    if len(temp_window) < 3 or current_temp is None:
        return None

    avg = mean(temp_window)
    sd  = stdev(temp_window)

    # Gdy sd = 0 (wszystkie wartości identyczne) nie ma anomalii
    if sd == 0:
        return None

    deviation = abs(current_temp - avg)
    if deviation > TEMP_SIGMA_THRESHOLD * sd:
        direction = "wzrost" if current_temp > avg else "spadek"
        return {
            "alert_type":      "TEMP_ANOMALY",
            "severity":        "WARNING",
            "message":         (
                f"Anomalia temperatury: {current_temp:.1f}°C — {direction} "
                f"o {deviation:.1f}°C ({deviation/sd:.1f}σ) od średniej kroczącej {avg:.1f}°C"
            ),
            "trigger_value":   round(current_temp, 3),
            "threshold_value": round(avg + TEMP_SIGMA_THRESHOLD * sd, 3),
        }
    return None


def check_pressure_drop(pressure_window: deque, current_pressure: float) -> dict | None:
    """
    Reguła 2: nagły skok lub spadek ciśnienia > 5 hPa względem poprzedniego pomiaru.
    Porównujemy tylko z bezpośrednio poprzednim pomiarem (nie średnią).
    """
    if len(pressure_window) < 1 or current_pressure is None:
        return None

    previous_pressure = pressure_window[-1]  # ostatnia zapisana wartość
    delta = current_pressure - previous_pressure

    if abs(delta) > PRESSURE_DELTA_HPA:
        direction = "wzrost" if delta > 0 else "spadek"
        return {
            "alert_type":      "PRESSURE_DROP",
            "severity":        "WARNING",
            "message":         (
                f"Nagły {direction} ciśnienia: {previous_pressure:.1f} → "
                f"{current_pressure:.1f} hPa (Δ{delta:+.1f} hPa)"
            ),
            "trigger_value":   round(abs(delta), 3),
            "threshold_value": PRESSURE_DELTA_HPA,
        }
    return None


def check_high_wind(wind_speed: float) -> dict | None:
    """
    Reguła 3: prędkość wiatru przekracza próg 15 m/s.
    Próg odpowiada silnemu wiatrowi (skala Beaufort: stopień 7+).
    """
    if wind_speed is None:
        return None

    if wind_speed > WIND_SPEED_THRESHOLD:
        # CRITICAL powyżej 25 m/s (orkan), WARNING powyżej 15 m/s
        severity = "CRITICAL" if wind_speed > 25.0 else "WARNING"
        return {
            "alert_type":      "HIGH_WIND",
            "severity":        severity,
            "message":         f"Silny wiatr: {wind_speed:.1f} m/s (próg: {WIND_SPEED_THRESHOLD} m/s)",
            "trigger_value":   round(wind_speed, 3),
            "threshold_value": WIND_SPEED_THRESHOLD,
        }
    return None


def check_low_visibility(visibility: int) -> dict | None:
    """
    Reguła 4: widoczność spada poniżej 1000 m (warunki mgłowe).
    """
    if visibility is None:
        return None

    if visibility < VISIBILITY_THRESHOLD:
        severity = "CRITICAL" if visibility < 200 else "WARNING"
        return {
            "alert_type":      "LOW_VISIBILITY",
            "severity":        severity,
            "message":         f"Niska widoczność: {visibility} m (próg: {VISIBILITY_THRESHOLD} m)",
            "trigger_value":   float(visibility),
            "threshold_value": float(VISIBILITY_THRESHOLD),
        }
    return None

# ── Zapis alertu do PostgreSQL ─────────────────────────────────────────────────

INSERT_ALERT_SQL = """
INSERT INTO weather_alerts (
    alert_type, severity, message,
    measured_at, trigger_value, threshold_value
) VALUES (
    %(alert_type)s, %(severity)s, %(message)s,
    %(measured_at)s, %(trigger_value)s, %(threshold_value)s
)
"""

def save_alert_to_db(conn, alert: dict, measured_at: datetime):
    """Zapisuje jeden alert do tabeli weather_alerts w PostgreSQL."""
    record = {**alert, "measured_at": measured_at}
    with conn.cursor() as cur:
        cur.execute(INSERT_ALERT_SQL, record)
    conn.commit()

# ── Budowanie połączeń ─────────────────────────────────────────────────────────

def build_db_connection():
    """Próbuje połączyć się z PostgreSQL do 10 razy co 5 sekund."""
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


def build_kafka_consumer() -> KafkaConsumer:
    """
    Tworzy konsumenta Kafki subskrybującego weather-raw.
    Osobna grupa konsumentów = niezależny offset od consumer.py.
    """
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
            log.info("Konsument Kafka połączony, topik: %s", KAFKA_TOPIC_RAW)
            return consumer
        except KafkaError as e:
            log.warning("Próba %d/10 połączenia z Kafka nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć się z Kafka po 10 próbach.")


def build_kafka_producer() -> KafkaProducer:
    """Tworzy producenta Kafki do publikowania alertów do weather-alerts."""
    for attempt in range(1, 11):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Producent Kafka połączony, topik alertów: %s", KAFKA_TOPIC_ALERTS)
            return producer
        except KafkaError as e:
            log.warning("Próba %d/10 połączenia producenta z Kafka nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć producenta z Kafka po 10 próbach.")

# ── Główna pętla ───────────────────────────────────────────────────────────────

def main():
    log.info(
        "Uruchamiam stream processor | in=%s, out=%s, group=%s",
        KAFKA_TOPIC_RAW, KAFKA_TOPIC_ALERTS, KAFKA_GROUP_ID,
    )

    conn     = build_db_connection()
    consumer = build_kafka_consumer()
    producer = build_kafka_producer()

    # Okna czasowe (sliding windows) — przechowują ostatnie WINDOW_SIZE wartości
    # deque z maxlen automatycznie usuwa najstarszy element gdy jest pełne
    temp_window     = deque(maxlen=WINDOW_SIZE)  # temperatura [°C]
    pressure_window = deque(maxlen=WINDOW_SIZE)  # ciśnienie [hPa]

    for message in consumer:
        try:
            raw_data    = message.value
            record      = parse_payload(raw_data)
            measured_at = record["measured_at"]

            log.debug(
                "Odebrano: measured_at=%s, temp=%.1f°C, pressure=%.1f hPa",
                measured_at,
                record["temp_c"] or 0,
                record["pressure_hpa"] or 0,
            )

            # Zbieramy wszystkie wykryte alerty dla tego pomiaru
            alerts = []

            # Sprawdzamy 4 reguły — okna muszą być wypełnione przed oceną
            alerts.append(check_temp_anomaly(temp_window,      record["temp_c"]))
            alerts.append(check_pressure_drop(pressure_window, record["pressure_hpa"]))
            alerts.append(check_high_wind(record["wind_speed_ms"]))
            alerts.append(check_low_visibility(record["visibility_m"]))

            # Filtrujemy None (reguły które nie wykryły anomalii)
            alerts = [a for a in alerts if a is not None]

            # Dla każdego wykrytego alertu: zapisz do bazy i opublikuj na Kafka
            for alert in alerts:
                log.warning(
                    "ALERT [%s/%s]: %s",
                    alert["alert_type"], alert["severity"], alert["message"],
                )

                # Zapis do PostgreSQL (tabela weather_alerts)
                try:
                    save_alert_to_db(conn, alert, measured_at)
                except (psycopg2.Error, psycopg2.OperationalError) as e:
                    log.error("Błąd zapisu alertu do PostgreSQL: %s — rekonekt", e)
                    conn.rollback()
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = build_db_connection()
                    save_alert_to_db(conn, alert, measured_at)

                # Publikacja alertu do topiku Kafka weather-alerts
                kafka_payload = {
                    **alert,
                    "measured_at":  measured_at.isoformat(),
                    "published_at": datetime.now(timezone.utc).isoformat(),
                }
                producer.send(KAFKA_TOPIC_ALERTS, value=kafka_payload)

            if alerts:
                producer.flush()
            else:
                log.info(
                    "Pomiar OK: measured_at=%s, temp=%.1f°C, pressure=%.1f hPa, wind=%.1f m/s",
                    measured_at,
                    record["temp_c"] or 0,
                    record["pressure_hpa"] or 0,
                    record["wind_speed_ms"] or 0,
                )

            # Aktualizujemy okna sliding window PO sprawdzeniu anomalii
            # (żeby bieżący pomiar nie wpływał na własną detekcję)
            if record["temp_c"] is not None:
                temp_window.append(record["temp_c"])
            if record["pressure_hpa"] is not None:
                pressure_window.append(record["pressure_hpa"])

            # Potwierdzamy przetworzenie wiadomości Kafka
            consumer.commit()

        except Exception as e:
            log.exception("Nieoczekiwany błąd przy przetwarzaniu wiadomości: %s", e)
            try:
                conn.rollback()
            except Exception:
                pass


if __name__ == "__main__":
    main()
