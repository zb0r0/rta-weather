"""
Historical Anomaly Scan — PostgreSQL weather_raw → weather_alerts
-----------------------------------------------------------------
Jednorazowy skrypt który przechodzi przez wszystkie dane historyczne
w tabeli weather_raw i uruchamia na nich te same reguły detekcji anomalii
co stream_processor.py.

Wyniki trafiają do tabeli weather_alerts — ta sama tabela, ten sam schemat.
Duplikaty są pomijane (ten sam alert_type + measured_at nie zostanie wstawiony dwa razy).

Uruchomienie (stack musi działać):
  docker compose run --rm stream-processor python historical_anomaly_scan.py

Lub lokalnie (z ustawionymi zmiennymi środowiskowymi):
  python historical_anomaly_scan.py
"""

import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from statistics import mean, stdev

import psycopg2
import psycopg2.extras

# ── Konfiguracja logowania ─────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Zmienne środowiskowe ───────────────────────────────────────────────────────

POSTGRES_HOST     = os.environ["POSTGRES_HOST"]
POSTGRES_PORT     = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB       = os.environ["POSTGRES_DB"]
POSTGRES_USER     = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]

# ── Progi — identyczne jak w stream_processor.py ──────────────────────────────

WINDOW_SIZE          = 20    # ile poprzednich pomiarów liczymy do średniej/odchylenia
TEMP_SIGMA_THRESHOLD = 2.0   # próg anomalii temperatury w odchyleniach standardowych
PRESSURE_DELTA_HPA   = 5.0   # max skok ciśnienia między sąsiednimi pomiarami [hPa]
WIND_SPEED_THRESHOLD = 15.0  # próg prędkości wiatru [m/s]
VISIBILITY_THRESHOLD = 1000  # próg widoczności [m]

# ── Zapytanie pobierające dane historyczne ─────────────────────────────────────
# Pobieramy tylko kolumny potrzebne do detekcji, sortujemy chronologicznie
# żeby sliding window miał sens

SELECT_SQL = """
SELECT
    measured_at,
    temp_c,
    pressure_hpa,
    wind_speed_ms,
    visibility_m
FROM weather_raw
ORDER BY measured_at ASC
"""

# ── INSERT alertu — pomija duplikaty (ten sam typ + czas pomiaru) ──────────────

INSERT_ALERT_SQL = """
INSERT INTO weather_alerts (
    alert_type, severity, message,
    measured_at, trigger_value, threshold_value
)
SELECT
    %(alert_type)s, %(severity)s, %(message)s,
    %(measured_at)s, %(trigger_value)s, %(threshold_value)s
WHERE NOT EXISTS (
    SELECT 1 FROM weather_alerts
    WHERE alert_type  = %(alert_type)s
      AND measured_at = %(measured_at)s
)
"""

# ── Reguły detekcji — skopiowane 1:1 z stream_processor.py ───────────────────

def check_temp_anomaly(temp_window: deque, current_temp: float) -> dict | None:
    """Reguła 1: temperatura odbiega od średniej kroczącej o więcej niż 2σ."""
    if len(temp_window) < 3 or current_temp is None:
        return None

    avg = mean(temp_window)
    sd  = stdev(temp_window)

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
    """Reguła 2: nagły skok/spadek ciśnienia > 5 hPa względem poprzedniego pomiaru."""
    if len(pressure_window) < 1 or current_pressure is None:
        return None

    previous_pressure = pressure_window[-1]
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
    """Reguła 3: prędkość wiatru przekracza próg 15 m/s."""
    if wind_speed is None:
        return None

    if wind_speed > WIND_SPEED_THRESHOLD:
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
    """Reguła 4: widoczność spada poniżej 1000 m."""
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

# ── Główna funkcja ─────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Historical Anomaly Scan — start")
    log.info("=" * 60)

    conn = psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT,
        dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    )

    # Pobieramy wszystkie rekordy z weather_raw posortowane chronologicznie
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(SELECT_SQL)
        rows = cur.fetchall()

    log.info("Pobrano %d rekordów z weather_raw do analizy", len(rows))

    temp_window     = deque(maxlen=WINDOW_SIZE)
    pressure_window = deque(maxlen=WINDOW_SIZE)

    total_alerts = 0
    total_skipped = 0

    start_time = time.monotonic()

    for i, row in enumerate(rows):
        measured_at  = row["measured_at"]
        temp_c       = float(row["temp_c"])       if row["temp_c"]       is not None else None
        pressure_hpa = float(row["pressure_hpa"]) if row["pressure_hpa"] is not None else None
        wind_speed   = float(row["wind_speed_ms"]) if row["wind_speed_ms"] is not None else None
        visibility   = int(row["visibility_m"])   if row["visibility_m"] is not None else None

        # Zbieramy alerty z 4 reguł
        alerts = [
            check_temp_anomaly(temp_window,      temp_c),
            check_pressure_drop(pressure_window, pressure_hpa),
            check_high_wind(wind_speed),
            check_low_visibility(visibility),
        ]
        alerts = [a for a in alerts if a is not None]

        # Zapisujemy alerty do bazy — INSERT pomija duplikaty przez WHERE NOT EXISTS
        with conn.cursor() as cur:
            for alert in alerts:
                record = {**alert, "measured_at": measured_at}
                cur.execute(INSERT_ALERT_SQL, record)
                if cur.rowcount:
                    total_alerts += 1
                    log.info(
                        "ALERT [%s/%s] @ %s: %s",
                        alert["alert_type"], alert["severity"],
                        measured_at, alert["message"],
                    )
                else:
                    total_skipped += 1

        conn.commit()

        # Aktualizujemy okna PO sprawdzeniu anomalii
        if temp_c is not None:
            temp_window.append(temp_c)
        if pressure_hpa is not None:
            pressure_window.append(pressure_hpa)

        # Logujemy postęp co 500 rekordów
        if (i + 1) % 500 == 0:
            elapsed = time.monotonic() - start_time
            log.info(
                "Postęp: %d/%d rekordów (%.1fs) | alerty: %d",
                i + 1, len(rows), elapsed, total_alerts,
            )

    elapsed = time.monotonic() - start_time
    log.info("=" * 60)
    log.info(
        "Gotowe: przeanalizowano %d rekordów w %.1fs",
        len(rows), elapsed,
    )
    log.info("Nowe alerty:     %d", total_alerts)
    log.info("Pominięte (już istniały): %d", total_skipped)
    log.info("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
