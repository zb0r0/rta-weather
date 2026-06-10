"""
Inference Service — serwis predykcji temperatury
-------------------------------------------------------
Ładuje modele XGBoost (1h/3h/6h/24h) i co PREDICT_INTERVAL_SECONDS sekund
generuje predykcje na podstawie najnowszych danych z PostgreSQL.
Wyniki zapisuje do tabeli weather_predictions.

Endpoints HTTP (FastAPI):
  GET /health  — status serwisu
  GET /predict — uruchom predykcję na żądanie i zwróć wyniki
"""

import logging
import os
import time
from datetime import timedelta
from threading import Thread

import joblib
import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import uvicorn
from fastapi import FastAPI

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

MODELS_DIR               = os.environ.get("MODELS_DIR", "/app/models")
PREDICT_INTERVAL_SECONDS = int(os.environ.get("PREDICT_INTERVAL_SECONDS", "3600"))
TARGET_HORIZONS          = [1, 3, 6, 24]
MODEL_NAME               = "xgboost"
MODEL_VERSION            = "1.0"

# ── Modele i cechy (globalne, ładowane raz przy starcie) ───────────────────────
models: dict = {}
feature_cols: list = []


def load_models() -> None:
    global models, feature_cols
    feature_cols = joblib.load(os.path.join(MODELS_DIR, "feature_cols.pkl"))
    log.info("Załadowano listę cech: %d cech", len(feature_cols))
    for h in TARGET_HORIZONS:
        path = os.path.join(MODELS_DIR, f"xgboost_temp_{h}h.pkl")
        models[h] = joblib.load(path)
        log.info("Załadowano model: xgboost_temp_%dh", h)


# ── Połączenie z PostgreSQL ────────────────────────────────────────────────────
def build_db_connection():
    for attempt in range(1, 11):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST, port=POSTGRES_PORT,
                dbname=POSTGRES_DB, user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )
            conn.autocommit = False
            log.info("Połączono z PostgreSQL: %s/%s", POSTGRES_HOST, POSTGRES_DB)
            return conn
        except psycopg2.OperationalError as e:
            log.warning("Próba %d/10 połączenia z PostgreSQL nieudana: %s", attempt, e)
            time.sleep(5)
    raise RuntimeError("Nie można połączyć się z PostgreSQL po 10 próbach.")


# ── Pobieranie danych z bazy ───────────────────────────────────────────────────
# Pobieramy 55 godzin wstecz — po resamplu do godzin daje ~55 wierszy,
# z czego 48 potrzebujemy na lag_48h, 6 na delta_6h i 24 na rolling_24h.
FETCH_QUERY = """
SELECT
    measured_at,
    temp_c,
    feels_like_c,
    pressure_hpa,
    humidity_pct,
    wind_speed_ms,
    wind_deg,
    wind_gust_ms,
    clouds_pct,
    rain_1h_mm,
    snow_1h_mm
FROM weather_raw
WHERE measured_at >= NOW() - INTERVAL '55 hours'
ORDER BY measured_at ASC
"""


def fetch_recent_data(conn) -> pd.DataFrame:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(FETCH_QUERY)
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[
        "measured_at", "temp_c", "feels_like_c", "pressure_hpa",
        "humidity_pct", "wind_speed_ms", "wind_deg", "wind_gust_ms",
        "clouds_pct", "rain_1h_mm", "snow_1h_mm",
    ])
    df["measured_at"] = pd.to_datetime(df["measured_at"], utc=True)
    df = df.set_index("measured_at")

    # Resample do godzinowych (last() per slot) — spójność z danymi treningowymi,
    # które były godzinowe (Open-Meteo archive). Dane live (co 15 min) agregowane.
    df = df.resample("h").last()
    df = df.dropna(subset=["temp_c"])
    return df


# ── Feature engineering (identyczny jak w analysis.ipynb) ─────────────────────
def build_features(df: pd.DataFrame) -> "np.ndarray | None":
    """
    Buduje wektor 59 cech dla ostatniego wiersza df.
    df musi być godzinowy, posortowany rosnąco, min. 49 wierszy.
    Zwraca tablicę (1, 59) lub None gdy za mało danych.
    """
    if len(df) < 49:
        log.warning(
            "Za mało danych: %d wierszy (potrzeba min. 49 na lag_48h)", len(df)
        )
        return None

    feat = df.copy()

    # Konwersja do czasu lokalnego Warszawy — model trenowany na tym timezone
    feat.index = feat.index.tz_convert("Europe/Warsaw")

    # Uzupełnianie braków (identycznie jak w notebooku)
    feat["rain_1h_mm"]   = feat["rain_1h_mm"].fillna(0.0)
    feat["snow_1h_mm"]   = feat["snow_1h_mm"].fillna(0.0)
    feat["wind_gust_ms"] = feat["wind_gust_ms"].fillna(feat["wind_speed_ms"])
    numeric_cols = feat.select_dtypes(include="number").columns
    feat[numeric_cols] = feat[numeric_cols].interpolate(method="time")

    # Cechy czasowe
    feat["hour"]       = feat.index.hour
    feat["dayofweek"]  = feat.index.dayofweek
    feat["month"]      = feat.index.month
    feat["dayofyear"]  = feat.index.dayofyear
    feat["is_weekend"] = (feat["dayofweek"] >= 5).astype(int)

    # Kodowanie cykliczne — godzina i miesiąc są cykliczne (23→0, Gru→Sty)
    feat["hour_sin"]  = np.sin(2 * np.pi * feat["hour"]      / 24)
    feat["hour_cos"]  = np.cos(2 * np.pi * feat["hour"]      / 24)
    feat["month_sin"] = np.sin(2 * np.pi * feat["month"]     / 12)
    feat["month_cos"] = np.cos(2 * np.pi * feat["month"]     / 12)
    feat["dow_sin"]   = np.sin(2 * np.pi * feat["dayofweek"] / 7)
    feat["dow_cos"]   = np.cos(2 * np.pi * feat["dayofweek"] / 7)

    # Lag features
    for lag in [1, 2, 3, 6, 12, 24, 48]:
        feat[f"temp_lag_{lag}h"]     = feat["temp_c"].shift(lag)
        feat[f"pressure_lag_{lag}h"] = feat["pressure_hpa"].shift(lag)
    for lag in [1, 3, 6, 24]:
        feat[f"humidity_lag_{lag}h"] = feat["humidity_pct"].shift(lag)
        feat[f"wind_lag_{lag}h"]     = feat["wind_speed_ms"].shift(lag)

    # Delta features
    feat["temp_delta_1h"]     = feat["temp_c"].diff(1)
    feat["temp_delta_3h"]     = feat["temp_c"].diff(3)
    feat["temp_delta_6h"]     = feat["temp_c"].diff(6)
    feat["pressure_delta_1h"] = feat["pressure_hpa"].diff(1)
    feat["pressure_delta_3h"] = feat["pressure_hpa"].diff(3)
    feat["humidity_delta_1h"] = feat["humidity_pct"].diff(1)
    feat["wind_delta_1h"]     = feat["wind_speed_ms"].diff(1)

    # Rolling statistics
    for window in [3, 6, 24]:
        feat[f"temp_roll_mean_{window}h"]     = feat["temp_c"].rolling(window, min_periods=1).mean()
        feat[f"temp_roll_std_{window}h"]      = feat["temp_c"].rolling(window, min_periods=2).std()
        feat[f"pressure_roll_mean_{window}h"] = feat["pressure_hpa"].rolling(window, min_periods=1).mean()

    last_row = feat.iloc[[-1]][feature_cols]

    if last_row.isnull().any().any():
        # Wypełnij ewentualne NaN medianą z okna (nie zerem — temp bliska 0 to ekstremalny mróz)
        col_medians = feat[feature_cols].median()
        last_row = last_row.fillna(col_medians)

    return last_row.values


# ── Zapis predykcji do bazy ────────────────────────────────────────────────────
INSERT_PREDICTION_SQL = """
INSERT INTO weather_predictions (
    predicted_for, horizon_hours, model_name, model_version, pred_temp_c
) VALUES (
    %(predicted_for)s, %(horizon_hours)s, %(model_name)s,
    %(model_version)s, %(pred_temp_c)s
)
"""


def save_predictions(conn, predictions: list) -> None:
    with conn.cursor() as cur:
        cur.executemany(INSERT_PREDICTION_SQL, predictions)
    conn.commit()
    log.info("Zapisano %d predykcji do bazy", len(predictions))


# ── Uzupełnianie actual_temp_c po upływie czasu predykcji ─────────────────────
UPDATE_ACTUAL_SQL = """
UPDATE weather_predictions p
SET
    actual_temp_c = r.temp_c,
    mae_temp      = ABS(p.pred_temp_c - r.temp_c)
FROM (
    SELECT DISTINCT ON (DATE_TRUNC('hour', measured_at))
        DATE_TRUNC('hour', measured_at) AS hour_bucket,
        temp_c
    FROM weather_raw
    ORDER BY DATE_TRUNC('hour', measured_at), measured_at DESC
) r
WHERE
    p.actual_temp_c IS NULL
    AND p.pred_temp_c IS NOT NULL
    AND DATE_TRUNC('hour', p.predicted_for) = r.hour_bucket
    AND p.predicted_for < NOW()
"""


def update_actuals(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(UPDATE_ACTUAL_SQL)
        updated = cur.rowcount
    conn.commit()
    if updated > 0:
        log.info("Zaktualizowano actual_temp_c dla %d predykcji", updated)


# ── Jeden cykl: pobierz → przewiduj → zapisz → zweryfikuj stare ───────────────
def run_prediction_cycle(conn) -> list:
    df = fetch_recent_data(conn)
    if df.empty:
        log.warning("Brak danych w bazie — pomijam predykcję")
        return []

    X = build_features(df)
    if X is None:
        return []

    # Czas ostatniego pomiaru (UTC) — od niego liczymy horyzonty
    now_utc = df.index[-1]
    predictions = []

    for h in TARGET_HORIZONS:
        pred_temp    = float(models[h].predict(X)[0])
        predicted_for = now_utc + timedelta(hours=h)
        predictions.append({
            "predicted_for": predicted_for,
            "horizon_hours": h,
            "model_name":    MODEL_NAME,
            "model_version": MODEL_VERSION,
            "pred_temp_c":   round(pred_temp, 3),
        })
        log.info(
            "Predykcja +%dh: %.2f°C (na %s UTC)",
            h, pred_temp, predicted_for.strftime("%Y-%m-%d %H:%M"),
        )

    save_predictions(conn, predictions)
    update_actuals(conn)

    # Zwracamy z serializowalnymi timestampami (dla /predict endpoint)
    return [
        {**p, "predicted_for": p["predicted_for"].isoformat()}
        for p in predictions
    ]


# ── Pętla predykcji działająca w tle ──────────────────────────────────────────
_last_predictions: list = []


def prediction_loop() -> None:
    global _last_predictions
    conn = build_db_connection()

    while True:
        try:
            _last_predictions = run_prediction_cycle(conn)
        except psycopg2.OperationalError as e:
            log.error("Utracono połączenie z PostgreSQL: %s — rekonekt", e)
            try:
                conn.close()
            except Exception:
                pass
            conn = build_db_connection()
        except Exception as e:
            log.exception("Nieoczekiwany błąd w cyklu predykcji: %s", e)

        log.info("Następna predykcja za %d s", PREDICT_INTERVAL_SECONDS)
        time.sleep(PREDICT_INTERVAL_SECONDS)


# ── FastAPI ────────────────────────────────────────────────────────────────────
app = FastAPI(title="Weather Inference Service")


@app.get("/health")
def health():
    return {
        "status":                   "ok",
        "models_loaded":            [f"xgboost_temp_{h}h" for h in sorted(models)],
        "feature_count":            len(feature_cols),
        "predict_interval_seconds": PREDICT_INTERVAL_SECONDS,
    }


@app.get("/predict")
def predict():
    conn = build_db_connection()
    try:
        preds = run_prediction_cycle(conn)
    finally:
        conn.close()

    if not preds:
        return {"status": "error", "message": "Za mało danych do predykcji"}
    return {"status": "ok", "predictions": preds}


# ── Entrypoint ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_models()
    Thread(target=prediction_loop, daemon=True).start()
    uvicorn.run(app, host="0.0.0.0", port=8080)
