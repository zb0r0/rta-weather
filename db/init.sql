-- ============================================================
-- Schemat bazy danych: monitoring pogody w czasie rzeczywistym
-- ============================================================

-- Surowe dane z OpenWeatherMap API
CREATE TABLE IF NOT EXISTS weather_raw (
    id                  SERIAL PRIMARY KEY,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),   -- kiedy consumer zapisał rekord
    measured_at         TIMESTAMPTZ NOT NULL,                 -- dt z API (czas pomiaru)

    -- temperatura w Kelwinach (oryginał API), przeliczona na °C
    temp_k              NUMERIC(7, 3),
    temp_c              NUMERIC(7, 3) GENERATED ALWAYS AS (temp_k - 273.15) STORED,
    feels_like_k        NUMERIC(7, 3),
    feels_like_c        NUMERIC(7, 3) GENERATED ALWAYS AS (feels_like_k - 273.15) STORED,
    temp_min_k          NUMERIC(7, 3),
    temp_max_k          NUMERIC(7, 3),

    -- atmosfera
    pressure_hpa        NUMERIC(7, 2),   -- ciśnienie w hPa
    humidity_pct        SMALLINT,        -- wilgotność w %
    visibility_m        INTEGER,         -- widoczność w metrach

    -- wiatr
    wind_speed_ms       NUMERIC(6, 3),   -- m/s
    wind_deg            SMALLINT,        -- kierunek w stopniach
    wind_gust_ms        NUMERIC(6, 3),   -- porywy wiatru m/s (opcjonalne)

    -- zachmurzenie i opady
    clouds_pct          SMALLINT,        -- zachmurzenie w %
    rain_1h_mm          NUMERIC(6, 2),   -- opady deszczu ostatnia 1h (mm)
    snow_1h_mm          NUMERIC(6, 2),   -- opady śniegu ostatnia 1h (mm)

    -- opis pogody
    weather_id          SMALLINT,        -- kod warunków pogodowych OWM
    weather_main        VARCHAR(64),     -- np. "Rain", "Clear", "Clouds"
    weather_description VARCHAR(128),    -- np. "light rain"
    weather_icon        VARCHAR(8),

    -- słońce
    sunrise_at          TIMESTAMPTZ,
    sunset_at           TIMESTAMPTZ,

    -- lokalizacja (na potrzeby przyszłej rozbudowy)
    lat                 NUMERIC(9, 6),
    lon                 NUMERIC(9, 6),

    -- surowy JSON z API (do debugowania i re-parsowania)
    raw_payload         JSONB
);

CREATE INDEX IF NOT EXISTS idx_weather_raw_measured_at ON weather_raw (measured_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_raw_ingested_at ON weather_raw (ingested_at DESC);


-- Alerty generowane przez stream processor (Osoba 2)
CREATE TABLE IF NOT EXISTS weather_alerts (
    id              SERIAL PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_type      VARCHAR(64) NOT NULL,    -- np. "HIGH_WIND", "TEMP_ANOMALY", "PRESSURE_DROP"
    severity        VARCHAR(16) NOT NULL,    -- "INFO" | "WARNING" | "CRITICAL"
    message         TEXT,
    measured_at     TIMESTAMPTZ,             -- czas pomiaru który wyzwolił alert
    trigger_value   NUMERIC(10, 3),          -- wartość która wywołała alert
    threshold_value NUMERIC(10, 3),          -- próg który został przekroczony
    resolved_at     TIMESTAMPTZ              -- kiedy warunek przestał obowiązywać
);

CREATE INDEX IF NOT EXISTS idx_weather_alerts_created_at ON weather_alerts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_type ON weather_alerts (alert_type);


-- Predykcje generowane przez serwis ML (Osoba 4)
CREATE TABLE IF NOT EXISTS weather_predictions (
    id                  SERIAL PRIMARY KEY,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- kiedy predykcja została wygenerowana
    predicted_for       TIMESTAMPTZ NOT NULL,                -- na kiedy jest predykcja
    horizon_hours       SMALLINT NOT NULL,                   -- horyzont predykcji (1, 3, 6, 12, 24h)
    model_name          VARCHAR(64),                         -- np. "prophet_v1", "xgboost_v2"
    model_version       VARCHAR(32),

    -- predykowane wartości
    pred_temp_c         NUMERIC(7, 3),
    pred_pressure_hpa   NUMERIC(7, 2),
    pred_humidity_pct   NUMERIC(5, 2),
    pred_wind_speed_ms  NUMERIC(6, 3),

    -- przedziały ufności (opcjonalne)
    pred_temp_lower     NUMERIC(7, 3),
    pred_temp_upper     NUMERIC(7, 3),

    -- wynik weryfikacji (uzupełniany post-factum przez Osobę 4)
    actual_temp_c       NUMERIC(7, 3),
    mae_temp            NUMERIC(7, 3)   -- Mean Absolute Error dla temperatury
);

CREATE INDEX IF NOT EXISTS idx_predictions_predicted_for ON weather_predictions (predicted_for DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_created_at   ON weather_predictions (created_at DESC);


-- Widok ułatwiający analizę: ostatnie 24h z przeliczonymi wartościami
CREATE OR REPLACE VIEW v_weather_last_24h AS
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
    snow_1h_mm,
    weather_main,
    weather_description,
    visibility_m
FROM weather_raw
WHERE measured_at >= NOW() - INTERVAL '24 hours'
ORDER BY measured_at DESC;


-- Widok: porównanie predykcji z rzeczywistością
CREATE OR REPLACE VIEW v_prediction_accuracy AS
SELECT
    p.predicted_for,
    p.horizon_hours,
    p.model_name,
    p.pred_temp_c,
    r.temp_c AS actual_temp_c,
    ABS(p.pred_temp_c - r.temp_c) AS abs_error_temp
FROM weather_predictions p
LEFT JOIN LATERAL (
    SELECT temp_c
    FROM weather_raw
    WHERE measured_at BETWEEN p.predicted_for - INTERVAL '5 minutes'
                          AND p.predicted_for + INTERVAL '5 minutes'
    ORDER BY ABS(EXTRACT(EPOCH FROM (measured_at - p.predicted_for)))
    LIMIT 1
) r ON true
WHERE p.pred_temp_c IS NOT NULL
ORDER BY p.predicted_for DESC;
