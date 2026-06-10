# Podział zadań — Weather Pipeline SGH

> Projekt: monitoring pogody w czasie rzeczywistym na koordynatach SGH (52.25°N, 21.0°E)
> Przedmiot: Analiza Danych w Czasie Rzeczywistym
> Zespół: 5 osób

---

## Krystian — Data Pipeline & Baza danych ✅

**Zakres:** infrastruktura Docker, przepływ danych Kafka, zapis do PostgreSQL, dane historyczne

### Zrobione
- [x] Docker Compose — Zookeeper, Kafka, PostgreSQL, pgAdmin, producer, consumer, historical-fetch
- [x] Schemat bazy danych (`db/init.sql`) — tabele `weather_raw`, `weather_alerts`, `weather_predictions`, widoki SQL
- [x] **Producer** — pobiera dane z Open-Meteo current API co 15 min, publikuje JSON do topiku `weather-raw`
- [x] **Consumer** — konsumuje `weather-raw`, parsuje format Open-Meteo, zapisuje do PostgreSQL
- [x] **Historical fetch** — jednorazowy skrypt pobierający dane historyczne z Open-Meteo archive (od dowolnej daty, domyślnie od 2026-01-01), zapis do tej samej tabeli `weather_raw`
- [x] Deduplikacja — `WHERE NOT EXISTS` blokuje duplikaty przy wielokrotnym uruchomieniu historical-fetch
- [x] Skrypty backup/restore (`scripts/backup.sh`, `scripts/restore.sh`) — pg_dump z działającego kontenera
- [x] README z pełną instrukcją uruchomienia

### Do zrobienia
- [ ] Rozszerzyć zakres danych historycznych dla lepszego modelu ML — zmienić `HISTORY_START_DATE` w `.env` np. na `2020-01-01` i uruchomić `historical-fetch`
- [ ] Zgranie backupu z danymi historycznymi i przekazanie zespołowi

---

## Maks — Stream Processing & Alerty

**Zakres:** przetwarzanie danych w czasie rzeczywistym, wykrywanie anomalii, generowanie alertów

### Opis
Konsument Kafki (topik `weather-raw`) z logiką analityczną działającą na bieżącym strumieniu danych. Wyniki trafiają do topiku `weather-alerts` oraz tabeli `weather_alerts` w PostgreSQL.

### Do zrobienia
- [ ] Konsument Kafki (`stream_processor.py`) subskrybujący topik `weather-raw`
- [ ] Implementacja okien czasowych — np. średnia krocząca z ostatnich N pomiarów (sliding window)
- [ ] Logika wykrywania anomalii, propozycje:
  - temperatura odbiega od średniej kroczącej o więcej niż 2σ
  - nagły skok lub spadek ciśnienia (>5 hPa w ciągu 1h)
  - prędkość wiatru przekracza próg (np. >15 m/s)
  - widoczność spada poniżej 1000 m (mgła)
- [ ] Publikowanie alertów do topiku Kafka `weather-alerts`
- [ ] Drugi konsument zapisujący alerty do tabeli `weather_alerts` w PostgreSQL
- [ ] Dodanie serwisu do `docker-compose.yml`
- [ ] Dockerfile + requirements.txt

### Schemat tabeli (gotowy w bazie)
```sql
weather_alerts (id, created_at, alert_type, severity, message,
                measured_at, trigger_value, threshold_value, resolved_at)
```

### Propozycje rozszerzenia
- Wysyłanie alertów e-mail lub na Slack webhook
- Mechanizm `resolved_at` — oznaczanie kiedy warunek anomalii ustąpił

---

## Małgosia — Analiza historyczna & Trening modelu ML

**Zakres:** eksploracja danych, budowa i trening modelu predykcyjnego

### Opis
Praca batch na danych zgromadzonych w PostgreSQL. Wynikiem jest wytrenowany model gotowy do użycia przez serwis inferenncji (Osoba 4).

### Do zrobienia
- [ ] Pobranie danych z PostgreSQL do pandas DataFrame
- [ ] EDA (Exploratory Data Analysis):
  - rozkłady temperatur, ciśnienia, wilgotności
  - sezonowość dzienna i tygodniowa
  - korelacje między zmiennymi
  - wizualizacje (matplotlib / seaborn)
- [ ] Feature engineering — cechy przydatne do predykcji:
  - godzina dnia, dzień tygodnia, miesiąc
  - wartości z poprzednich N kroków (lag features)
  - różnice między kolejnymi pomiarami (delta)
- [ ] Trening modelu — propozycje (do wyboru lub porównania):
  - **Prophet** (Facebook) — dobry dla sezonowości, łatwy w użyciu
  - **XGBoost** — szybki, dobre wyniki dla danych tabelarycznych
  - **LSTM** (PyTorch/Keras) — dla sekwencji czasowych
- [ ] Ewaluacja modelu (MAE, RMSE, MAPE) na zbiorze testowym
- [ ] Eksport modelu do pliku (`.pkl` dla sklearn/XGBoost, `.pt` dla PyTorch)
- [ ] Notatnik Jupyter z całą analizą (`analysis.ipynb`)

### Dane wejściowe
Tabela `weather_raw` — dane godzinowe od `HISTORY_START_DATE`, uzupełniane co 15 min danymi live.

### Propozycje rozszerzenia
- Porównanie kilku modeli i wybór najlepszego
- Predykcja wielu zmiennych jednocześnie (temperatura + ciśnienie)
- Analiza błędów predykcji w zależności od pory roku

---

## Alicja — Serwis predykcji (inference)

**Zakres:** mikroserwis ładujący model ML i generujący predykcje na bieżąco

### Opis
Aplikacja Flask/FastAPI działająca w Dockerze. Regularnie pobiera najnowsze dane z PostgreSQL, wywołuje model (plik z Osoby 3) i zapisuje predykcje z powrotem do bazy.

### Do zrobienia
- [ ] Mikroserwis Flask lub FastAPI (`inference_service.py`)
- [ ] Ładowanie modelu z pliku przy starcie serwisu
- [ ] Cykliczne generowanie predykcji (np. co 15 min, co godzinę) — na 1h, 3h, 6h, 24h naprzód
- [ ] Zapis predykcji do tabeli `weather_predictions` w PostgreSQL
- [ ] Endpoint REST `/predict` — opcjonalnie, do odpytywania predykcji na żądanie
- [ ] Endpoint `/health` — sprawdzenie statusu serwisu
- [ ] Dockerfile + requirements.txt
- [ ] Dodanie serwisu do `docker-compose.yml`
- [ ] Po pojawieniu się rzeczywistych danych — uzupełnianie kolumny `actual_temp_c` i `mae_temp` w tabeli predykcji

### Schemat tabeli (gotowy w bazie)
```sql
weather_predictions (id, created_at, predicted_for, horizon_hours,
                     model_name, model_version,
                     pred_temp_c, pred_pressure_hpa, pred_humidity_pct, pred_wind_speed_ms,
                     pred_temp_lower, pred_temp_upper,
                     actual_temp_c, mae_temp)
```

### Gotowy widok do weryfikacji dokładności
```sql
SELECT * FROM v_prediction_accuracy;
```

### Propozycje rozszerzenia
- Automatyczna weryfikacja predykcji po upływie czasu (cron job w serwisie)
- Endpoint `/accuracy` zwracający metryki jakości modelu na żywo

---

## Łukasz — Dashboard & Wizualizacja ✅

**Zakres:** Grafana, panele wizualizacyjne, prezentacja projektu

### Opis
Dashboard w Grafanie podłączony do PostgreSQL. Prezentuje dane live, historię, alerty i porównanie predykcji z rzeczywistością.

### Zrobione
- [x] Podłączenie Grafany do PostgreSQL jako data source (provisioning: `grafana/provisioning/datasources/postgres.yml`)
- [x] Panel: **temperatura na żywo** — ostatnie 24h, gauge z aktualną wartością
- [x] Panel: **temperatura historyczna** — time series z zakresem dat do wyboru (time picker + auto-agregacja)
- [x] Panel: **ciśnienie, wilgotność, wiatr** — time series (3 panele)
- [x] Panel: **alerty** — tabela ostatnich alertów z `weather_alerts` (kolorowanie severity)
- [x] Panel: **predykcje vs rzeczywistość** — wykres nakładający predykcje na rzeczywiste dane (widok `v_prediction_accuracy`) + panel MAE per horyzont + zmienna dashboardu do filtrowania horyzontu
- [x] Panel: **statystyki dzienne** — min/max/średnia temperatury per dzień
- [x] Konfiguracja alertów w Grafanie — reguła alertu (FIRING gdy nowy wpis w `weather_alerts` w ciągu 15 min) + stat panel z czerwonym tłem + adnotacje alertów na wykresach
- [x] Dodanie Grafany do `docker-compose.yml` jako serwis (port 3000, dane w volume `grafana_data`)
- [x] Eksport konfiguracji dashboardu do pliku JSON (`grafana/dashboards/weather.json`, auto-ładowany przez provisioning — działa po `git clone`)

### Propozycje rozszerzenia
- Panel z mapą wiatru (kierunek + prędkość)
- Statystyki miesięczne / sezonowe na podstawie danych historycznych
- Embed dashboardu w prostej stronie HTML jako "wizytówka projektu"

---

## Propozycje dodatkowych elementów dla całego projektu

Jeśli zostanie czas lub projekt wymaga rozbudowy na ocenę:

**Technicznie**
- [ ] **Kafka Connect** — zamiast ręcznego consumera do PostgreSQL, użycie gotowego JDBC Sink Connector
- [ ] **Schema Registry** — walidacja struktury wiadomości Kafka przez Avro schema
- [ ] **Dead Letter Queue** — topik Kafka dla wiadomości których consumer nie mógł przetworzyć
- [ ] **Monitoring stacku** — Prometheus + Grafana do monitorowania Kafki i PostgreSQL (oddzielnie od dashboardu pogodowego)
- [ ] **Testy jednostkowe** — pytest dla funkcji parsujących dane

**Biznesowo / analitycznie**
- [ ] Korelacja pogody z np. publicznymi danymi o transporcie miejskim w Warszawie
- [ ] Wykrywanie ekstremalnych zjawisk pogodowych i ich statystyki historyczne dla Warszawy
- [ ] Porównanie predykcji modelu z oficjalną prognozą pogody (np. z innego API)

---

## Przepływ danych — diagram

```
Open-Meteo API (co 15 min)
        ↓
   [Producer]  ──────────────────→  Kafka: weather-raw
                                           ↓              ↓
                                    [Consumer]      [Stream Processor]
                                           ↓              ↓
                                      PostgreSQL    Kafka: weather-alerts
                                      weather_raw         ↓
                                           ↓         PostgreSQL
                                    [ML Training]    weather_alerts
                                    (Osoba 3, batch)      ↓
                                           ↓         [Grafana]
                                    [Inference Svc]
                                    (Osoba 4)
                                           ↓
                                      PostgreSQL
                                      weather_predictions
                                           ↓
                                      [Grafana Dashboard]
                                      (Osoba 5)
```

```
Open-Meteo Archive API (jednorazowo)
        ↓
[Historical Fetch] ──→ PostgreSQL: weather_raw (dane od 2026-01-01 lub wcześniej)
```
