# Polymarket BTC 5-Minute Prediction Engine

Production-ready Python system with:

- Parallel rule + ML signal generation
- Full observation logging (anti-selection-bias dataset)
- Outcome tracking and continuous learning
- Meta model for rule-vs-ML routing
- Polymarket-only BTC reference feed (Gamma/CLOB + Chainlink RTDS)
- Strict DB retention/compression controls
- FastAPI operational endpoints

## Folder Structure

```text
polyModel/
├── app/
│   ├── config.py
│   ├── logging_config.py
│   ├── main.py
│   ├── core/
│   │   ├── cleanup.py
│   │   ├── data_clients.py
│   │   ├── feature_engine.py
│   │   ├── learning_engine.py
│   │   ├── meta_engine.py
│   │   ├── ml_engine.py
│   │   ├── outcome_tracker.py
│   │   ├── pipeline.py
│   │   └── rule_engine.py
│   └── db/
│       ├── models.py
│       ├── repository.py
│       └── session.py
├── models/
├── scripts/
│   └── test_data_source.py
├── .env.example
├── Procfile
└── requirements.txt
```

## Modes

- `observation`: log all observations only
- `shadow`: simulate decisions, no retraining
- `learning`: full retraining + rule updates + meta updates

`MARKET_SLUG` can be a fixed slug or a template:

- Fixed: `btc-updown-5m-1744214400`
- Dynamic template: `btc-updown-5m-<WINDOW_START_TS>`

## Quickstart

1. Create `.env` from `.env.example`.
2. Set `DATABASE_URL` to your Neon Postgres async URL.
   If this is an upgrade, run `scripts/migrate_schema.sql` first.
3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Run API:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

5. Validate market/BTC source:

```bash
python scripts/test_data_source.py
```

6. Validate fetch fallbacks and schema locally:

```bash
python scripts/test_fetch_points.py
python scripts/validate_schema.py
```

## API

- `GET /health`
- `GET /stats`
- `GET /comparison`
- `GET /recent`
- `GET /buckets`

## Retention / Compression Policy

- Full raw observations: last 24 hours
- Compression window: 24 hours to 7 days
- Aggregates older than 7 days are removed
- Background cleanup: every 30 minutes
- Emergency prune guard if DB exceeds configured max size (`MAX_DB_SIZE_MB`, default `500`)
