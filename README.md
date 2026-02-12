# London Air Data Pipeline

Fetches air quality monitoring data from the London Air / ERG network and stores it in the AirStat Supabase database. Handles ~250 automatic sensors across 33 London boroughs, pulling hourly readings for NO2, PM2.5, PM10, and O3 with full historical backfill.

## Quick Start

```bash
# Install Python dependencies
pip install -r requirements.txt

# Copy .env.example to .env and fill in credentials
cp .env.example .env

# Full pipeline: sync sensors then fetch all monitoring data
python pipeline.py

# Sensor sync only (fast, ~10 seconds)
python pipeline.py --sensors-only

# Data fetch only (skip sensor sync)
python pipeline.py --data-only

# Preview what would happen without writing
python pipeline.py --dry-run

# Filter to specific sites
python pipeline.py --data-only --sites RI1,WA7,CD1

# Override start date (useful for limiting backfill range)
python pipeline.py --data-only --start-date 2021-01-01

# FAST backfill via direct Postgres (requires DATABASE_URL in .env)
python pipeline.py --backfill --data-only
```

## Environment Variables

Set these in a `.env` file or as environment variables (see `.env.example`):

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_KEY` | Yes | Supabase service role or anon key |
| `DATABASE_URL` | For --backfill | Direct Postgres connection string (session pooler) |
| `API_CONCURRENCY` | No | Max concurrent API requests (default: 20) |
| `DB_CONCURRENCY` | No | Max concurrent DB writes via REST (default: 5) |
| `TASK_CONCURRENCY` | No | Max sensor x pollutant combos in flight (default: 30) |
| `INSERT_BATCH_SIZE` | No | Rows per REST API insert batch (default: 200) |
| `PG_CONCURRENCY` | No | Max concurrent Postgres connections for backfill (default: 20) |
| `PG_BATCH_SIZE` | No | Rows per Postgres COPY batch for backfill (default: 1000) |
| `MAX_RETRIES` | No | Retry attempts with backoff (default: 4) |

## What It Does

### Phase 1: Sensor Sync

1. Calls the ERG `MonitoringSiteSpecies` API to get all London monitoring sites
2. Compares with existing **Automatic** sensors in Supabase (filters by `sensor_type=Automatic`)
3. Inserts new sensors, updates changed ones (pollutants, dates)
4. Generates IDs: `id_installation = {borough_code}AA0{site_code}`, `id_site = X{borough_code}AA{site_code}`

### Phase 2: Monitoring Data (Async Engine)

1. Builds a work list of sensor x pollutant combinations
2. For each combo, checks the latest existing hourly record (incremental)
3. Fetches hourly data from the ERG API in yearly chunks, concurrently
4. Inserts hourly records and computes daily averages from hourly data
5. Fetches official monthly/annual averages from ERG MonitoringReport API (NO2, PM10, PM2.5)
6. Falls back to computing monthly/annual from hourly data for O3 (no official mean available)

### Backfill Mode (`--backfill`)

Same as Phase 2 but writes directly to Postgres using the COPY protocol instead of the Supabase REST API. ~30x faster (20 concurrent connections, 1000 rows/batch vs 5 concurrent, 200 rows/batch).

Requires `DATABASE_URL` — use the Supabase **session pooler** connection string (port 6543).

## CLI Arguments

| Argument | Description |
|----------|-------------|
| `--sensors-only` | Run Phase 1 only |
| `--data-only` | Run Phase 2 only (skips sensor sync) |
| `--backfill` | Fast bulk load via direct Postgres (requires DATABASE_URL) |
| `--dry-run` | Preview mode, no database writes |
| `--sites RI1,WA7` | Comma-separated site codes to process |
| `--start-date 2021-01-01` | Override the earliest date to fetch |

## Data Sources

| Data | API | Used For |
|------|-----|----------|
| Hourly readings | `/Data/SiteSpecies/...` | hourly_averages + daily_averages (computed) |
| Monthly/Annual means | `/Annual/MonitoringReport/...` | monthly_averages + annual_averages (official ERG values) |
| O3 monthly/annual | Computed from hourly | O3 has no Mean row in MonitoringReport |

## GitHub Actions

The pipeline runs daily at 5:00 AM UTC via `.github/workflows/daily-pipeline.yml`. Uses the normal REST API mode (not backfill). Can be triggered manually from the Actions tab. 60-minute timeout.

Required secrets: `SUPABASE_URL`, `SUPABASE_KEY`.

## Flask Server

`server.py` provides a Flask endpoint at `/run-pipeline` that triggers the pipeline as a subprocess (1 hour timeout).

## Dependencies

- Python 3.12+
- `aiohttp` - async HTTP client (API fetching)
- `requests` - sync HTTP client (Phase 1)
- `python-dotenv` - .env file loading
- `supabase` - Supabase Python client (Phase 1 + normal mode writes)
- `psycopg[binary]` - Direct Postgres client (backfill mode)
- `flask` - Web server for pipeline triggering

## Files

| File | Description |
|------|-------------|
| `pipeline.py` | Main pipeline script (async) |
| `server.py` | Flask server with `/run-pipeline` endpoint |
| `requirements.txt` | Python dependencies |
| `.env.example` | Environment variable template |
| `.gitignore` | Git ignore rules |
| `CLAUDE.md` | Instructions for AI coding agents |
| `SCHEMA.md` | Architecture, API sources, DB schema, gotchas |
| `README.md` | This file |
