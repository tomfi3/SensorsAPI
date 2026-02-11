# London Air Data Pipeline

Fetches air quality monitoring data from the London Air / ERG network and stores it in the AirStat Supabase database. Handles ~250 sensors across 33 London boroughs, pulling hourly readings for NO2, PM2.5, PM10, and O3 with full historical backfill.

## Quick Start

```bash
# Install Python dependencies
pip install -r requirements.txt

# Ensure environment variables are set (see below)

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
```

## Environment Variables

Set these in a `.env` file or as environment variables:

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_KEY` | Yes | Supabase service role or anon key |
| `API_CONCURRENCY` | No | Max concurrent API requests (default: 20) |
| `DB_CONCURRENCY` | No | Max concurrent DB writes (default: 5) |
| `TASK_CONCURRENCY` | No | Max sensor x pollutant combos in flight (default: 30) |
| `INSERT_BATCH_SIZE` | No | Rows per INSERT batch (default: 200) |
| `MAX_RETRIES` | No | Retry attempts with backoff (default: 4) |

## What It Does

### Phase 1: Sensor Sync

1. Calls the ERG `MonitoringSiteSpecies` API to get all London monitoring sites
2. Compares with existing `sensors` table in Supabase
3. Inserts new sensors, updates changed ones (pollutants, dates)
4. Generates IDs matching the existing format: `id_installation = {borough_code}AA0{site_code}`, `id_site = X{borough_code}AA{site_code}`

### Phase 2: Monitoring Data (Async Engine)

1. Builds a work list of sensor x pollutant combinations
2. For each combo, checks the latest existing hourly record (incremental)
3. Fetches hourly data from the ERG API in yearly chunks, concurrently
4. Inserts hourly records into Supabase in 200-row batches
5. Computes and inserts daily, monthly, and annual averages

## CLI Arguments

| Argument | Description |
|----------|-------------|
| `--sensors-only` | Run Phase 1 only |
| `--data-only` | Run Phase 2 only (skips sensor sync) |
| `--dry-run` | Preview mode, no database writes |
| `--sites RI1,WA7` | Comma-separated site codes to process |
| `--start-date 2021-01-01` | Override the earliest date to fetch |

## Concurrency Tuning

| Constant | Default | Purpose |
|----------|---------|---------|
| `API_CONCURRENCY` | 20 | Max concurrent ERG API requests |
| `DB_CONCURRENCY` | 5 | Max concurrent Supabase writes |
| `TASK_CONCURRENCY` | 30 | Max sensor x pollutant combos in flight |
| `INSERT_BATCH_SIZE` | 200 | Rows per INSERT call |
| `MAX_RETRIES` | 4 | Retry attempts with exponential backoff |
| `MAX_DAYS_PER_CHUNK` | 365 | Days per API request chunk |

**Do not increase `DB_CONCURRENCY` above 5** without testing. Supabase's Cloudflare proxy starts returning 400/502 errors at higher concurrency.

## GitHub Actions

The pipeline runs daily at 5:00 AM UTC via `.github/workflows/daily-pipeline.yml`. It can also be triggered manually from the Actions tab. The workflow has a 60-minute timeout.

Required secrets: `SUPABASE_URL`, `SUPABASE_KEY`.

## Flask Server

`server.py` provides a Flask endpoint at `/run-pipeline` that triggers the pipeline as a subprocess. The timeout is set to 3600 seconds (1 hour).

## Dependencies

- Python 3.12+
- `aiohttp` - async HTTP client (Phase 2)
- `requests` - sync HTTP client (Phase 1)
- `python-dotenv` - .env file loading
- `supabase` - Supabase Python client (Phase 1)
- `flask` - Web server for pipeline triggering

## Files

| File | Description |
|------|-------------|
| `pipeline.py` | Main pipeline script (async) |
| `server.py` | Flask server with `/run-pipeline` endpoint |
| `requirements.txt` | Python dependencies |
| `PLAN.md` | Async rewrite design document |
| `SCHEMA.md` | Architecture, API sources, DB schema, gotchas |
| `CLAUDE.md` | Instructions for AI coding agents |
| `README.md` | This file |
