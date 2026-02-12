# CLAUDE.md - Pipeline Agent Instructions

Instructions for AI coding agents working on the London Air data pipeline. Read this before making any changes.

---

## What This Code Does

`pipeline.py` is a two-phase data pipeline:

- **Phase 1** (sync, fast): Calls the ERG MonitoringSiteSpecies API, compares ~254 London air quality monitoring sites with the Supabase `sensors` table (Automatic type only), and inserts/updates as needed.
- **Phase 2** (async, long-running): For each sensor x pollutant combination, fetches historical hourly data from the ERG API, inserts it, computes daily averages from hourly, and fetches official monthly/annual means from the MonitoringReport API (with hourly fallback for O3).
- **Backfill mode** (`--backfill`): Same as Phase 2 but writes directly to Postgres via COPY protocol (~30x faster than REST API).

The pipeline is **incremental** - it checks what data already exists and only fetches/inserts new records.

---

## Critical Knowledge (Things That Will Break If You Don't Know)

### 1. API Domain

The ERG API is at `api.erg.ic.ac.uk/AirQuality`. Do NOT use `api.londonair.org.uk` - that domain does not resolve and all requests will fail with DNS errors.

### 2. Data Tables Have NO Unique Constraints

`hourly_averages`, `daily_averages`, `monthly_averages`, and `annual_averages` all use auto-increment `id` as their only primary key. There is no composite unique constraint.

**This means:**
- You CANNOT use UPSERT / ON CONFLICT
- You MUST use plain INSERT
- You MUST implement incremental logic to avoid duplicates
- If you need idempotent re-runs, you'd need to add unique constraints to the DB first

### 3. hourly_averages Is Massive

Queries against `hourly_averages` without a `year` filter WILL timeout. The `sb_get_latest_hourly()` / `pg_get_latest_hourly()` functions scan year-by-year from the current year backwards specifically to avoid this. Do not refactor into a single ORDER BY query.

### 4. Supabase Cloudflare Rate Limits (REST API mode)

More than 5 concurrent write operations cause HTTP 400/502/SSL errors. `DB_CONCURRENCY` semaphore (default 5) prevents this. `INSERT_BATCH_SIZE` is 200 rows - batches of 500+ were rejected.

This does NOT apply to `--backfill` mode which uses direct Postgres.

### 5. Sensor ID Generation

IDs follow this pattern:
- `id_installation = {borough_code}AA0{site_code}`
- `id_site = X{borough_code}AA{site_code}`

The `borough_code` comes from the `BOROUGH_CODE` dict (maps borough name to 2-letter code). It does NOT come from the first 2 characters of the site code.

### 6. Species Can Be Dict or Array

The ERG API returns JSON converted from XML. When a site has only 1 species, `Species` is a single dict instead of an array. Always use `normalise_species()`.

### 7. Sensors Table Is Shared

The `sensors` table contains 3,713 rows across multiple types:
- **Automatic** (244): ERG/London Air network — managed by this pipeline
- **Diffusion Tube** (2,670): Managed by other processes
- **Clarity** (799): Breathe London — managed by other processes

Phase 1 (`sync_sensors`) filters by `sensor_type=Automatic` to avoid touching other sensor types. Do NOT remove this filter.

### 8. MonitoringReport API

- URL: `/Annual/MonitoringReport/SiteCode={code}/Year={year}/json`
- Filter: `@ReportItem == "7"` AND `@ReportItemName` starts with `"Mean:"`
- Provides official monthly/annual means for **NO2, PM10, PM25** only
- **O3 has NO Mean row** — its AQS objective is exceedance-day based, not mean-based
- Returns `{"SiteReport": null}` for unknown site/year combos — must guard against null
- Returns ALL species per call, so cache by `(site_code, year)` to avoid redundant requests

### 9. Postgres Connection (Backfill Mode)

- Use the Supabase **session pooler** at port `6543` (NOT 5432 for session, NOT direct host)
- Direct host (`db.*.supabase.co`) is IPv6 only — fails on most home/office networks
- `asyncpg` has a SCRAM auth bug with Supabase pooler — use `psycopg` instead
- `DATABASE_URL` env var required for `--backfill` mode

### 10. Windows Console Encoding

The script wraps stdout/stderr with `codecs.getwriter("utf-8")` for Windows compatibility. Without this, Unicode in log output causes `UnicodeEncodeError`.

---

## Architecture

```
Phase 1: sync (requests library)
  fetch_site_species() -> api_site_to_row() -> sync_sensors()

Phase 2 - Normal mode (aiohttp + Supabase REST):
  build_work_items() -> run_async_pipeline() -> process_item_async()
    +-- sb_get_latest_hourly()              # Incremental check
    +-- fetch_chunk_async()                 # Fetch yearly chunks (API sem)
    +-- sb_insert_rows()                    # Insert hourly (DB sem + retry)
    +-- fetch_monitoring_report_async()     # Official monthly/annual
    +-- sb_insert_rows()                    # Insert daily/monthly/annual

Phase 2 - Backfill mode (aiohttp + direct Postgres COPY):
  build_work_items() -> run_backfill_pipeline() -> process_item_backfill()
    +-- pg_get_latest_hourly()              # Incremental check (SQL)
    +-- fetch_chunk_async()                 # Same API fetching
    +-- pg_insert_rows()                    # COPY protocol bulk insert
    +-- fetch_monitoring_report_async()     # Same MonitoringReport
    +-- pg_insert_rows()                    # COPY for daily/monthly/annual
```

Semaphores (normal mode):
- `TASK_CONCURRENCY` (30): sensor x pollutant combos in flight
- `API_CONCURRENCY` (20): ERG API requests
- `DB_CONCURRENCY` (5): Supabase REST writes

Backfill mode:
- `API_CONCURRENCY` (20): ERG API requests
- `PG_CONCURRENCY` (20): Postgres connections (each task gets its own)

---

## Key Functions

| Function | Purpose |
|----------|---------|
| `fetch_site_species()` | GET metadata for all London sites |
| `api_site_to_row()` | Convert API site dict to sensors table row |
| `generate_ids()` | Create id_installation and id_site |
| `normalise_species()` | Handle single-dict vs array Species quirk |
| `sync_sensors()` | Phase 1: diff API vs DB (Automatic only) |
| `build_work_items()` | Generate sensor x pollutant task list with start_year |
| `fetch_chunk_async()` | Fetch one yearly chunk of hourly data |
| `fetch_monitoring_report_async()` | Fetch official monthly/annual with cache+lock |
| `process_item_async()` | Full pipeline for one item (REST mode) |
| `process_item_backfill()` | Full pipeline for one item (Postgres mode) |
| `pg_insert_rows()` | Bulk insert via COPY protocol |
| `run_async_pipeline()` | Orchestrate items (REST mode) |
| `run_backfill_pipeline()` | Orchestrate items (Postgres mode) |

---

## Testing

```bash
# Dry run (no writes)
python pipeline.py --dry-run

# Single sensor test (REST mode)
python pipeline.py --data-only --sites CD1 --start-date 2025-01-01

# Single sensor test (backfill mode)
python pipeline.py --backfill --data-only --sites CD1 --start-date 2025-01-01

# Verify incremental (should report "up to date" instantly)
python pipeline.py --backfill --data-only --sites CD1
```

---

## Do Not

- Use `api.londonair.org.uk` (DNS fails, use `api.erg.ic.ac.uk`)
- Use UPSERT on data tables (no unique constraints)
- Query `hourly_averages` without a `year` filter (timeout)
- Set `DB_CONCURRENCY` above 5 in REST mode (Cloudflare errors)
- Set `INSERT_BATCH_SIZE` above 200 in REST mode (Cloudflare rejection)
- Remove the `sensor_type=Automatic` filter in sync_sensors (will corrupt other sensor types)
- Remove the Windows `codecs` wrapper (breaks on Windows)
- Assume Species is always an array (can be single dict)
- Derive borough code from site code (use `BOROUGH_CODE` mapping)
- Use `asyncpg` for Supabase connections (SCRAM auth bug — use `psycopg`)
- Use Supabase direct host for Postgres (IPv6 only — use session pooler port 6543)
