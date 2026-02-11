# CLAUDE.md - Pipeline Agent Instructions

Instructions for AI coding agents working on the London Air data pipeline. Read this before making any changes.

---

## What This Code Does

`pipeline.py` is a two-phase data pipeline:

- **Phase 1** (sync, fast): Calls the ERG MonitoringSiteSpecies API, compares ~254 London air quality monitoring sites with the Supabase `sensors` table, and inserts/updates as needed.
- **Phase 2** (async, long-running): For each sensor x pollutant combination, fetches historical hourly data from the ERG API, inserts it into Supabase, and computes daily/monthly/annual aggregates.

The pipeline is **incremental** - it checks what data already exists and only fetches/inserts new records.

---

## Critical Knowledge (Things That Will Break If You Don't Know)

### 1. API Domain

The ERG API is at `api.erg.ic.ac.uk/AirQuality`. Do NOT use `api.londonair.org.uk` - that domain does not resolve and all requests will fail with DNS errors.

### 2. Data Tables Have NO Unique Constraints

`hourly_averages`, `daily_averages`, `monthly_averages`, and `annual_averages` all use auto-increment `id` as their only primary key. There is no composite unique constraint on `(id_site, pollutant, year, month, day, hour)` or similar.

**This means:**
- You CANNOT use UPSERT / ON CONFLICT - it will throw `there is no unique or exclusion constraint matching the ON CONFLICT specification`
- You MUST use plain INSERT
- You MUST implement incremental logic (check latest existing record, only insert newer data) to avoid duplicates
- If you need idempotent re-runs, you'd need to add unique constraints to the DB first

### 3. hourly_averages Is Massive

Queries against `hourly_averages` without a `year` filter WILL timeout (Supabase statement timeout). The `sb_get_latest_hourly()` function scans year-by-year from the current year backwards specifically to avoid this. Do not refactor this into a single ORDER BY query - it will timeout.

### 4. Supabase Cloudflare Rate Limits

Supabase is fronted by Cloudflare. More than 5 concurrent write operations cause:
- HTTP 400 Bad Request (Cloudflare HTML error page)
- HTTP 502 Bad Gateway
- SSL EOF errors (`_ssl.c:2427`)

The `DB_CONCURRENCY` semaphore (default 5) prevents this. Do not increase it without testing. The `INSERT_BATCH_SIZE` is 200 rows - batches of 500+ were rejected by Cloudflare.

### 5. Sensor ID Generation

IDs follow this pattern:
- `id_installation = {borough_code}AA0{site_code}`
- `id_site = X{borough_code}AA{site_code}`

The `borough_code` comes from the `BOROUGH_CODE` dict (maps borough name to 2-letter code). It does NOT come from the first 2 characters of the site code. Example: site `RHI` in Richmond gets code `RI` (from "Richmond"), producing `RIAA0RHI`, not `RHAA0RHI`.

### 6. Species Can Be Dict or Array

The ERG API returns JSON converted from XML. When a site has only 1 species, `Species` is a single dict instead of an array. The `normalise_species()` function handles this. Always use it when iterating species.

### 7. Windows Console Encoding

The script wraps stdout/stderr with `codecs.getwriter("utf-8")` for Windows compatibility. Without this, any Unicode characters in log output cause `UnicodeEncodeError` on Windows terminals using cp1252.

---

## Architecture

```
Phase 1: sync (requests library)
  fetch_site_species() -> api_site_to_row() -> sync_sensors()

Phase 2: async (aiohttp library)
  build_work_items() -> run_async_pipeline() -> process_item_async()
    +-- sb_get_latest_hourly()     # Check incremental state
    +-- fetch_chunk_async()        # Fetch yearly chunks (API semaphore)
    +-- sb_insert_rows()           # Insert hourly (DB semaphore + retry)
    |   +-- sb_insert_batch()      # Single batch with exponential backoff
    +-- sb_get_latest_agg()        # Check aggregate state
    +-- sb_insert_rows()           # Insert daily/monthly/annual
```

Three async semaphores control concurrency:
- `TASK_CONCURRENCY` (30): sensor x pollutant combos in flight
- `API_CONCURRENCY` (20): ERG API requests in flight
- `DB_CONCURRENCY` (5): Supabase REST API writes in flight

---

## Key Functions

| Function | Purpose |
|----------|---------|
| `fetch_site_species()` | GET metadata for all London sites |
| `api_site_to_row()` | Convert API site dict to sensors table row |
| `generate_ids()` | Create id_installation and id_site from site_code + borough |
| `normalise_species()` | Handle single-dict vs array Species quirk |
| `sync_sensors()` | Phase 1: diff API sites vs DB, upsert/update |
| `build_work_items()` | Generate sensor x pollutant task list |
| `generate_yearly_chunks()` | Split date range into 365-day chunks |
| `process_item_async()` | Full async pipeline for one sensor x pollutant |
| `sb_get_latest_hourly()` | Year-by-year scan for latest hourly record |
| `fetch_chunk_async()` | Fetch one yearly chunk from ERG API with retry |
| `sb_insert_batch()` | Insert one 200-row batch with exponential backoff |
| `run_async_pipeline()` | Orchestrate all items with semaphores |

---

## Common Modifications

### Adding a new pollutant

1. Add to `SPECIES_MAP` dict: `"API_CODE": "DB_NAME"`
2. Run `--sensors-only` to update `pollutants_measured` arrays
3. Run `--data-only` to backfill the new pollutant's data

### Changing concurrency limits

Edit the constants at the top of the file or use environment variables. Reducing `DB_CONCURRENCY` below 5 is safe. Increasing above 5 requires testing against Cloudflare limits.

### Fixing a failed run

The dead letter queue at the end of a run shows which sensor x pollutant x table combinations had failed inserts. To retry:
```bash
python pipeline.py --data-only --sites SITE1,SITE2
```
The incremental logic will pick up from where data exists.

---

## Environment

- **Python**: 3.12+
- **Key dependencies**: `aiohttp`, `requests`, `python-dotenv`, `supabase`
- **Config**: Reads from `.env` or environment variables
- **Target DB**: Supabase project at `SUPABASE_URL`
- **Source API**: `api.erg.ic.ac.uk/AirQuality`

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_KEY` | Yes | Supabase service role or anon key |
| `API_CONCURRENCY` | No | Max concurrent API requests (default: 20) |
| `DB_CONCURRENCY` | No | Max concurrent DB writes (default: 5) |
| `TASK_CONCURRENCY` | No | Max sensor x pollutant combos in flight (default: 30) |
| `INSERT_BATCH_SIZE` | No | Rows per INSERT batch (default: 200) |
| `MAX_RETRIES` | No | Retry attempts with backoff (default: 4) |

---

## Testing

```bash
# Dry run (no writes)
python pipeline.py --dry-run

# Single sensor test (short backfill)
python pipeline.py --data-only --sites CD1 --start-date 2025-01-01

# Verify incremental (should report "up to date" instantly)
python pipeline.py --data-only --sites CD1 --start-date 2025-01-01
```

---

## Do Not

- Use `api.londonair.org.uk` (DNS fails, use `api.erg.ic.ac.uk`)
- Use UPSERT on data tables (no unique constraints)
- Query `hourly_averages` without a `year` filter (timeout)
- Set `DB_CONCURRENCY` above 5 (Cloudflare errors)
- Set `INSERT_BATCH_SIZE` above 200 (Cloudflare payload rejection)
- Remove the Windows `codecs` wrapper (breaks on Windows)
- Assume Species is always an array (can be single dict)
- Derive borough code from site code (use `BOROUGH_CODE` mapping)
