# Async Pipeline Rewrite — Implementation Plan

## 1. Context

### 1.1 Problem

The current pipeline (`pipeline.py`) fetches air quality data **sequentially** — one API
call at a time, one sensor-pollutant pair at a time. It is hardcoded to 3 boroughs
(Wandsworth, Richmond, Merton) with ~11 active sensors.

We need to expand to **all London boroughs** (~100+ automatic sensors, active and
historical). The first backfill run will require ~60,000+ API calls. At the current
sequential rate (~0.5s per call), that's **~8-9 hours**. This needs to drop to
**~25-30 minutes**.

### 1.2 Solution

Rewrite `pipeline.py` using `asyncio` + `aiohttp` for concurrent API fetching with a
semaphore-based rate limiter. Add automatic sensor discovery from the London Air API.
Keep all existing business logic (missing period detection, daily average calculation,
record building) intact.

### 1.3 Scope of change

| File | Change |
|------|--------|
| `pipeline.py` | Full rewrite — async concurrency, sensor discovery, remove hardcoded filters |
| `server.py` | Increase subprocess timeout 600s → 3600s |
| `requirements.txt` | Add `aiohttp>=3.9.0` |
| `.github/workflows/daily-pipeline.yml` | Add timeout + new env var options |
| `daily-pipeline.yml` | Same (duplicate file at repo root) |

No new files created. No schema changes.

### 1.4 Key Requirements

These are the three driving goals behind the rewrite — not just speed.

**R1: All London sensors, including those not yet in Supabase**
The current pipeline only processes sensors that already exist in the `sensors` table,
filtered to 3 boroughs. Many London sensors have never been added to Supabase. The new
pipeline must **auto-discover all automatic monitoring stations** across every London
borough from the London Air API (`MonitoringSiteSpecies` endpoint) and create them in
Supabase before fetching data. This means the pipeline is self-bootstrapping — it does
not depend on sensors being manually pre-populated.

**R2: Include inactive/decommissioned sensors**
The current pipeline filters to `end_date IS NULL` (active sensors only). Historical
sensors that have been decommissioned still have years of valuable data. The new pipeline
must **remove the active-only filter** and backfill data for all sensors regardless of
their operational status.

**R3: All pollutants including Ozone (O3)**
The current pipeline only processes pollutants already listed in each sensor's
`pollutants_measured` array. O3 was not previously included for some sensors. The new
`discover_and_sync_sensors()` function pulls the **complete species list** from the
London Air API for every sensor — including O3, NO2, PM10, PM2.5, SO2, and any others
the API reports. If a sensor's `pollutants_measured` array in Supabase is missing species
that the API reports, it gets updated. The pipeline then processes **every pollutant** in
the array — no species whitelist or blacklist.

**R4: Concurrent fetching for speed**
Wrap all API calls in `asyncio` + `aiohttp` with a semaphore-based rate limiter.
Target: first full backfill in ~25-30 minutes instead of ~8-9 hours.

---

## 2. Current System Analysis

### 2.1 Current `pipeline.py` flow (sequential)

```
main()
  → load_sensors()          # DB query, filtered to 3 boroughs + active only
  → for each sensor:
      for each pollutant in sensor.pollutants_measured:
        → process_sensor_pollutant()
            → determine_missing_periods()       # 3 DB queries (annual, monthly, daily latest)
            → for year in missing_years:
                → fetch_annual_monthly_data()    # 1 HTTP call per year (requests.get)
            → upload_to_supabase(annual)         # 1 DB insert
            → upload_to_supabase(monthly)        # 1 DB insert
            → query monthly_averages             # 1 DB query (daily cutoff)
            → for chunk in 90-day-chunks:
                → fetch_hourly_and_calculate_daily()  # 1 HTTP call per chunk (requests.get)
                → upload_to_supabase(daily)      # 1 DB insert per chunk
```

### 2.2 Current `pipeline.py` functions

| Function | Lines | Purpose |
|----------|-------|---------|
| `get_supabase_client()` | 21-35 | Create Supabase client from env vars |
| `load_sensors()` | 37-57 | Query sensors table, filter to 3 boroughs + active + Automatic |
| `get_latest_period()` | 59-82 | Query latest row from annual/monthly/daily tables for a sensor-pollutant |
| `determine_missing_periods()` | 84-129 | Calculate which years/months/days are missing for a sensor-pollutant |
| `fetch_annual_monthly_data()` | 131-167 | HTTP GET annual API, parse annual value + 12 monthly values |
| `fetch_hourly_and_calculate_daily()` | 169-202 | HTTP GET hourly API, calculate daily means (≥18 readings) |
| `upload_to_supabase()` | 204-210 | Insert list of records into a table |
| `process_sensor_pollutant()` | 212-281 | Orchestrate: determine gaps → fetch → upload for one sensor-pollutant |
| `main()` | 283-298 | Entry point: load sensors, loop through all pairs |

### 2.3 Current API endpoints used

| Endpoint | Pattern | Returns |
|----------|---------|---------|
| Annual/Monthly | `AirQuality/Annual/MonitoringReport/SiteCode={code}/Year={year}/json` | `@Annual` + `@Month1`-`@Month12` per species per report item |
| Hourly | `AirQuality/Data/Site/SiteCode={code}/StartDate={start}/EndDate={end}/Json` | Hourly readings per species |

### 2.4 Current limitations

- **Hardcoded boroughs**: `['Wandsworth', 'Richmond', 'Merton']` (line 43)
- **Active-only filter**: `.is_('end_date', 'null')` (line 41) — skips decommissioned sensors with historical data
- **No sensor discovery**: sensors must be manually added to DB
- **Sequential HTTP**: one `requests.get()` at a time
- **Per-chunk DB inserts**: daily data uploaded after each 90-day chunk instead of batched
- **No retry**: failed HTTP requests are logged and skipped, no retry

---

## 3. Target Architecture

### 3.1 High-level flow

```
main()
  ├── get_supabase_client()                          [sync]
  ├── discover_and_sync_sensors(supabase)            [sync — 1 API call + DB upserts]
  ├── load_sensors(supabase)                         [sync — 1 DB query, all boroughs]
  └── asyncio.run(async_pipeline(supabase, sensors))
        │
        ├── aiohttp.ClientSession                    [shared connection pool]
        ├── asyncio.Semaphore(MAX_CONCURRENT)        [global rate limiter]
        │
        └── asyncio.gather(*[                        [all sensor-pollutant pairs concurrent]
              process_sensor_pollutant(session, sem, sensor, pollutant, supabase)
                ├── determine_missing_periods()      [DB via to_thread]
                ├── gather(*[                        [all missing years concurrent]
                │     fetch_annual_monthly_data()     [async HTTP, rate-limited]
                │   ])
                ├── upload_to_supabase(annual)        [DB via to_thread, single batch]
                ├── upload_to_supabase(monthly)       [DB via to_thread, single batch]
                ├── query daily cutoff               [DB via to_thread]
                ├── gather(*[                        [all 90-day chunks concurrent]
                │     fetch_hourly_and_calculate_daily()  [async HTTP, rate-limited]
                │   ])
                └── upload_to_supabase(daily)        [DB via to_thread, single batch]
            ])
```

### 3.2 Concurrency model

```
                    ┌─────────────────────────────┐
                    │  asyncio.Semaphore(20)       │
                    │  (global concurrency limit)  │
                    └─────────┬───────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                      ▼
  ┌──────────┐         ┌──────────┐          ┌──────────┐
  │ Worker 1 │         │ Worker 2 │   ...    │ Worker N │
  │ BT1/NO2  │         │ BT1/PM10 │          │ WA9/O3   │
  └────┬─────┘         └────┬─────┘          └────┬─────┘
       │                    │                      │
       ├─ annual yr1 ──►    ├─ annual yr1 ──►     ├─ ...
       ├─ annual yr2 ──►    ├─ annual yr2 ──►     │
       ├─ daily chunk1 ──►  ├─ daily chunk1 ──►   │
       ├─ daily chunk2 ──►  ├─ daily chunk2 ──►   │
       │                    │                      │
       ▼                    ▼                      ▼
  All HTTP requests pass through the semaphore.
  At most 20 are in-flight at any time.
  Each request sleeps API_CALL_DELAY after completion.
```

**Key insight**: The semaphore is **global** across all workers. A worker that needs to
fetch 25 years of annual data launches 25 async tasks, but they all compete for the same
20 semaphore slots. This naturally balances load — no one worker hogs the API.

### 3.3 Error isolation

Each sensor-pollutant pair runs in its own async task with a top-level try/except.
A failure in one worker (e.g., API timeout for `BT1/NO2`) does not affect any other
worker. Failed pairs are logged and automatically retried on the next pipeline run
(since `determine_missing_periods()` will detect the missing data again).

---

## 4. Detailed Function Specifications

### 4.1 Constants and configuration

```python
# Existing
ANNUAL_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringReport/SiteCode={}/Year={}/json"
HOURLY_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Data/Site/SiteCode={}/StartDate={}/EndDate={}/Json"

# New
SITES_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json"

# Configurable via env
MAX_CONCURRENT_REQUESTS = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "20"))
API_CALL_DELAY = float(os.environ.get("API_CALL_DELAY", "0.2"))
HTTP_MAX_RETRIES = int(os.environ.get("HTTP_MAX_RETRIES", "3"))
```

### 4.2 `fetch_json()` — async HTTP helper

**New function.** Central HTTP GET with rate limiting and retry.

```python
async def fetch_json(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    timeout: int = 30
) -> dict:
```

**Behaviour:**
1. Acquire semaphore (blocks if 20 requests already in-flight)
2. `session.get(url)` with `aiohttp.ClientTimeout(total=timeout)`
3. `resp.raise_for_status()` — raises on 4xx/5xx
4. `await resp.json(content_type=None)` — London Air API sometimes returns wrong content-type
5. `await asyncio.sleep(API_CALL_DELAY)` — rate limit delay
6. Release semaphore (via `async with`)
7. On exception: retry up to `HTTP_MAX_RETRIES` times with exponential backoff (1s, 2s, 4s)
8. After all retries exhausted: log error, re-raise exception

**Why `content_type=None`**: The London Air API occasionally serves JSON with
`text/html` content-type. `aiohttp` defaults to rejecting non-JSON content types.

### 4.3 `discover_and_sync_sensors()` — sensor auto-discovery

**New function.** Synchronous. Runs once at pipeline startup before any data fetching.

```python
def discover_and_sync_sensors(supabase: Client) -> None:
```

**Input:** Supabase client
**Output:** None (side effect: inserts/updates sensors table)

**Steps:**

1. **Fetch all London sites from API:**
   ```
   GET https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json
   ```
   Response structure:
   ```json
   {
     "Sites": {
       "Site": [
         {
           "@SiteCode": "BT1",
           "@SiteName": "Brent - Ikea",
           "@Latitude": "51.5...",
           "@Longitude": "-0.2...",
           "@DateOpened": "2005-01-01 00:00:00",
           "@DateClosed": "",
           "@LocalAuthorityName": "Brent",
           "Species": [
             {"@SpeciesCode": "NO2", ...},
             {"@SpeciesCode": "PM10", ...}
           ]
         },
         ...
       ]
     }
   }
   ```

2. **Parse each site:**
   - `site_code` = `@SiteCode`
   - `site_name` = `@SiteName`
   - `lat` = `float(@Latitude)` or `None` if empty/invalid
   - `lon` = `float(@Longitude)` or `None` if empty/invalid
   - `start_date` = `@DateOpened` parsed as date, or `None`
   - `end_date` = `@DateClosed` parsed as date if non-empty, else `None`
   - `borough` = `@LocalAuthorityName`
   - `pollutants_measured` = list of `@SpeciesCode` from `Species`
     - **Edge case**: If site has exactly one species, API returns `Species` as a dict
       instead of a list. Normalize: `if isinstance(species, dict): species = [species]`

3. **Validate site_code:**
   - Must match `^[A-Za-z]+[0-9]*$` (letters optionally followed by digits)
   - Skip sites that don't match (some have codes like "LONDON" which aren't real sensors)

4. **Query existing sensors from DB:**
   ```python
   existing = supabase.table('sensors').select(
       'site_code, id_installation, pollutants_measured, borough'
   ).execute()
   existing_map = {row['site_code']: row for row in existing.data}
   ```

5. **Build borough prefix map** from existing sensors:
   ```python
   # e.g. {"Wandsworth": "WA", "Richmond": "RI", "Merton": "ME"}
   borough_prefix_map = {}
   for row in existing.data:
       if row.get('borough') and row.get('id_installation'):
           prefix = row['id_installation'].split('AA0')[0]
           borough_prefix_map[row['borough']] = prefix
   ```

6. **For each API site, determine action:**

   **a) Site exists in DB** (`site_code` in `existing_map`):
   - Compare `pollutants_measured` arrays
   - If API has species not in DB array → update:
     ```python
     merged = list(set(existing_pollutants + api_pollutants))
     supabase.table('sensors').update({
         'pollutants_measured': merged
     }).eq('site_code', site_code).execute()
     ```
   - Also update `end_date` if it changed (sensor reopened or closed)

   **b) Site is new** (`site_code` not in `existing_map`):
   - Determine borough prefix:
     - If `borough` in `borough_prefix_map` → use that prefix
     - Else → use first 2 uppercase characters of `site_code`
     - Store new prefix in `borough_prefix_map` for subsequent sensors
   - Generate IDs:
     - `id_installation` = `f"{prefix}AA0{site_code}"`
     - `id_site` = `f"X{prefix}AA{site_code}"`
   - Insert:
     ```python
     {
         'id_installation': id_installation,
         'id_site': id_site,
         'site_code': site_code,
         'site_name': site_name,
         'borough': borough,
         'lat': lat,
         'lon': lon,
         'start_date': start_date_str,  # 'YYYY-MM-DD' or None
         'end_date': end_date_str,        # 'YYYY-MM-DD' or None
         'sensor_type': 'Automatic',
         'provider': 'Automatic',
         'pollutants_measured': pollutants_measured
         # organization_id left NULL — DB trigger handles it
     }
     ```

7. **Log summary:** `"Sensor discovery: {n_created} created, {n_updated} updated, {n_skipped} skipped"`

### 4.4 `load_sensors()` — updated

**Modified function.** Remove restrictive filters.

```python
def load_sensors(supabase: Client) -> pd.DataFrame:
```

**Changes from current:**
- **Remove** `.is_('end_date', 'null')` — include inactive sensors for historical backfill
- **Remove** `.in_('borough', ['Wandsworth', 'Richmond', 'Merton'])` — include all boroughs
- **Keep** `.eq('sensor_type', 'Automatic')` — only automatic monitoring stations
- **Keep** all validation (date parsing, site_code regex, drop rows with no start_date)

### 4.5 `get_latest_period()` — unchanged

No changes. This function is synchronous and will be called via `asyncio.to_thread()`
from within async workers.

### 4.6 `determine_missing_periods()` — unchanged

No changes. Called via `asyncio.to_thread()`. The existing logic correctly handles
both historical backfill (no latest → start from sensor's start_date) and incremental
updates (latest exists → start from day after).

### 4.7 `fetch_annual_monthly_data()` — converted to async

**Modified function.** Replaces `requests.get()` with `fetch_json()`.

```python
async def fetch_annual_monthly_data(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    site_code: str,
    year: int,
    pollutant: str
) -> tuple[Optional[float], list]:
```

**Changes from current:**
- `requests.get(url, timeout=30)` → `await fetch_json(session, semaphore, url)`
- Remove try/except around HTTP call (handled by `fetch_json` with retries)
- All parsing logic remains identical:
  - Find `ReportItem` with matching `@SpeciesCode` and `@ReportItem == "7"`
  - Extract `@Annual` value (skip if "-999")
  - Extract `@Month1` through `@Month12` (skip if "-999")
  - Return `(annual_value, monthly_results_list)`

### 4.8 `fetch_hourly_and_calculate_daily()` — converted to async

**Modified function.**

```python
async def fetch_hourly_and_calculate_daily(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    site_code: str,
    start_date,
    end_date,
    pollutant: str
) -> list:
```

**Changes from current:**
- `requests.get(url, timeout=60)` → `await fetch_json(session, semaphore, url, timeout=60)`
- All parsing logic remains identical:
  - Filter `AirQualityData.Data` items by `@SpeciesCode == pollutant`
  - Parse `@MeasurementDateGMT` and `@Value` (skip "-999")
  - Group by date, calculate mean where `count >= 18`
  - Return list of `{'date': date_val, 'value': mean_val}`

### 4.9 `upload_to_supabase()` — add batch chunking

**Modified function.** Supabase has a payload size limit. Large batch inserts
(e.g., 10,000 daily records) should be chunked.

```python
def upload_to_supabase(supabase: Client, table: str, records: list) -> None:
```

**Changes from current:**
- Add chunking: insert in batches of 500 records
  ```python
  BATCH_SIZE = 500
  for i in range(0, len(records), BATCH_SIZE):
      chunk = records[i:i + BATCH_SIZE]
      supabase.table(table).insert(chunk).execute()
  ```
- Remains synchronous — called via `asyncio.to_thread()` from async workers

### 4.10 `process_sensor_pollutant()` — converted to async

**Modified function.** The main per-worker orchestration function.

```python
async def process_sensor_pollutant(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    sensor_row: pd.Series,
    pollutant: str,
    supabase: Client
) -> None:
```

**Full logic:**

```
1. Log: "Processing {site_code} - {pollutant}"

2. Determine missing periods (DB query, run in thread):
   missing = await asyncio.to_thread(determine_missing_periods, sensor_row, pollutant, supabase)

3. If no missing data at all → log and return early

4. ANNUAL + MONTHLY FETCH (concurrent):
   - Create async task for each missing year:
     tasks = [fetch_annual_monthly_data(session, semaphore, site_code, year, pollutant)
              for year in missing['annual_years']]
   - results = await asyncio.gather(*tasks, return_exceptions=True)
   - For each (year, result):
     - If result is Exception → log error, skip
     - Else → unpack (annual_val, monthly_vals)
       - Build annual record if annual_val is not None
       - Build monthly records, filtering to only months in missing['monthly_periods']

5. BATCH UPLOAD annual + monthly:
   await asyncio.to_thread(upload_to_supabase, supabase, 'annual_averages', annual_records)
   await asyncio.to_thread(upload_to_supabase, supabase, 'monthly_averages', monthly_records)

6. DAILY CUTOFF CHECK (DB query, run in thread):
   - Query earliest monthly average for this sensor-pollutant (same as current logic)
   - Only fetch daily data starting from 2 months before first monthly data
   - Filter missing['daily_dates'] to dates >= cutoff

7. DAILY FETCH (concurrent):
   - Build 90-day chunks from filtered daily_dates (same chunking logic as current)
   - Create async task for each chunk:
     tasks = [fetch_hourly_and_calculate_daily(session, semaphore, site_code, chunk_start, chunk_end, pollutant)
              for chunk_start, chunk_end in chunks]
   - results = await asyncio.gather(*tasks, return_exceptions=True)
   - For each result:
     - If Exception → log error, skip
     - Else → build daily records from returned averages

8. BATCH UPLOAD daily:
   await asyncio.to_thread(upload_to_supabase, supabase, 'daily_averages', all_daily_records)

9. Log: "Completed {site_code} - {pollutant}: {n_annual} annual, {n_monthly} monthly, {n_daily} daily"
```

**Key difference from current:** Steps 4 and 7 launch all HTTP requests concurrently
within the worker (via `asyncio.gather`). The global semaphore prevents overloading
the API. Step 8 does a single batch upload instead of per-chunk uploads.

### 4.11 `async_pipeline()` — new

**New function.** Async entry point that creates the session and launches all workers.

```python
async def async_pipeline(supabase: Client, sensors_df: pd.DataFrame) -> None:
```

**Logic:**
```
1. Create semaphore: asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

2. Create aiohttp session: async with aiohttp.ClientSession() as session:

3. Build task list:
   tasks = []
   for _, sensor in sensors_df.iterrows():
       pollutants = sensor['pollutants_measured']
       if not isinstance(pollutants, list):
           continue
       for pollutant in pollutants:
           pollutant_code = pollutant.replace('.', '')
           tasks.append(
               process_sensor_pollutant(session, semaphore, sensor, pollutant_code, supabase)
           )

4. Log: "Processing {len(tasks)} sensor-pollutant combinations with max {MAX_CONCURRENT} concurrent requests"

5. Run all workers: results = await asyncio.gather(*tasks, return_exceptions=True)

6. Summarise:
   succeeded = sum(1 for r in results if not isinstance(r, Exception))
   failed = sum(1 for r in results if isinstance(r, Exception))
   for r in results:
       if isinstance(r, Exception):
           logger.error(f"Worker failed: {r}")
   logger.info(f"Pipeline complete: {succeeded} succeeded, {failed} failed")
```

### 4.12 `main()` — updated

```python
def main():
    logger.info("Starting Air Quality Pipeline")
    supabase = get_supabase_client()

    discover_and_sync_sensors(supabase)

    sensors_df = load_sensors(supabase)
    if sensors_df.empty:
        logger.warning("No sensors to process")
        return

    asyncio.run(async_pipeline(supabase, sensors_df))
    logger.info("Pipeline completed")
```

---

## 5. Supporting File Changes

### 5.1 `requirements.txt`

```
pandas>=2.0.0
requests>=2.31.0
supabase>=2.0.0
python-dotenv>=1.0.0
flask>=3.0.0
aiohttp>=3.9.0
```

`requests` is kept — used by `discover_and_sync_sensors()` (single sync call).

### 5.2 `server.py`

Change subprocess timeout from 600 to 3600 (1 hour — generous buffer for first run):

```python
result = subprocess.run(
    ['python', 'pipeline.py'],
    capture_output=True,
    text=True,
    timeout=3600
)
```

Update timeout message:

```python
except subprocess.TimeoutExpired:
    return jsonify({
        "status": "timeout",
        "message": "Pipeline execution exceeded 60 minute timeout"
    }), 500
```

### 5.3 `.github/workflows/daily-pipeline.yml`

Add `timeout-minutes` to prevent runaway builds, and pass through optional config env vars:

```yaml
- name: Run air quality pipeline
  timeout-minutes: 60
  env:
    SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
    SUPABASE_KEY: ${{ secrets.SUPABASE_KEY }}
    MAX_CONCURRENT_REQUESTS: "20"
    API_CALL_DELAY: "0.2"
  run: python pipeline.py
```

Same change to `daily-pipeline.yml` at repo root.

---

## 6. Database Reference

### 6.1 Tables read/written by pipeline

| Table | Read | Write | Purpose |
|-------|------|-------|---------|
| `sensors` | `load_sensors()`, `discover_and_sync_sensors()` | `discover_and_sync_sensors()` | Sensor metadata |
| `annual_averages` | `get_latest_period()` | `upload_to_supabase()` | Yearly mean pollutant values |
| `monthly_averages` | `get_latest_period()`, daily cutoff query | `upload_to_supabase()` | Monthly mean pollutant values |
| `daily_averages` | `get_latest_period()` | `upload_to_supabase()` | Daily mean from hourly (≥18 readings) |

### 6.2 `sensors` table schema

```sql
create table public.sensors (
  id_installation text not null,        -- PK: {borough_prefix}AA0{site_code}
  id_site text not null,                -- {X}{borough_prefix}AA{site_code}
  site_code text not null,              -- from API: @SiteCode (e.g., "BT1")
  provider text null,                   -- "Automatic"
  site_name text null,                  -- from API: @SiteName
  borough text null,                    -- from API: @LocalAuthorityName
  lat double precision null,            -- from API: @Latitude
  lon double precision null,            -- from API: @Longitude
  sensor_type text null,                -- "Automatic"
  pollutants_measured text[] null,      -- from API: Species[].@SpeciesCode
  height double precision null,
  dist_kerb double precision null,
  dist_receptor double precision null,
  asr boolean null,
  scheme text null,
  triplicate text null,
  start_date date null,                 -- from API: @DateOpened
  end_date date null,                   -- from API: @DateClosed (NULL if active)
  organization_id uuid null,            -- AUTO-SET by trigger
  constraint sensors_pkey primary key (id_installation)
);
```

**ID generation rules:**
- `borough_prefix`: derived from existing sensors in DB for known boroughs. For new
  boroughs, use first 2 uppercase characters of the site_code.
- `id_installation` = `"{prefix}AA0{site_code}"`
- `id_site` = `"X{prefix}AA{site_code}"`
- `organization_id` left as `NULL` — the `assign_organization_by_borough` DB trigger
  populates it automatically.

### 6.3 Data table record formats

**annual_averages:**
```python
{
    'id_site': 'XWAAWA7',
    'pollutant': 'NO2',
    'value': 28.5,
    'year': 2023,
    'date': '2023-01-01',
    'averaging_period': 'annual'
}
```

**monthly_averages:**
```python
{
    'id_site': 'XWAAWA7',
    'pollutant': 'NO2',
    'value': 32.1,
    'year': 2023,
    'month': 3,
    'date': '2023-03-01',
    'averaging_period': 'monthly'
}
```

**daily_averages:**
```python
{
    'id_site': 'XWAAWA7',
    'pollutant': 'NO2',
    'value': 25.7,
    'year': 2024,
    'month': 1,
    'day': 15,
    'date': '2024-01-15',
    'averaging_period': 'daily'
}
```

---

## 7. London Air API Reference

### 7.1 MonitoringSiteSpecies (sensor discovery)

**URL:** `https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json`

**Response structure:**
```json
{
  "Sites": {
    "Site": [
      {
        "@SiteCode": "BT1",
        "@SiteName": "Brent - Ikea",
        "@SiteType": "Suburban",
        "@Latitude": "51.553",
        "@Longitude": "-0.257",
        "@DateOpened": "2005-01-01 00:00:00",
        "@DateClosed": "",
        "@LocalAuthorityName": "Brent",
        "Species": [
          {"@SpeciesCode": "NO2", "@SpeciesName": "Nitrogen Dioxide", ...},
          {"@SpeciesCode": "PM10", "@SpeciesName": "PM10 Particulate", ...}
        ]
      }
    ]
  }
}
```

**Edge cases:**
- `Species` can be a dict (single species) or list (multiple species)
- `@DateClosed` is empty string `""` for active sensors, datetime string for closed
- `@Latitude`/`@Longitude` can be empty strings for some sensors
- Some `@SiteCode` values are non-standard (e.g., "LONDON") — filter with regex

### 7.2 Annual/Monthly (data fetch)

**URL:** `https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringReport/SiteCode={code}/Year={year}/json`

**Key fields in response:**
- `SiteReport.ReportItem[]` — array of report items
- Match on: `@SpeciesCode == pollutant` AND `@ReportItem == "7"` (gravimetric equivalent)
- Annual: `@Annual` field
- Monthly: `@Month1` through `@Month12`
- Missing/invalid data: value is `"-999"` or empty

### 7.3 Hourly (data fetch for daily calculation)

**URL:** `https://api.erg.ic.ac.uk/AirQuality/Data/Site/SiteCode={code}/StartDate={YYYY-MM-DD}/EndDate={YYYY-MM-DD}/Json`

**Key fields in response:**
- `AirQualityData.Data[]` — array of hourly readings
- Each item: `@SpeciesCode`, `@MeasurementDateGMT`, `@Value`
- Missing/invalid data: value is `"-999"` or empty
- **Max range**: ~90 days per request (API returns truncated data for larger ranges)

---

## 8. Configuration

| Env Variable | Default | Description |
|---|---|---|
| `SUPABASE_URL` | **required** | Supabase project URL |
| `SUPABASE_KEY` | **required** | Supabase service role or anon key |
| `MAX_CONCURRENT_REQUESTS` | `20` | Max simultaneous in-flight API calls |
| `API_CALL_DELAY` | `0.2` | Seconds to sleep after each API call (rate limiting) |
| `HTTP_MAX_RETRIES` | `3` | Number of retry attempts per failed HTTP request |

**Tuning guidance:**
- If London Air API starts returning 429s or timeouts → reduce `MAX_CONCURRENT_REQUESTS` to 10 and/or increase `API_CALL_DELAY` to 0.5
- For the first backfill, 20 concurrent / 0.2s delay is a conservative starting point
- Daily incremental runs are light enough that these settings won't matter much

---

## 9. Performance Estimates

### 9.1 Scale calculation

| Metric | Estimate |
|--------|----------|
| London automatic sensors | ~100-150 |
| Pollutants per sensor (avg) | ~3-5 |
| Sensor-pollutant pairs | ~300-500 |
| Years per pair (backfill from ~2000) | ~25 |
| Annual API calls (1 per year per pair) | ~7,500-12,500 |
| Daily chunks (90-day, ~25 years) | ~100 per pair |
| Daily API calls | ~30,000-50,000 |
| **Total first-run API calls** | **~40,000-65,000** |

### 9.2 Throughput comparison

| Mode | Effective calls/sec | First run time | Daily run time |
|------|-------------------|----------------|----------------|
| Sequential (current) | ~2/sec | ~8-9 hours | ~5-10 min |
| Async, 10 concurrent | ~8/sec | ~60-90 min | ~1-2 min |
| Async, 20 concurrent | ~16/sec | ~25-45 min | ~30-60 sec |
| Async, 50 concurrent | ~40/sec | ~15-25 min | ~15-30 sec |

20 concurrent is the recommended starting point — fast enough without risking
API throttling.

### 9.3 Memory usage

- Each async task is lightweight (~few KB for coroutine state)
- Largest in-memory object: hourly data for a 90-day chunk (~2,160 readings × ~100 bytes ≈ 200KB)
- With 20 concurrent requests, peak memory for API responses: ~4MB
- Supabase batch inserts: largest batch is daily records, ~500 records × ~200 bytes ≈ 100KB per chunk
- **Total pipeline memory**: well under 100MB

---

## 10. Deployment

### 10.1 First run strategy

1. Deploy code changes
2. Set GitHub secrets if not already set
3. Trigger workflow manually via `workflow_dispatch`
4. Monitor logs in GitHub Actions — expect ~25-45 minutes
5. Verify in Supabase: `SELECT COUNT(*) FROM daily_averages;` should show substantial data
6. Daily 5am UTC cron continues as before — subsequent runs are incremental (~30-60 sec)

### 10.2 Rollback

If the async rewrite causes issues:
1. Revert the commit (all changes are in 3-4 files)
2. The data already inserted is valid — no need to clean up
3. The sequential pipeline will pick up where it left off via `determine_missing_periods()`

---

## 11. Implementation Checklist

- [ ] Add `aiohttp>=3.9.0` to `requirements.txt`
- [ ] Add new imports and constants to `pipeline.py`
- [ ] Implement `fetch_json()` async HTTP helper
- [ ] Implement `discover_and_sync_sensors()`
- [ ] Update `load_sensors()` — remove borough/end_date filters
- [ ] Convert `fetch_annual_monthly_data()` to async
- [ ] Convert `fetch_hourly_and_calculate_daily()` to async
- [ ] Add batch chunking to `upload_to_supabase()`
- [ ] Convert `process_sensor_pollutant()` to async
- [ ] Implement `async_pipeline()` orchestrator
- [ ] Update `main()` to call discover + async pipeline
- [ ] Update `server.py` timeout (600 → 3600)
- [ ] Update both workflow YAML files with timeout-minutes + env vars
