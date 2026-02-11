# Schema & Context

Technical reference for the London Air data pipeline: architecture, data sources, destination tables, edge cases, and operational logic.

---

## Architecture Overview

```
                          Phase 1 (sync)                    Phase 2 (async)
                      +------------------+            +--------------------------+
                      |  MonitoringSite   |            |    ERG Hourly Data API   |
                      |  Species API      |            |  /Data/SiteSpecies/...   |
                      |  (254 sites)      |            |  (~250 sensor x pollutant|
                      +--------+---------+            |   x yearly chunks)       |
                               |                      +------------+-------------+
                          requests.get()                           |
                               |                        aiohttp.ClientSession
                               v                        (TCP connection pooling)
                      +------------------+                         |
                      |  Compare with    |              +----------+----------+
                      |  existing DB     |              |   asyncio.Semaphore |
                      |  sensors table   |              |   API=20  DB=5      |
                      +--------+---------+              +----------+----------+
                               |                                   |
                    UPSERT / UPDATE                     INSERT (200-row batches)
                               |                        + exponential backoff
                               v                                   v
                      +----------------------------------------------------+
                      |              Supabase (PostgreSQL)                  |
                      |  sensors | hourly | daily | monthly | annual       |
                      +----------------------------------------------------+
```

---

## API Sources

### 1. MonitoringSiteSpecies (Metadata)

- **URL**: `https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json`
- **Method**: GET
- **Response format**: `{ "Sites": { "Site": [...] } }`
- **Called once** at pipeline start for both phases
- **Fields used**: `@SiteCode`, `@SiteName`, `@LocalAuthorityName`, `@Latitude`, `@Longitude`, `@DateOpened`, `@DateClosed`, `Species` (array or single dict)

**CRITICAL**: The domain `api.londonair.org.uk` does NOT resolve. Always use `api.erg.ic.ac.uk`.

### 2. SiteSpecies Hourly Data

- **URL pattern**: `https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies/SiteCode={site}/SpeciesCode={species}/StartDate={start}/EndDate={end}/Json`
- **Method**: GET
- **Response format**: `{ "RawAQData": { "Data": [{ "@MeasurementDateGMT": "YYYY-MM-DD HH:MM:SS", "@Value": "float" }] } }`
- **Chunked** into 365-day requests to avoid API timeouts
- **All chunks for a sensor x pollutant are fetched concurrently** (bounded by API semaphore)

### Species Codes

| API Code | DB Pollutant | Description |
|----------|-------------|-------------|
| `NO2` | `NO2` | Nitrogen Dioxide |
| `PM25` | `PM2.5` | Fine Particulate Matter |
| `PM10` | `PM10` | Coarse Particulate Matter |
| `O3` | `O3` | Ozone |

---

## Supabase Destination Tables

### `sensors`

Primary key: `id_installation` (has unique constraint, supports UPSERT).

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `id_installation` | text PK | Generated | `{borough_code}AA0{site_code}` |
| `id_site` | text | Generated | `X{borough_code}AA{site_code}` |
| `site_code` | text | API `@SiteCode` | e.g. `RI1`, `WA7`, `CD1` |
| `provider` | text | Hardcoded | `"Automatic"` |
| `site_name` | text | API `@SiteName` | Borough prefix stripped |
| `borough` | text | API `@LocalAuthorityName` | Full borough name |
| `lat` | float | API `@Latitude` | |
| `lon` | float | API `@Longitude` | |
| `sensor_type` | text | Hardcoded | `"Automatic"` |
| `pollutants_measured` | text[] | Derived from Species | e.g. `{NO2,PM2.5,PM10}` |
| `start_date` | date | Earliest species start | Falls back to `@DateOpened` |
| `end_date` | date | API `@DateClosed` | NULL if still active |
| `organization_id` | int | Auto-assigned | Trigger: `trigger_assign_org_sensors` |

**ID Generation**: The borough code comes from `BOROUGH_CODE` mapping (borough name -> 2 letters), NOT from the first 2 characters of the site code. Example: site `RHI` in Richmond gets `id_installation = RIAA0RHI` (RI from Richmond), not `RHAA0RHI`.

### `hourly_averages`

**No unique constraint** - only an auto-increment `id` PK. Must use INSERT, not UPSERT.

| Column | Type | Source |
|--------|------|--------|
| `id` | serial PK | Auto |
| `id_site` | text | From sensors |
| `pollutant` | text | Species mapping |
| `year` | int | Parsed from datetime |
| `month` | int | Parsed from datetime |
| `day` | int | Parsed from datetime |
| `hour` | int | Parsed from datetime |
| `value` | float | API `@Value` |
| `datetime_start` | timestamptz | Hour start (ISO 8601) |
| `datetime_end` | timestamptz | Hour end (ISO 8601) |

**This table is extremely large.** Queries without a `year` filter will timeout. The pipeline's `get_latest_hourly` scans year-by-year descending to avoid this.

### `daily_averages`

**No unique constraint** - auto-increment `id` PK.

| Column | Type | Source |
|--------|------|--------|
| `id` | serial PK | Auto |
| `id_site` | text | From sensors |
| `pollutant` | text | Species mapping |
| `value` | float | Mean of hourly values |
| `year` | int | From date |
| `month` | int | From date |
| `day` | int | From date |
| `date` | date | `YYYY-MM-DD` |
| `averaging_period` | text | `"daily"` |

**Validity rule**: Only created when >= 18 hourly readings exist for that day (75% of 24).

### `monthly_averages`

Same structure minus `day`. Date is `YYYY-MM-01`. `averaging_period = "monthly"`.

### `annual_averages`

Same structure minus `day`, `month`. Date is `YYYY-01-01`. `averaging_period = "annual"`.

**Validity rule**: Only created when >= 75% of expected hourly readings exist for that year (6570 for normal year, 6588 for leap year).

---

## Incremental Logic

The pipeline avoids re-fetching or duplicating data:

1. **Hourly**: Query the latest `(year, month, day, hour)` for each `(id_site, pollutant)` combination, scanning year-by-year from current year backwards. Only fetch data after that timestamp.

2. **Daily/Monthly/Annual**: Query the latest `date` (or `year` for annual) for each `(id_site, pollutant)`. Only insert aggregates for dates after the latest existing.

3. **Cutoff filtering**: Even within a fetched chunk, individual records at or before the cutoff timestamp are skipped during parsing. This handles the edge case where we start fetching from the cutoff day but some hours that day already exist.

---

## Batching Logic

### API Fetching

- Date ranges are split into **365-day chunks** (1 year max per request)
- For a sensor with data from 2000-2026, that's ~26 API requests per pollutant
- All chunks for one sensor x pollutant are fetched **concurrently** via `asyncio.gather`
- The `API_CONCURRENCY` semaphore (default 20) bounds how many HTTP requests are in flight globally

### DB Writing

- Rows are inserted in **200-row batches** via Supabase REST API POST
- The `DB_CONCURRENCY` semaphore (default 5) bounds concurrent writes
- Each batch has **exponential backoff retry** (up to 4 attempts): wait 2s, 3s, 5s, 9s
- Failed batches after all retries are logged to the **dead letter queue**
- Batches within a single sensor x pollutant are inserted sequentially (not concurrently) to maintain order

### Why These Numbers

| Parameter | Value | Reason |
|-----------|-------|--------|
| API concurrency: 20 | ERG API handles this comfortably | Tested with no 429s |
| DB concurrency: 5 | Supabase Cloudflare proxy limit | >5 causes 400/502/SSL errors |
| Batch size: 200 | Cloudflare request body limit | 500 rows caused payload rejections |
| Retry attempts: 4 | Handles transient Cloudflare errors | Most succeed by attempt 2-3 |
| Chunk size: 365 days | API response size limit | Larger ranges may timeout |

---

## Concurrent Worker Logic

The async engine uses three layers of concurrency control:

```
TASK_CONCURRENCY (30)        # How many sensor x pollutant combos run at once
  +-- API_CONCURRENCY (20)   # How many ERG API requests are in flight
  +-- DB_CONCURRENCY (5)     # How many Supabase writes are in flight
```

**Event loop model**: A single Python process with one thread. Tasks yield control while waiting for network I/O (API response or DB acknowledgment). While one task waits, others make progress. This is fundamentally different from threading - there's no OS thread overhead, no GIL contention, and no race conditions.

**TCP connection pooling**: The `aiohttp.ClientSession` reuses TCP connections via `TCPConnector`. The TLS handshake (expensive) happens once per host, not once per request.

---

## Things To Watch Out For

### API Quirks

1. **DNS**: `api.londonair.org.uk` does NOT resolve. Always use `api.erg.ic.ac.uk/AirQuality`.
2. **XML-to-JSON Species quirk**: When a site has only 1 species, the API returns a single dict instead of an array. The `normalise_species()` function handles this.
3. **`@DateMeasurementFinished`**: Species with this field set are no longer active. The pipeline skips them.
4. **Empty string vs null**: The API uses empty strings `""` for missing dates, not null/absent fields.

### Database Quirks

5. **No unique constraints on data tables**: `hourly_averages`, `daily_averages`, `monthly_averages`, `annual_averages` all use auto-increment `id` as PK with no composite unique constraint. This means:
   - You **cannot use UPSERT** (ON CONFLICT) - it will error
   - You **must use INSERT** with incremental logic to avoid duplicates
   - If the pipeline crashes mid-run, you may get partial data but no duplicates (incremental check catches this)

6. **hourly_averages is massive**: Any query without a `year` filter will timeout. Always include `year=eq.{year}` in queries.

7. **Organization trigger**: The `trigger_assign_org_sensors` trigger automatically sets `organization_id` when a sensor is inserted, based on borough name matching. You don't need to set it manually.

### Supabase / Cloudflare

8. **DB write concurrency**: More than 5 concurrent writes cause Cloudflare 400 Bad Request, 502 Bad Gateway, or SSL EOF errors. The `DB_CONCURRENCY` semaphore prevents this.

9. **Batch size**: Batches of 500+ rows were rejected by Cloudflare. Keep at 200.

10. **Rate limiting**: If Supabase returns HTTP 429, the pipeline reads the `Retry-After` header and sleeps accordingly. This is handled automatically.

### Windows

11. **Console encoding**: Python on Windows defaults to cp1252 which can't print Unicode arrows/special chars. The `codecs.getwriter("utf-8")` wrapper at the top of the script fixes this.

---

## Data Flow Per Sensor x Pollutant

```
1. Check latest hourly record in DB (year-by-year scan)
   +-- If up to date -> skip entirely

2. Generate yearly chunks from effective_start to today
   +-- e.g. 2021-01-01->2021-12-31, 2022-01-01->2022-12-31, ...

3. Fetch all chunks concurrently (bounded by API semaphore)
   +-- Each chunk: GET /Data/SiteSpecies/SiteCode=.../SpeciesCode=.../StartDate=.../EndDate=.../Json
   +-- Parse response, extract (datetime, value) pairs

4. Filter out records at or before cutoff timestamp

5. Build hourly rows + aggregation buckets (daily/monthly/annual)

6. INSERT hourly rows in 200-row batches (bounded by DB semaphore)
   +-- Retry with exponential backoff on failure

7. Check latest daily/monthly/annual records in DB
8. Compute averages (with validity rules) for dates after latest
9. INSERT aggregate rows
```
