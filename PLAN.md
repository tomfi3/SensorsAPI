# Plan: Extend Air Quality Sensors to All London Boroughs

## Summary of Requirements

Based on user answers:
- **Scope**: All boroughs across London, Automatic sensors only
- **Discovery**: Use London Air API to auto-discover sensors and create missing ones in DB
- **Inactive sensors**: Include decommissioned sensors (with end_date set)
- **Ozone**: Ensure O3 measurements are pulled (already structurally supported)
- **Rate limiting**: Add configurable delays between API calls

---

## Changes to `pipeline.py`

### Step 1: Add London Air API discovery endpoint constant

Add a new constant for the MonitoringSiteSpecies endpoint:
```
SITES_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json"
```

### Step 2: Add rate limiting infrastructure

- Import `time` module
- Add a configurable `API_CALL_DELAY` constant (default 0.5s, overridable via `API_CALL_DELAY` env var)
- Add a `rate_limited_get()` wrapper around `requests.get()` that sleeps for the configured delay after each call

### Step 3: New function `discover_and_sync_sensors(supabase)`

This is the core new functionality. It will:

1. **Call the London Air API** at the MonitoringSiteSpecies endpoint to get all London sites and their species
2. **Parse the response** extracting per site:
   - `@SiteCode` → `site_code` and `id_site`
   - `@SiteName` → `site_name`
   - `@SiteType` → `site_type_description` (note: this is "Suburban"/"Roadside"/etc, NOT "Automatic"/"Diffusion Tube")
   - `@Latitude` / `@Longitude` → `latitude`, `longitude`
   - `@DateOpened` → `start_date`
   - `@DateClosed` → `end_date` (NULL if empty string)
   - `@LocalAuthorityName` → `borough`
   - `Species[].@SpeciesCode` → `pollutants_measured` array
3. **Handle Species normalization**: The API `Species` field can be a single dict or a list — normalize to list
4. **Filter**: Only include sites where `site_code` passes the existing regex validation (`^[A-Za-z]+[0-9]*$`)
5. **Query existing sensors** from DB to determine which are new
6. **Upsert new sensors** into the `sensors` table with `sensor_type = 'Automatic'`
7. **Update existing sensors'** `pollutants_measured` if the API reports new species (e.g., O3 was added to a site)

**Key design decision**: Set `sensor_type = 'Automatic'` for all discovered sensors. The MonitoringSiteSpecies API endpoint returns continuously-monitored stations (automatic), not diffusion tubes. Diffusion tubes don't have species-level data in this endpoint.

**Key design decision for `id_site`**: Use `@SiteCode` as the `id_site` value for new sensors. This is the natural unique identifier from the API. Need to verify this is consistent with existing data — if existing sensors use a different `id_site` scheme, we'll need to map accordingly. **Assumption**: existing `id_site` values match `@SiteCode`. If not, we'll use `site_code` as a lookup key and preserve whatever `id_site` scheme exists.

### Step 4: Modify `load_sensors(supabase)`

Remove filters that restrict scope:
- **Remove**: `.is_('end_date', 'null')` — to include inactive/decommissioned sensors
- **Remove**: `.in_('borough', ['Wandsworth', 'Richmond', 'Merton'])` — to include all boroughs
- **Keep**: `.eq('sensor_type', 'Automatic')` — user confirmed Automatic only

Updated query:
```python
result = supabase.table('sensors').select('*')\
    .eq('sensor_type', 'Automatic')\
    .execute()
```

### Step 5: Add rate limiting to API fetch functions

Modify `fetch_annual_monthly_data()` and `fetch_hourly_and_calculate_daily()` to use the `rate_limited_get()` wrapper instead of raw `requests.get()`.

### Step 6: Update `main()` flow

New execution order:
1. `get_supabase_client()`
2. `discover_and_sync_sensors(supabase)` — **NEW**: discover all sensors from API, create missing ones
3. `load_sensors(supabase)` — now returns all Automatic sensors across all boroughs (including inactive)
4. Process sensors as before (existing loop unchanged)

### Step 7: Ozone (O3) handling

No code changes needed for O3 specifically. The pipeline is already pollutant-agnostic — it reads `pollutants_measured` from each sensor and processes each code. The discovery function in Step 3 will populate `pollutants_measured` with whatever species the API reports for each site, including O3 where applicable.

The existing PM25 normalization (`pollutant.replace('.', '')`) in `main()` also correctly handles the O3 code (no dots to strip).

---

## Changes to `server.py`

### Step 8: Increase pipeline timeout

With potentially hundreds of sensors across all of London, the 10-minute (600s) timeout may be insufficient. Increase to 30 minutes (1800s) or make it configurable via env var.

---

## No changes needed to:
- `requirements.txt` — no new dependencies (uses existing `requests`, `time` is stdlib)
- `.github/workflows/daily-pipeline.yml` — GitHub Actions has a 6-hour default job timeout, which should be sufficient
- Database schema — existing `sensors`, `annual_averages`, `monthly_averages`, `daily_averages` tables already support all needed fields

---

## Risk Assessment

1. **API response format assumption**: The MonitoringSiteSpecies JSON structure is based on documentation research, not a live response test. The parsing code should handle gracefully if the structure differs slightly (e.g., missing fields, different nesting).

2. **id_site collision**: If existing sensors use a different `id_site` format than `@SiteCode`, inserting new sensors with `id_site = @SiteCode` could cause inconsistencies. Mitigation: look up existing sensors by `site_code` first.

3. **Volume increase**: Going from ~3 boroughs to all of London could mean 100+ sensors × multiple pollutants × years of backfill. The daily pipeline run time will increase significantly. Rate limiting helps avoid API issues but extends run time further.

4. **Duplicate data**: The current `upload_to_supabase` uses `.insert()` which may fail on duplicate keys. This is an existing issue, not introduced by this change, but becomes more likely at scale. Consider using `.upsert()` as a follow-up improvement.
