"""
London Air Data Pipeline (async)

Phase 1: Sync sensors from MonitoringSiteSpecies API -> Supabase sensors table
Phase 2: Fetch monitoring data with async I/O engine (aiohttp + semaphores)

Architecture:
    - Single aiohttp.ClientSession with TCP connection pooling
    - API Semaphore (20): bounds concurrent ERG API requests
    - DB Semaphore (5): bounds concurrent Supabase writes (Cloudflare limit)
    - Pre-computed task list: all sensor x pollutant x yearly chunks
    - Exponential backoff + dead letter queue for resilience

Usage:
    python pipeline.py                          # Full pipeline
    python pipeline.py --sensors-only           # Phase 1 only
    python pipeline.py --data-only              # Phase 2 only
    python pipeline.py --dry-run                # Preview without writing
    python pipeline.py --sites RI1,WA7          # Filter to specific sites
    python pipeline.py --start-date 2021-01-01  # Override start date
"""

import os
import asyncio
import aiohttp
import pandas as pd
import requests
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
import logging
import re

import aiohttp
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# Fix Windows console encoding
if sys.platform == "win32":
    sys.stdout = codecs.getwriter("utf-8")(sys.stdout.buffer, "strict")
    sys.stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, "strict")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# API endpoints
ANNUAL_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringReport/SiteCode={}/Year={}/json"
HOURLY_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Data/Site/SiteCode={}/StartDate={}/EndDate={}/Json"
SITES_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies/GroupName=London/Json"

# Configurable via env
MAX_CONCURRENT_REQUESTS = int(os.environ.get("MAX_CONCURRENT_REQUESTS", "20"))
API_CALL_DELAY = float(os.environ.get("API_CALL_DELAY", "0.2"))
HTTP_MAX_RETRIES = int(os.environ.get("HTTP_MAX_RETRIES", "3"))
BATCH_SIZE = 500


def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        database_url = os.environ.get("DATABASE_URL")
        if database_url and "supabase" in database_url:
            parts = database_url.split("@")[1].split(".")
            project_ref = parts[0]
            url = f"https://{project_ref}.supabase.co"
            logger.warning("SUPABASE_URL and SUPABASE_KEY not found. Using DATABASE_URL for connection.")
        else:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required")

    return create_client(url, key)


def _parse_date(date_str: str) -> Optional[datetime]:
    """Parse a date string from the API, returning None if empty/invalid."""
    if not date_str or not date_str.strip():
        return None
    try:
        return datetime.strptime(date_str.strip().split('.')[0], '%Y-%m-%d %H:%M:%S')
    except (ValueError, IndexError):
        try:
            return datetime.strptime(date_str.strip()[:10], '%Y-%m-%d')
        except ValueError:
            return None


def discover_and_sync_sensors(supabase: Client) -> None:
    """Discover all London sensors from the API and sync to Supabase."""
    logger.info("Starting sensor discovery from London Air API")

    try:
        response = requests.get(SITES_API_URL, timeout=60)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        logger.error(f"Failed to fetch sensor list from API: {e}")
        return

    raw_sites = data.get("Sites", {}).get("Site", [])
    if not raw_sites:
        logger.warning("No sites returned from API")
        return

    # Merge duplicate site entries (API returns same SiteCode multiple times)
    sites_by_code = {}
    for site in raw_sites:
        code = site.get("@SiteCode", "")
        if not code:
            continue
        if code in sites_by_code:
            existing = sites_by_code[code]
            new_species = site.get("Species", [])
            if isinstance(new_species, dict):
                new_species = [new_species]
            existing["_species_list"].extend(new_species)
        else:
            species = site.get("Species", [])
            if isinstance(species, dict):
                species = [species]
            site["_species_list"] = list(species)
            sites_by_code[code] = site

    logger.info(f"API returned {len(raw_sites)} entries, {len(sites_by_code)} unique sites")

    # Query existing sensors from DB
    try:
        existing = supabase.table('sensors').select(
            'site_code, id_installation, pollutants_measured, borough, end_date'
        ).execute()
        existing_map = {row['site_code']: row for row in existing.data}
    except Exception as e:
        logger.error(f"Failed to query existing sensors: {e}")
        existing_map = {}

    # Build borough prefix map from existing sensors
    borough_prefix_map = {}
    for row in existing_map.values():
        if row.get('borough') and row.get('id_installation'):
            id_inst = row['id_installation']
            if 'AA0' in id_inst:
                prefix = id_inst.split('AA0')[0]
                borough_prefix_map[row['borough']] = prefix

    n_created = 0
    n_updated = 0
    n_skipped = 0

    for site_code, site in sites_by_code.items():
        # Validate site_code
        if not re.match(r'^[A-Za-z]+[0-9]*$', site_code):
            n_skipped += 1
            continue

        site_name = site.get("@SiteName", "")
        borough = site.get("@LocalAuthorityName", "")
        lat_str = site.get("@Latitude", "")
        lon_str = site.get("@Longitude", "")
        date_opened = site.get("@DateOpened", "")
        date_closed = site.get("@DateClosed", "")

        # Parse coordinates
        try:
            lat = float(lat_str) if lat_str and lat_str.strip() else None
        except (ValueError, TypeError):
            lat = None
        try:
            lon = float(lon_str) if lon_str and lon_str.strip() else None
        except (ValueError, TypeError):
            lon = None

        # Parse dates
        start_date = _parse_date(date_opened)
        end_date = _parse_date(date_closed)

        # Build deduplicated pollutants list
        species_list = site.get("_species_list", [])
        api_pollutants = list(set(
            s.get("@SpeciesCode", "") for s in species_list if s.get("@SpeciesCode")
        ))
        api_pollutants.sort()

        if not api_pollutants:
            n_skipped += 1
            continue

        start_date_str = start_date.strftime('%Y-%m-%d') if start_date else None
        end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None

        if site_code in existing_map:
            # Site exists — check if pollutants or end_date need updating
            existing_row = existing_map[site_code]
            existing_pollutants = existing_row.get('pollutants_measured') or []
            updates = {}

            # Merge pollutants
            merged = sorted(set(existing_pollutants + api_pollutants))
            if merged != sorted(existing_pollutants):
                updates['pollutants_measured'] = merged

            # Update end_date if changed
            existing_end = existing_row.get('end_date')
            if end_date_str != existing_end:
                updates['end_date'] = end_date_str

            if updates:
                try:
                    supabase.table('sensors').update(updates).eq(
                        'site_code', site_code
                    ).execute()
                    n_updated += 1
                except Exception as e:
                    logger.error(f"Failed to update sensor {site_code}: {e}")
        else:
            # New site — determine IDs and insert
            if borough in borough_prefix_map:
                prefix = borough_prefix_map[borough]
            else:
                prefix = site_code[:2].upper()
                borough_prefix_map[borough] = prefix

            id_installation = f"{prefix}AA0{site_code}"
            id_site = f"X{prefix}AA{site_code}"

            record = {
                'id_installation': id_installation,
                'id_site': id_site,
                'site_code': site_code,
                'site_name': site_name,
                'borough': borough,
                'lat': lat,
                'lon': lon,
                'start_date': start_date_str,
                'end_date': end_date_str,
                'sensor_type': 'Automatic',
                'provider': 'Automatic',
                'pollutants_measured': api_pollutants,
            }

            try:
                supabase.table('sensors').insert(record).execute()
                n_created += 1
            except Exception as e:
                logger.error(f"Failed to insert sensor {site_code}: {e}")

    logger.info(f"Sensor discovery: {n_created} created, {n_updated} updated, {n_skipped} skipped")


def load_sensors(supabase: Client) -> pd.DataFrame:
    """Load all automatic sensors from Supabase (all boroughs, active and inactive)."""
    logger.info("Loading sensor configuration from Supabase sensors table")

    result = supabase.table('sensors').select('*')\
        .eq('sensor_type', 'Automatic')\
        .execute()

    if not result.data:
        logger.warning("No automatic sensors found")
        return pd.DataFrame()

    df = pd.DataFrame(result.data)
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    df = df[df['start_date'].notna()]
    df = df[df['site_code'].str.match(r'^[A-Za-z]+[0-9]*$')]

    logger.info(f"Loaded {len(df)} sensors")
    return df


def get_latest_period(supabase: Client, id_site: str, pollutant: str, table: str) -> Optional[dict]:
    try:
        lat = float(lat_str)
        lon = float(lon_str)
    except (ValueError, TypeError):
        return None
    if lat == 0 or lon == 0:
        return None


def determine_missing_periods(sensor_row: pd.Series, pollutant: str, supabase: Client) -> dict:
    id_site = sensor_row['id_site']
    site_code = sensor_row['site_code']
    start_date = sensor_row['start_date']
    current_date = datetime.now()

    latest_annual = get_latest_period(supabase, id_site, pollutant, 'annual_averages')
    latest_monthly = get_latest_period(supabase, id_site, pollutant, 'monthly_averages')
    latest_daily = get_latest_period(supabase, id_site, pollutant, 'daily_averages')

    missing = {'id_site': id_site, 'site_code': site_code, 'pollutant': pollutant,
               'annual_years': [], 'monthly_periods': [], 'daily_dates': []}

    start_year = start_date.year
    current_year = current_date.year
    last_complete_year = current_year - 1

    if latest_annual:
        annual_start_year = latest_annual['year'] + 1
    else:
        annual_start_year = start_year
    missing['annual_years'] = list(range(annual_start_year, last_complete_year + 1))

    if latest_monthly:
        monthly_start = datetime(latest_monthly['year'], latest_monthly['month'], 1) + timedelta(days=32)
        monthly_start = monthly_start.replace(day=1)
    else:
        monthly_start = start_date.replace(day=1)

    last_complete_month = (current_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    month_cursor = monthly_start
    while month_cursor <= last_complete_month:
        missing['monthly_periods'].append({'year': month_cursor.year, 'month': month_cursor.month})
        month_cursor = (month_cursor + timedelta(days=32)).replace(day=1)

    if latest_daily:
        daily_start = pd.to_datetime(latest_daily['date']) + timedelta(days=1)
    else:
        daily_start = start_date
    yesterday = (current_date - timedelta(days=1)).date()
    day_cursor = daily_start.date() if isinstance(daily_start, pd.Timestamp) else daily_start
    while day_cursor <= yesterday:
        missing['daily_dates'].append(day_cursor)
        day_cursor += timedelta(days=1)

    return missing


async def fetch_json(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    timeout: int = 30
) -> dict:
    """Async HTTP GET with rate limiting and retry."""
    last_error = None
    for attempt in range(HTTP_MAX_RETRIES):
        try:
            async with semaphore:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as resp:
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                    await asyncio.sleep(API_CALL_DELAY)
                    return data
        except Exception as e:
            last_error = e
            if attempt < HTTP_MAX_RETRIES - 1:
                wait = 2 ** attempt
                logger.warning(f"Retry {attempt+1}/{HTTP_MAX_RETRIES} for {url}: {e}")
                await asyncio.sleep(wait)
    raise last_error


async def fetch_annual_monthly_data(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    site_code: str,
    year: int,
    pollutant: str
) -> tuple:
    """Fetch annual and monthly data in a single async API call."""
    url = ANNUAL_API_URL.format(site_code, year)
    data = await fetch_json(session, semaphore, url)

    site_report = data.get("SiteReport", {})
    report_items = site_report.get("ReportItem", [])
    pollutant_code = "PM25" if pollutant.upper() in ["PM25", "PM2.5"] else pollutant

    annual_value = None
    monthly_results = []

    for item in report_items:
        if item.get("@SpeciesCode") == pollutant_code and item.get("@ReportItem") == "7":
            val = item.get("@Annual")
            if val and val != "-999":
                try:
                    annual_value = float(val)
                except ValueError:
                    pass
            for m in range(1, 13):
                m_val = item.get(f"@Month{m}")
                if m_val and m_val != "-999":
                    try:
                        monthly_results.append({'year': year, 'month': m, 'value': float(m_val)})
                    except ValueError:
                        continue
            break

    return annual_value, monthly_results


async def fetch_hourly_and_calculate_daily(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    site_code: str,
    start_date,
    end_date,
    pollutant: str
) -> list:
    """Fetch hourly data and calculate daily averages (min 18 readings)."""
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    url = HOURLY_API_URL.format(site_code, start_str, end_str)
    data = await fetch_json(session, semaphore, url, timeout=60)

    hourly_data = []
    for item in data.get("AirQualityData", {}).get("Data", []):
        if item.get("@SpeciesCode") == pollutant:
            ts = item.get("@MeasurementDateGMT")
            val = item.get("@Value")
            if ts and val and val != "-999":
                try:
                    hourly_data.append({'timestamp': pd.to_datetime(ts), 'value': float(val)})
                except (ValueError, TypeError):
                    continue

    if not hourly_data:
        return []

    df = pd.DataFrame(hourly_data)
    df['date'] = df['timestamp'].dt.date

    daily_averages = []
    for date_val, group in df.groupby('date'):
        if len(group) >= 18:
            daily_averages.append({'date': date_val, 'value': group['value'].mean()})
    return daily_averages


def upload_to_supabase(supabase: Client, table: str, records: list):
    """Insert records in batches to Supabase."""
    if not records:
        return
    try:
        for i in range(0, len(records), BATCH_SIZE):
            chunk = records[i:i + BATCH_SIZE]
            supabase.table(table).insert(chunk).execute()
    except Exception as e:
        logger.error(f"Error uploading {len(records)} records to {table}: {e}")


async def process_sensor_pollutant(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    sensor_row: pd.Series,
    pollutant: str,
    supabase: Client
) -> None:
    """Process a single sensor-pollutant pair: find gaps, fetch data, upload."""
    site_code = sensor_row['site_code']
    logger.info(f"Processing {site_code} - {pollutant}")

    # Determine missing periods (DB queries, run in thread)
    missing = await asyncio.to_thread(determine_missing_periods, sensor_row, pollutant, supabase)

    total_missing = len(missing['annual_years']) + len(missing['monthly_periods']) + len(missing['daily_dates'])
    if total_missing == 0:
        logger.info(f"No missing data for {site_code} - {pollutant}")
        return

    # --- ANNUAL + MONTHLY FETCH (concurrent) ---
    annual_records = []
    monthly_records = []

    if missing['annual_years']:
        tasks = [
            fetch_annual_monthly_data(session, semaphore, site_code, year, pollutant)
            for year in missing['annual_years']
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for year, result in zip(missing['annual_years'], results):
            if isinstance(result, Exception):
                logger.error(f"Error fetching annual data {site_code}/{pollutant}/{year}: {result}")
                continue
            annual_val, monthly_vals = result
            if annual_val is not None:
                annual_records.append({
                    'id_site': sensor_row['id_site'],
                    'pollutant': pollutant,
                    'value': annual_val,
                    'year': year,
                    'date': f"{year}-01-01",
                    'averaging_period': 'annual'
                })
            for mv in monthly_vals:
                if any(p['year'] == mv['year'] and p['month'] == mv['month'] for p in missing['monthly_periods']):
                    monthly_records.append({
                        'id_site': sensor_row['id_site'],
                        'pollutant': pollutant,
                        'value': mv['value'],
                        'year': mv['year'],
                        'month': mv['month'],
                        'date': f"{mv['year']}-{mv['month']:02d}-01",
                        'averaging_period': 'monthly'
                    })

    # Upload annual + monthly
    if annual_records:
        await asyncio.to_thread(upload_to_supabase, supabase, 'annual_averages', annual_records)
    if monthly_records:
        await asyncio.to_thread(upload_to_supabase, supabase, 'monthly_averages', monthly_records)

    # --- DAILY FETCH ---
    # Only fetch daily data if monthly data exists (cutoff logic)
    monthly_result = await asyncio.to_thread(
        lambda: supabase.table('monthly_averages')
        .select('year, month')
        .eq('id_site', sensor_row['id_site']).eq('pollutant', pollutant)
        .order('year', desc=False).order('month', desc=False).limit(1).execute()
    )

    daily_dates_to_fetch = missing['daily_dates']
    if monthly_result.data:
        first_month = monthly_result.data[0]
        first_month_date = datetime(first_month['year'], first_month['month'], 1)
        daily_cutoff_start = (first_month_date - pd.DateOffset(months=2)).date()
        daily_dates_to_fetch = [d for d in daily_dates_to_fetch if d >= daily_cutoff_start]

    if not daily_dates_to_fetch:
        logger.info(f"Completed {site_code} - {pollutant}: {len(annual_records)} annual, {len(monthly_records)} monthly, 0 daily")
        return

    # Build 90-day chunks
    chunks = []
    i = 0
    while i < len(daily_dates_to_fetch):
        chunk_start = daily_dates_to_fetch[i]
        chunk_end_limit = chunk_start + timedelta(days=90)
        end_idx = i
        while end_idx < len(daily_dates_to_fetch) and daily_dates_to_fetch[end_idx] <= chunk_end_limit:
            end_idx += 1
        end_idx = min(end_idx, len(daily_dates_to_fetch)) - 1
        chunk_end = daily_dates_to_fetch[end_idx]
        chunks.append((chunk_start, chunk_end))
        i = end_idx + 1

    # Fetch all chunks concurrently
    daily_tasks = [
        fetch_hourly_and_calculate_daily(session, semaphore, site_code, cs, ce, pollutant)
        for cs, ce in chunks
    ]
    daily_results = await asyncio.gather(*daily_tasks, return_exceptions=True)

    all_daily_records = []
    for result in daily_results:
        if isinstance(result, Exception):
            logger.error(f"Error fetching daily data for {site_code}/{pollutant}: {result}")
            continue
        for r in result:
            d = r['date']
            all_daily_records.append({
                'id_site': sensor_row['id_site'],
                'pollutant': pollutant,
                'value': r['value'],
                'year': d.year,
                'month': d.month,
                'day': d.day,
                'date': str(d),
                'averaging_period': 'daily'
            })

    # Single batch upload for all daily records
    if all_daily_records:
        await asyncio.to_thread(upload_to_supabase, supabase, 'daily_averages', all_daily_records)

    logger.info(
        f"Completed {site_code} - {pollutant}: "
        f"{len(annual_records)} annual, {len(monthly_records)} monthly, {len(all_daily_records)} daily"
    )


async def async_pipeline(supabase: Client, sensors_df: pd.DataFrame) -> None:
    """Async entry point: create session and launch all workers."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession() as session:
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

        logger.info(
            f"Processing {len(tasks)} sensor-pollutant combinations "
            f"with max {MAX_CONCURRENT_REQUESTS} concurrent requests"
        )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        succeeded = sum(1 for r in results if not isinstance(r, Exception))
        failed = sum(1 for r in results if isinstance(r, Exception))
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Worker failed: {r}")
        logger.info(f"Pipeline complete: {succeeded} succeeded, {failed} failed")


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


if __name__ == "__main__":
    main()
