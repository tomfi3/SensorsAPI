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
    python pipeline.py --backfill               # Fast bulk load via direct Postgres
    python pipeline.py --dry-run                # Preview without writing
    python pipeline.py --sites RI1,WA7          # Filter to specific sites
    python pipeline.py --start-date 2021-01-01  # Override start date
"""

import os
import sys
import codecs
import argparse
import asyncio
import math
from datetime import datetime, timedelta
from typing import Optional
from collections import defaultdict
import logging

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

# ── Environment ───────────────────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# ── API Configuration ─────────────────────────────────────────────────────────

API_BASE = "https://api.erg.ic.ac.uk/AirQuality"
SITE_SPECIES_URL = f"{API_BASE}/Information/MonitoringSiteSpecies/GroupName=London/Json"
MONITORING_REPORT_URL = API_BASE + "/Annual/MonitoringReport/SiteCode={site_code}/Year={year}/json"

# Tuning knobs (configurable via env)
API_CONCURRENCY = int(os.environ.get("API_CONCURRENCY", "20"))
DB_CONCURRENCY = int(os.environ.get("DB_CONCURRENCY", "5"))
TASK_CONCURRENCY = int(os.environ.get("TASK_CONCURRENCY", "30"))
INSERT_BATCH_SIZE = int(os.environ.get("INSERT_BATCH_SIZE", "200"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "4"))
MAX_DAYS_PER_CHUNK = 365

# Backfill mode: direct Postgres (bypasses Supabase REST / Cloudflare)
PG_CONCURRENCY = int(os.environ.get("PG_CONCURRENCY", "20"))
PG_BATCH_SIZE = int(os.environ.get("PG_BATCH_SIZE", "1000"))
DATABASE_URL = os.environ.get("DATABASE_URL")

# Species we track (API code -> DB pollutant name)
SPECIES_MAP = {
    "NO2": "NO2",
    "PM25": "PM2.5",
    "PM10": "PM10",
    "O3": "O3",
}

# Borough name -> 2-letter code for ID generation
BOROUGH_CODE = {
    "Barking and Dagenham": "BG",
    "Barnet": "BN",
    "Bexley": "BX",
    "Brent": "BR",
    "Bromley": "BM",
    "Camden": "CD",
    "City of London": "CT",
    "Corporation of London": "CT",
    "Croydon": "CR",
    "Ealing": "EA",
    "Enfield": "EN",
    "Greenwich": "GR",
    "Hackney": "HK",
    "Hammersmith and Fulham": "HF",
    "Haringey": "HR",
    "Harrow": "HW",
    "Havering": "HV",
    "Hillingdon": "HI",
    "Hounslow": "HO",
    "Islington": "IS",
    "Kensington and Chelsea": "KC",
    "Kingston upon Thames": "KT",
    "Kingston": "KT",
    "Lambeth": "LB",
    "Lewisham": "LW",
    "Merton": "ME",
    "Newham": "NM",
    "Redbridge": "RB",
    "Richmond": "RI",
    "Richmond upon Thames": "RI",
    "Southwark": "SK",
    "Sutton": "ST",
    "Tower Hamlets": "TH",
    "Waltham Forest": "WF",
    "Wandsworth": "WA",
    "Westminster": "WM",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_borough_code(borough: str, site_code: str) -> str:
    return BOROUGH_CODE.get(borough, site_code[:2].upper())


def generate_ids(site_code: str, borough: str) -> tuple[str, str]:
    code = get_borough_code(borough, site_code)
    sc = site_code.upper()
    return (f"{code}AA0{sc}", f"X{code}AA{sc}")


def clean_site_name(raw: str, borough: str) -> str:
    for prefix in [f"{borough} - ", f"{borough} -", "- "]:
        if raw.startswith(prefix):
            return raw[len(prefix):].strip()
    return raw.strip()


def normalise_species(species) -> list[dict]:
    """Handle the XML-to-JSON quirk: single dict vs list of dicts."""
    if isinstance(species, list):
        return species
    if isinstance(species, dict):
        return [species]
    return []


def parse_api_date(raw: Optional[str]) -> Optional[str]:
    if not raw or raw.strip() == "":
        return None
    try:
        dt = datetime.strptime(raw.strip(), "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def extract_pollutants(species) -> list[str]:
    result = []
    for sp in normalise_species(species):
        code = sp.get("@SpeciesCode", "")
        if code in SPECIES_MAP:
            result.append(SPECIES_MAP[code])
    return result


def get_earliest_species_date(species) -> Optional[str]:
    earliest = None
    for sp in normalise_species(species):
        d = parse_api_date(sp.get("@DateMeasurementStarted"))
        if d and (earliest is None or d < earliest):
            earliest = d
    return earliest


# ── Phase 1: Sync Sensors (sync, runs once) ──────────────────────────────────

def fetch_site_species() -> list[dict]:
    logger.info(f"Fetching {SITE_SPECIES_URL}")
    resp = requests.get(SITE_SPECIES_URL, headers={"Accept": "application/json"}, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    container = data.get("Sites", data)
    sites = container.get("Site", [])
    if not isinstance(sites, list):
        sites = [sites] if sites else []

    logger.info(f"Retrieved {len(sites)} sites from API")
    return sites


def api_site_to_row(site: dict) -> Optional[dict]:
    site_code = site.get("@SiteCode", "")
    borough = site.get("@LocalAuthorityName", "")
    lat_str = site.get("@Latitude", "")
    lon_str = site.get("@Longitude", "")

    try:
        lat = float(lat_str)
        lon = float(lon_str)
    except (ValueError, TypeError):
        return None
    if lat == 0 or lon == 0:
        return None

    id_inst, id_site = generate_ids(site_code, borough)
    start_date = get_earliest_species_date(site.get("Species", []))
    if not start_date:
        start_date = parse_api_date(site.get("@DateOpened"))
    end_date = parse_api_date(site.get("@DateClosed"))

    return {
        "id_installation": id_inst,
        "id_site": id_site,
        "site_code": site_code,
        "provider": "Automatic",
        "site_name": clean_site_name(site.get("@SiteName", site_code), borough),
        "borough": borough,
        "lat": lat,
        "lon": lon,
        "sensor_type": "Automatic",
        "pollutants_measured": extract_pollutants(site.get("Species", [])),
        "start_date": start_date,
        "end_date": end_date,
    }


def sync_sensors(supabase: Client, api_sites: list[dict], dry_run: bool) -> dict[str, dict]:
    logger.info("Phase 1: Syncing sensors...")

    api_rows = {}
    skipped = []
    for site in api_sites:
        row = api_site_to_row(site)
        if row:
            api_rows[row["site_code"]] = row
        else:
            skipped.append(site.get("@SiteCode", "?"))

    if skipped:
        logger.info(f"Skipped {len(skipped)} sites (missing coords): {', '.join(skipped[:20])}")

    logger.info("Fetching existing Automatic sensors from database...")
    result = supabase.table("sensors").select(
        "id_installation, id_site, site_code, pollutants_measured, end_date, start_date"
    ).eq("sensor_type", "Automatic").execute()

    existing = {r["site_code"]: r for r in (result.data or [])}
    logger.info(f"Existing: {len(existing)} sensors in DB | API: {len(api_rows)} sites")

    to_insert = []
    to_update = []
    unchanged = 0

    for code, row in api_rows.items():
        ex = existing.get(code)
        if not ex:
            to_insert.append(row)
        else:
            changes = {}
            new_poll = sorted(row.get("pollutants_measured") or [])
            old_poll = sorted(ex.get("pollutants_measured") or [])
            if new_poll != old_poll:
                changes["pollutants_measured"] = row["pollutants_measured"]
            if (row.get("end_date") or None) != (ex.get("end_date") or None):
                changes["end_date"] = row.get("end_date")
            if not ex.get("start_date") and row.get("start_date"):
                changes["start_date"] = row["start_date"]
            if changes:
                changes["id_installation"] = ex["id_installation"]
                to_update.append(changes)
            else:
                unchanged += 1

    logger.info(f"New: {len(to_insert)} | Updated: {len(to_update)} | Unchanged: {unchanged}")

    if dry_run:
        for r in to_insert[:15]:
            logger.info(f"  [DRY RUN] + {r['site_code']} -> {r['id_installation']} ({r['site_name']}, {r['borough']})")
    else:
        if to_insert:
            for i in range(0, len(to_insert), 100):
                batch = to_insert[i:i + 100]
                try:
                    supabase.table("sensors").upsert(batch, on_conflict="id_installation").execute()
                    logger.info(f"Inserted sensors batch {i + 1}-{min(i + 100, len(to_insert))}")
                except Exception as e:
                    logger.error(f"Error inserting sensors batch {i}: {e}")
        for upd in to_update:
            id_inst = upd.pop("id_installation")
            try:
                supabase.table("sensors").update(upd).eq("id_installation", id_inst).execute()
            except Exception as e:
                logger.error(f"Error updating sensor {id_inst}: {e}")

    return api_rows


# ── Phase 2: Async Monitoring Data Engine ─────────────────────────────────────

def _sb_headers() -> dict:
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }


def _sb_rest_url(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


async def sb_get_latest_hourly(
    session: aiohttp.ClientSession, db_sem: asyncio.Semaphore,
    id_site: str, pollutant: str,
) -> Optional[str]:
    """Scan year-by-year to find latest hourly record (avoids timeout on large table)."""
    current_year = datetime.now().year
    for year in range(current_year, current_year - 30, -1):
        url = (
            f"{_sb_rest_url('hourly_averages')}"
            f"?select=year,month,day,hour"
            f"&id_site=eq.{id_site}&pollutant=eq.{pollutant}&year=eq.{year}"
            f"&order=month.desc,day.desc,hour.desc&limit=1"
        )
        async with db_sem:
            try:
                async with session.get(url, headers=_sb_headers()) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data:
                            r = data[0]
                            return f"{r['year']}-{r['month']:02d}-{r['day']:02d} {r['hour']:02d}:00:00"
            except Exception:
                pass
    return None


async def sb_get_latest_agg(
    session: aiohttp.ClientSession, db_sem: asyncio.Semaphore,
    table: str, id_site: str, pollutant: str,
) -> Optional[str]:
    col = "date" if table != "annual_averages" else "year"
    url = (
        f"{_sb_rest_url(table)}"
        f"?select={col}"
        f"&id_site=eq.{id_site}&pollutant=eq.{pollutant}"
        f"&order={col}.desc&limit=1"
    )
    async with db_sem:
        try:
            async with session.get(url, headers=_sb_headers()) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data:
                        return str(data[0][col])
        except Exception:
            pass
    return None


async def sb_insert_batch(
    session: aiohttp.ClientSession, db_sem: asyncio.Semaphore,
    table: str, rows: list[dict], tag: str,
) -> int:
    url = _sb_rest_url(table)
    headers = _sb_headers()

    for attempt in range(MAX_RETRIES):
        async with db_sem:
            try:
                async with session.post(url, headers=headers, json=rows) as resp:
                    if resp.status in (200, 201):
                        return len(rows)
                    body = await resp.text()
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        wait = int(retry_after) if retry_after else (2 ** attempt + 1)
                    else:
                        wait = 2 ** attempt + 1
                        if attempt == MAX_RETRIES - 1:
                            logger.error(f"{tag}: {table} insert failed ({resp.status}) after {MAX_RETRIES} retries: {body[:200]}")
                            return 0
            except Exception as e:
                wait = 2 ** attempt + 1
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"{tag}: {table} insert exception after {MAX_RETRIES} retries: {e}")
                    return 0
        await asyncio.sleep(wait)
    return 0


async def sb_insert_rows(
    session: aiohttp.ClientSession, db_sem: asyncio.Semaphore,
    table: str, rows: list[dict], tag: str,
) -> int:
    if not rows:
        return 0
    inserted = 0
    for i in range(0, len(rows), INSERT_BATCH_SIZE):
        batch = rows[i:i + INSERT_BATCH_SIZE]
        count = await sb_insert_batch(session, db_sem, table, batch, tag)
        inserted += count
    return inserted


async def fetch_chunk_async(
    session: aiohttp.ClientSession, api_sem: asyncio.Semaphore,
    site_code: str, species_code: str, start: str, end: str,
) -> list[dict]:
    url = (
        f"{API_BASE}/Data/SiteSpecies"
        f"/SiteCode={site_code}/SpeciesCode={species_code}"
        f"/StartDate={start}/EndDate={end}/Json"
    )
    for attempt in range(MAX_RETRIES):
        async with api_sem:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=90)) as resp:
                    if resp.status == 429:
                        retry_after = resp.headers.get("Retry-After")
                        wait = int(retry_after) if retry_after else (2 ** attempt + 1)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        return []
                    data = await resp.json(content_type=None)
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
                    continue
                return []

        records = (data.get("RawAQData") or data.get("AirQualityData") or {}).get("Data", [])
        if not isinstance(records, list):
            return []

        results = []
        for rec in records:
            date_str = rec.get("@MeasurementDateGMT") or rec.get("@DateTime") or rec.get("@Date")
            val_str = rec.get("@Value") or rec.get("@Concentration")
            if not date_str or val_str is None or val_str == "":
                continue
            try:
                results.append({"date": date_str, "value": float(val_str)})
            except (ValueError, TypeError):
                continue
        return results
    return []


# Locks for deduplicating concurrent MonitoringReport fetches for the same site+year
_report_locks: dict[tuple[str, int], asyncio.Lock] = {}


async def fetch_monitoring_report_async(
    session: aiohttp.ClientSession, api_sem: asyncio.Semaphore,
    site_code: str, year: int,
    report_cache: dict[tuple[str, int], dict],
) -> dict[str, dict]:
    """Fetch official annual/monthly averages from ERG MonitoringReport API.

    Returns dict keyed by species code, e.g.:
        {"NO2": {"annual": 35.2, "monthly": {1: 40.1, 2: 38.5, ...}}}

    Uses report_cache to avoid duplicate API calls (same site+year returns all species).
    """
    cache_key = (site_code, year)
    if cache_key in report_cache:
        return report_cache[cache_key]

    # Ensure only one coroutine fetches per (site_code, year)
    if cache_key not in _report_locks:
        _report_locks[cache_key] = asyncio.Lock()
    async with _report_locks[cache_key]:
        # Re-check after acquiring lock
        if cache_key in report_cache:
            return report_cache[cache_key]

        url = MONITORING_REPORT_URL.format(site_code=site_code, year=year)
        result: dict[str, dict] = {}

        for attempt in range(MAX_RETRIES):
            async with api_sem:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=90)) as resp:
                        if resp.status == 429:
                            retry_after = resp.headers.get("Retry-After")
                            wait = int(retry_after) if retry_after else (2 ** attempt + 1)
                            await asyncio.sleep(wait)
                            continue
                        if resp.status != 200:
                            report_cache[cache_key] = result
                            return result
                        data = await resp.json(content_type=None)
                except Exception:
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    report_cache[cache_key] = result
                    return result

            # Parse SiteReport -> ReportItem array
            site_report = data.get("SiteReport")
            if not site_report:
                # API returns {"SiteReport": null} for unknown site/year combos
                break
            report_items = site_report.get("ReportItem", [])
            if isinstance(report_items, dict):
                report_items = [report_items]

            for item in report_items:
                # Filter: ReportItem type "7" and name starts with "Mean:"
                if item.get("@ReportItem") != "7":
                    continue
                item_name = item.get("@ReportItemName", "")
                if not item_name.startswith("Mean:"):
                    continue

                species_code = item.get("@SpeciesCode", "")
                if not species_code:
                    continue

                # Extract annual value
                annual_val = item.get("@Annual", "")
                annual = None
                if annual_val and annual_val not in ("-999", ""):
                    try:
                        annual = float(annual_val)
                    except (ValueError, TypeError):
                        pass

                # Extract monthly values (@Month1 through @Month12)
                monthly: dict[int, float] = {}
                for m in range(1, 13):
                    mv = item.get(f"@Month{m}", "")
                    if mv and mv not in ("-999", ""):
                        try:
                            monthly[m] = float(mv)
                        except (ValueError, TypeError):
                            pass

                result[species_code] = {"annual": annual, "monthly": monthly}

            break  # success, exit retry loop

        report_cache[cache_key] = result
        return result


def generate_yearly_chunks(start_date: str, end_date: str) -> list[tuple[str, str]]:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    chunks = []
    cursor = start
    while cursor < end:
        chunk_end = min(cursor + timedelta(days=MAX_DAYS_PER_CHUNK), end)
        chunks.append((cursor.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def build_work_items(
    sensors: dict[str, dict], api_sites: list[dict],
    site_filter: set[str] | None = None, start_override: str | None = None,
) -> list[dict]:
    items = []
    end_date = datetime.now().strftime("%Y-%m-%d")

    for site in api_sites:
        site_code = site.get("@SiteCode", "")
        if site_filter and site_code not in site_filter:
            continue
        sensor = sensors.get(site_code)
        if not sensor:
            continue

        for sp in normalise_species(site.get("Species", [])):
            api_code = sp.get("@SpeciesCode", "")
            db_pollutant = SPECIES_MAP.get(api_code)
            if not db_pollutant:
                continue

            species_end = parse_api_date(sp.get("@DateMeasurementFinished"))
            if species_end:
                continue

            species_start = parse_api_date(sp.get("@DateMeasurementStarted")) or "2020-01-01"
            if start_override and start_override > species_start:
                species_start = start_override

            start_year = int(species_start[:4])

            items.append({
                "id_site": sensor["id_site"],
                "site_code": site_code,
                "species_code": api_code,
                "pollutant": db_pollutant,
                "start_date": species_start,
                "end_date": end_date,
                "start_year": start_year,
            })

    return items


async def process_item_async(
    session: aiohttp.ClientSession,
    api_sem: asyncio.Semaphore,
    db_sem: asyncio.Semaphore,
    item: dict, idx: int, total: int,
    dead_letter: list,
    report_cache: dict[tuple[str, int], dict],
) -> dict:
    tag = f"[{idx + 1}/{total}] {item['site_code']}/{item['pollutant']}"
    counts = {"hourly": 0, "daily": 0, "monthly": 0, "annual": 0}

    # 1. Check latest existing hourly data (incremental)
    latest_hourly = await sb_get_latest_hourly(session, db_sem, item["id_site"], item["pollutant"])

    effective_start = item["start_date"]
    if latest_hourly:
        try:
            latest_dt = datetime.strptime(latest_hourly, "%Y-%m-%d %H:%M:%S")
            incremental_start = (latest_dt + timedelta(hours=1)).strftime("%Y-%m-%d")
            if incremental_start > item["end_date"]:
                logger.info(f"{tag}: up to date (latest: {latest_hourly})")
                return counts
            effective_start = incremental_start
            logger.info(f"{tag}: incremental from {effective_start}")
        except ValueError:
            pass

    # 2. Fetch all yearly chunks concurrently
    chunks = generate_yearly_chunks(effective_start, item["end_date"])
    fetch_tasks = [
        fetch_chunk_async(session, api_sem, item["site_code"], item["species_code"], cs, ce)
        for cs, ce in chunks
    ]
    chunk_results = await asyncio.gather(*fetch_tasks)

    all_measurements = []
    for result in chunk_results:
        all_measurements.extend(result)

    if not all_measurements:
        return counts

    logger.info(f"{tag}: fetched {len(all_measurements)} hourly records ({len(chunks)} chunks)")

    # 3. Parse, deduplicate, build rows + aggregation buckets
    cutoff = datetime.strptime(latest_hourly, "%Y-%m-%d %H:%M:%S") if latest_hourly else None

    hourly_rows = []
    daily_bucket: dict[str, list[float]] = defaultdict(list)
    monthly_bucket: dict[str, list[float]] = defaultdict(list)
    annual_bucket: dict[int, list[float]] = defaultdict(list)

    for m in all_measurements:
        try:
            dt = datetime.strptime(m["date"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.fromisoformat(m["date"].replace("Z", "+00:00"))
                dt = dt.replace(tzinfo=None)
            except (ValueError, TypeError):
                continue

        if cutoff and dt <= cutoff:
            continue

        year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
        val = m["value"]

        dt_start = dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        dt_end = (dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        hourly_rows.append({
            "id_site": item["id_site"],
            "pollutant": item["pollutant"],
            "year": year, "month": month, "day": day, "hour": hour,
            "value": val,
            "datetime_start": dt_start,
            "datetime_end": dt_end,
        })

        day_key = f"{year}-{month:02d}-{day:02d}"
        month_key = f"{year}-{month:02d}"
        daily_bucket[day_key].append(val)
        monthly_bucket[month_key].append(val)
        annual_bucket[year].append(val)

    if not hourly_rows:
        logger.info(f"{tag}: no new records after filtering")
        return counts

    # 4. Insert hourly data
    counts["hourly"] = await sb_insert_rows(session, db_sem, "hourly_averages", hourly_rows, tag)

    if counts["hourly"] < len(hourly_rows):
        dead_letter.append({"item": tag, "table": "hourly_averages", "failed_rows": len(hourly_rows) - counts["hourly"]})

    # 5. Daily averages (>= 18 hourly readings)
    latest_daily = await sb_get_latest_agg(session, db_sem, "daily_averages", item["id_site"], item["pollutant"])
    daily_rows = []
    for day_key, vals in sorted(daily_bucket.items()):
        if latest_daily and day_key <= latest_daily:
            continue
        if len(vals) >= 18:
            y, mo, d = day_key.split("-")
            daily_rows.append({
                "id_site": item["id_site"], "pollutant": item["pollutant"],
                "value": round(sum(vals) / len(vals), 2),
                "year": int(y), "month": int(mo), "day": int(d),
                "date": day_key, "averaging_period": "daily",
            })
    counts["daily"] = await sb_insert_rows(session, db_sem, "daily_averages", daily_rows, tag)

    # 6. Monthly & Annual averages
    # O3 has no Mean row in MonitoringReport — fall back to computing from hourly.
    # For NO2, PM10, PM25: use official ERG MonitoringReport values.
    latest_monthly = await sb_get_latest_agg(session, db_sem, "monthly_averages", item["id_site"], item["pollutant"])
    latest_annual = await sb_get_latest_agg(session, db_sem, "annual_averages", item["id_site"], item["pollutant"])

    current_year = datetime.now().year
    start_year = item.get("start_year", current_year)

    # Try MonitoringReport first (has official means for NO2, PM10, PM25 but not O3)
    annual_from = int(latest_annual) + 1 if latest_annual else start_year
    if latest_monthly:
        monthly_from_year = int(latest_monthly[:4])
    else:
        monthly_from_year = start_year
    fetch_from = min(annual_from, monthly_from_year)

    report_tasks = [
        fetch_monitoring_report_async(session, api_sem, item["site_code"], y, report_cache)
        for y in range(fetch_from, current_year + 1)
    ]
    report_results = await asyncio.gather(*report_tasks)
    year_reports = {y: r for y, r in zip(range(fetch_from, current_year + 1), report_results)}

    # Check if MonitoringReport has Mean data for this species in any year
    has_official_mean = any(
        item["species_code"] in yr_data for yr_data in year_reports.values()
    )

    if has_official_mean:
        # Use official ERG values for annual and monthly
        annual_rows = []
        for year in sorted(year_reports.keys()):
            if latest_annual and year <= int(latest_annual):
                continue
            species_data = year_reports[year].get(item["species_code"], {})
            annual_val = species_data.get("annual")
            if annual_val is not None:
                annual_rows.append({
                    "id_site": item["id_site"], "pollutant": item["pollutant"],
                    "value": round(annual_val, 2),
                    "year": year, "date": f"{year}-01-01", "averaging_period": "annual",
                })
        counts["annual"] = await sb_insert_rows(session, db_sem, "annual_averages", annual_rows, tag)

        monthly_rows = []
        for year in sorted(year_reports.keys()):
            species_data = year_reports[year].get(item["species_code"], {})
            monthly_vals = species_data.get("monthly", {})
            for month in sorted(monthly_vals.keys()):
                month_date = f"{year}-{month:02d}-01"
                if latest_monthly and month_date <= latest_monthly:
                    continue
                monthly_rows.append({
                    "id_site": item["id_site"], "pollutant": item["pollutant"],
                    "value": round(monthly_vals[month], 2),
                    "year": year, "month": month,
                    "date": month_date, "averaging_period": "monthly",
                })
        counts["monthly"] = await sb_insert_rows(session, db_sem, "monthly_averages", monthly_rows, tag)
    else:
        # Fallback: compute from hourly data (e.g. O3 has no Mean in MonitoringReport)
        monthly_rows = []
        for month_key, vals in sorted(monthly_bucket.items()):
            month_date = f"{month_key}-01"
            if latest_monthly and month_date <= latest_monthly:
                continue
            y, mo = month_key.split("-")
            monthly_rows.append({
                "id_site": item["id_site"], "pollutant": item["pollutant"],
                "value": round(sum(vals) / len(vals), 2),
                "year": int(y), "month": int(mo),
                "date": month_date, "averaging_period": "monthly",
            })
        counts["monthly"] = await sb_insert_rows(session, db_sem, "monthly_averages", monthly_rows, tag)

        annual_rows = []
        for year, vals in sorted(annual_bucket.items()):
            if latest_annual and year <= int(latest_annual):
                continue
            is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
            expected = 8784 if is_leap else 8760
            if len(vals) >= math.floor(expected * 0.75):
                annual_rows.append({
                    "id_site": item["id_site"], "pollutant": item["pollutant"],
                    "value": round(sum(vals) / len(vals), 2),
                    "year": year, "date": f"{year}-01-01", "averaging_period": "annual",
                })
        counts["annual"] = await sb_insert_rows(session, db_sem, "annual_averages", annual_rows, tag)

    logger.info(f"{tag}: inserted h={counts['hourly']} d={counts['daily']} m={counts['monthly']} a={counts['annual']}")
    return counts


async def run_async_pipeline(items: list[dict]):
    logger.info(f"Phase 2: Async engine | API={API_CONCURRENCY} DB={DB_CONCURRENCY} tasks={len(items)}")

    api_sem = asyncio.Semaphore(API_CONCURRENCY)
    db_sem = asyncio.Semaphore(DB_CONCURRENCY)
    task_sem = asyncio.Semaphore(TASK_CONCURRENCY)
    dead_letter: list[dict] = []
    report_cache: dict[tuple[str, int], dict] = {}
    totals = {"hourly": 0, "daily": 0, "monthly": 0, "annual": 0, "errors": 0}

    connector = aiohttp.TCPConnector(limit=API_CONCURRENCY + DB_CONCURRENCY + 10, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=120, connect=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async def bounded_task(idx: int, item: dict):
            async with task_sem:
                try:
                    return await process_item_async(session, api_sem, db_sem, item, idx, len(items), dead_letter, report_cache)
                except Exception as e:
                    logger.error(f"Error [{idx + 1}/{len(items)}] {item['site_code']}/{item['pollutant']}: {e}")
                    dead_letter.append({"item": f"{item['site_code']}/{item['pollutant']}", "error": str(e)})
                    return None

        tasks = [bounded_task(i, item) for i, item in enumerate(items)]
        results = await asyncio.gather(*tasks)

    for result in results:
        if result:
            for key in ("hourly", "daily", "monthly", "annual"):
                totals[key] += result[key]
        else:
            totals["errors"] += 1

    logger.info("=" * 50)
    logger.info("Phase 2 Summary")
    logger.info(f"  Hourly records:   {totals['hourly']:,}")
    logger.info(f"  Daily averages:   {totals['daily']:,}")
    logger.info(f"  Monthly averages: {totals['monthly']:,}")
    logger.info(f"  Annual averages:  {totals['annual']:,}")
    logger.info(f"  Task errors:      {totals['errors']}")
    if dead_letter:
        logger.warning(f"  Dead letter queue: {len(dead_letter)} entries")
        for dl in dead_letter[:20]:
            logger.warning(f"    - {dl}")
    logger.info("=" * 50)


# ── Backfill Mode: Direct Postgres Engine ────────────────────────────────────

# Column lists for COPY (exclude auto-increment id)
PG_COLS = {
    "hourly_averages": ("id_site", "pollutant", "year", "month", "day", "hour", "value", "datetime_start", "datetime_end"),
    "daily_averages": ("id_site", "pollutant", "value", "year", "month", "day", "date", "averaging_period"),
    "monthly_averages": ("id_site", "pollutant", "value", "year", "month", "date", "averaging_period"),
    "annual_averages": ("id_site", "pollutant", "value", "year", "date", "averaging_period"),
}


def pg_get_latest_hourly(conn, id_site: str, pollutant: str) -> Optional[str]:
    """Find latest hourly record via direct Postgres (year-by-year scan)."""
    current_year = datetime.now().year
    for year in range(current_year, current_year - 30, -1):
        row = conn.execute(
            "SELECT year, month, day, hour FROM hourly_averages "
            "WHERE id_site = %s AND pollutant = %s AND year = %s "
            "ORDER BY month DESC, day DESC, hour DESC LIMIT 1",
            (id_site, pollutant, year),
        ).fetchone()
        if row:
            return f"{row[0]}-{row[1]:02d}-{row[2]:02d} {row[3]:02d}:00:00"
    return None


def pg_get_latest_agg(conn, table: str, id_site: str, pollutant: str) -> Optional[str]:
    col = "date" if table != "annual_averages" else "year"
    row = conn.execute(
        f"SELECT {col} FROM {table} "
        f"WHERE id_site = %s AND pollutant = %s "
        f"ORDER BY {col} DESC LIMIT 1",
        (id_site, pollutant),
    ).fetchone()
    if row:
        return str(row[0])
    return None


def pg_insert_rows(conn, table: str, rows: list[dict], tag: str) -> int:
    """Bulk insert using psycopg COPY protocol — fastest possible Postgres insert."""
    if not rows:
        return 0
    cols = PG_COLS[table]
    inserted = 0
    for i in range(0, len(rows), PG_BATCH_SIZE):
        batch = rows[i:i + PG_BATCH_SIZE]
        with conn.cursor() as cur:
            with cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as copy:
                for row in batch:
                    copy.write_row(tuple(row[c] for c in cols))
        conn.commit()
        inserted += len(batch)
    return inserted


async def process_item_backfill(
    session: aiohttp.ClientSession,
    api_sem: asyncio.Semaphore,
    pg_conn,
    item: dict, idx: int, total: int,
    dead_letter: list,
    report_cache: dict[tuple[str, int], dict],
) -> dict:
    """Like process_item_async but writes via direct Postgres instead of REST API."""
    tag = f"[{idx + 1}/{total}] {item['site_code']}/{item['pollutant']}"
    counts = {"hourly": 0, "daily": 0, "monthly": 0, "annual": 0}

    # 1. Check latest existing hourly data (incremental)
    latest_hourly = pg_get_latest_hourly(pg_conn, item["id_site"], item["pollutant"])

    effective_start = item["start_date"]
    if latest_hourly:
        try:
            latest_dt = datetime.strptime(latest_hourly, "%Y-%m-%d %H:%M:%S")
            incremental_start = (latest_dt + timedelta(hours=1)).strftime("%Y-%m-%d")
            if incremental_start > item["end_date"]:
                logger.info(f"{tag}: up to date (latest: {latest_hourly})")
                return counts
            effective_start = incremental_start
            logger.info(f"{tag}: incremental from {effective_start}")
        except ValueError:
            pass

    # 2. Fetch all yearly chunks concurrently
    chunks = generate_yearly_chunks(effective_start, item["end_date"])
    fetch_tasks = [
        fetch_chunk_async(session, api_sem, item["site_code"], item["species_code"], cs, ce)
        for cs, ce in chunks
    ]
    chunk_results = await asyncio.gather(*fetch_tasks)

    all_measurements = []
    for result in chunk_results:
        all_measurements.extend(result)

    if not all_measurements:
        return counts

    logger.info(f"{tag}: fetched {len(all_measurements)} hourly records ({len(chunks)} chunks)")

    # 3. Parse, deduplicate, build rows + aggregation buckets
    cutoff = datetime.strptime(latest_hourly, "%Y-%m-%d %H:%M:%S") if latest_hourly else None

    hourly_rows = []
    daily_bucket: dict[str, list[float]] = defaultdict(list)
    monthly_bucket: dict[str, list[float]] = defaultdict(list)
    annual_bucket: dict[int, list[float]] = defaultdict(list)

    for m in all_measurements:
        try:
            dt = datetime.strptime(m["date"], "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.fromisoformat(m["date"].replace("Z", "+00:00"))
                dt = dt.replace(tzinfo=None)
            except (ValueError, TypeError):
                continue

        if cutoff and dt <= cutoff:
            continue

        year, month, day, hour = dt.year, dt.month, dt.day, dt.hour
        val = m["value"]

        dt_start = dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
        dt_end = (dt + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

        hourly_rows.append({
            "id_site": item["id_site"],
            "pollutant": item["pollutant"],
            "year": year, "month": month, "day": day, "hour": hour,
            "value": val,
            "datetime_start": dt_start,
            "datetime_end": dt_end,
        })

        day_key = f"{year}-{month:02d}-{day:02d}"
        month_key = f"{year}-{month:02d}"
        daily_bucket[day_key].append(val)
        monthly_bucket[month_key].append(val)
        annual_bucket[year].append(val)

    if not hourly_rows:
        logger.info(f"{tag}: no new records after filtering")
        return counts

    # 4. Insert hourly data via COPY
    counts["hourly"] = pg_insert_rows(pg_conn, "hourly_averages", hourly_rows, tag)

    # 5. Daily averages (>= 18 hourly readings)
    latest_daily = pg_get_latest_agg(pg_conn, "daily_averages", item["id_site"], item["pollutant"])
    daily_rows = []
    for day_key, vals in sorted(daily_bucket.items()):
        if latest_daily and day_key <= latest_daily:
            continue
        if len(vals) >= 18:
            y, mo, d = day_key.split("-")
            daily_rows.append({
                "id_site": item["id_site"], "pollutant": item["pollutant"],
                "value": round(sum(vals) / len(vals), 2),
                "year": int(y), "month": int(mo), "day": int(d),
                "date": day_key, "averaging_period": "daily",
            })
    counts["daily"] = pg_insert_rows(pg_conn, "daily_averages", daily_rows, tag)

    # 6. Monthly & Annual averages (MonitoringReport for NO2/PM/PM25, hourly fallback for O3)
    latest_monthly = pg_get_latest_agg(pg_conn, "monthly_averages", item["id_site"], item["pollutant"])
    latest_annual = pg_get_latest_agg(pg_conn, "annual_averages", item["id_site"], item["pollutant"])

    current_year = datetime.now().year
    start_year = item.get("start_year", current_year)

    annual_from = int(latest_annual) + 1 if latest_annual else start_year
    if latest_monthly:
        monthly_from_year = int(latest_monthly[:4])
    else:
        monthly_from_year = start_year
    fetch_from = min(annual_from, monthly_from_year)

    report_tasks = [
        fetch_monitoring_report_async(session, api_sem, item["site_code"], y, report_cache)
        for y in range(fetch_from, current_year + 1)
    ]
    report_results = await asyncio.gather(*report_tasks)
    year_reports = {y: r for y, r in zip(range(fetch_from, current_year + 1), report_results)}

    has_official_mean = any(
        item["species_code"] in yr_data for yr_data in year_reports.values()
    )

    if has_official_mean:
        annual_rows = []
        for year in sorted(year_reports.keys()):
            if latest_annual and year <= int(latest_annual):
                continue
            species_data = year_reports[year].get(item["species_code"], {})
            annual_val = species_data.get("annual")
            if annual_val is not None:
                annual_rows.append({
                    "id_site": item["id_site"], "pollutant": item["pollutant"],
                    "value": round(annual_val, 2),
                    "year": year, "date": f"{year}-01-01", "averaging_period": "annual",
                })
        counts["annual"] = pg_insert_rows(pg_conn, "annual_averages", annual_rows, tag)

        monthly_rows = []
        for year in sorted(year_reports.keys()):
            species_data = year_reports[year].get(item["species_code"], {})
            monthly_vals = species_data.get("monthly", {})
            for mo in sorted(monthly_vals.keys()):
                month_date = f"{year}-{mo:02d}-01"
                if latest_monthly and month_date <= latest_monthly:
                    continue
                monthly_rows.append({
                    "id_site": item["id_site"], "pollutant": item["pollutant"],
                    "value": round(monthly_vals[mo], 2),
                    "year": year, "month": mo,
                    "date": month_date, "averaging_period": "monthly",
                })
        counts["monthly"] = pg_insert_rows(pg_conn, "monthly_averages", monthly_rows, tag)
    else:
        monthly_rows = []
        for month_key, vals in sorted(monthly_bucket.items()):
            month_date = f"{month_key}-01"
            if latest_monthly and month_date <= latest_monthly:
                continue
            y, mo = month_key.split("-")
            monthly_rows.append({
                "id_site": item["id_site"], "pollutant": item["pollutant"],
                "value": round(sum(vals) / len(vals), 2),
                "year": int(y), "month": int(mo),
                "date": month_date, "averaging_period": "monthly",
            })
        counts["monthly"] = pg_insert_rows(pg_conn, "monthly_averages", monthly_rows, tag)

        annual_rows = []
        for year, vals in sorted(annual_bucket.items()):
            if latest_annual and year <= int(latest_annual):
                continue
            is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
            expected = 8784 if is_leap else 8760
            if len(vals) >= math.floor(expected * 0.75):
                annual_rows.append({
                    "id_site": item["id_site"], "pollutant": item["pollutant"],
                    "value": round(sum(vals) / len(vals), 2),
                    "year": year, "date": f"{year}-01-01", "averaging_period": "annual",
                })
        counts["annual"] = pg_insert_rows(pg_conn, "annual_averages", annual_rows, tag)

    logger.info(f"{tag}: inserted h={counts['hourly']} d={counts['daily']} m={counts['monthly']} a={counts['annual']}")
    return counts


async def run_backfill_pipeline(items: list[dict]):
    """Fast backfill using direct Postgres COPY + async API fetches."""
    import psycopg

    logger.info(f"Backfill mode: Direct Postgres | API={API_CONCURRENCY} PG={PG_CONCURRENCY} tasks={len(items)}")

    api_sem = asyncio.Semaphore(API_CONCURRENCY)
    task_sem = asyncio.Semaphore(PG_CONCURRENCY)
    dead_letter: list[dict] = []
    report_cache: dict[tuple[str, int], dict] = {}
    totals = {"hourly": 0, "daily": 0, "monthly": 0, "annual": 0, "errors": 0}

    connector = aiohttp.TCPConnector(limit=API_CONCURRENCY + 10, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=120, connect=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async def bounded_task(idx: int, item: dict):
            async with task_sem:
                # Each task gets its own Postgres connection (thread-safe COPY)
                pg_conn = psycopg.connect(DATABASE_URL, autocommit=False)
                try:
                    result = await process_item_backfill(
                        session, api_sem, pg_conn, item, idx, len(items), dead_letter, report_cache,
                    )
                    return result
                except Exception as e:
                    logger.error(f"Error [{idx + 1}/{len(items)}] {item['site_code']}/{item['pollutant']}: {e}")
                    dead_letter.append({"item": f"{item['site_code']}/{item['pollutant']}", "error": str(e)})
                    return None
                finally:
                    pg_conn.close()

        tasks = [bounded_task(i, item) for i, item in enumerate(items)]
        results = await asyncio.gather(*tasks)

    for result in results:
        if result:
            for key in ("hourly", "daily", "monthly", "annual"):
                totals[key] += result[key]
        else:
            totals["errors"] += 1

    logger.info("=" * 50)
    logger.info("Backfill Summary (Direct Postgres)")
    logger.info(f"  Hourly records:   {totals['hourly']:,}")
    logger.info(f"  Daily averages:   {totals['daily']:,}")
    logger.info(f"  Monthly averages: {totals['monthly']:,}")
    logger.info(f"  Annual averages:  {totals['annual']:,}")
    logger.info(f"  Task errors:      {totals['errors']}")
    if dead_letter:
        logger.warning(f"  Dead letter queue: {len(dead_letter)} entries")
        for dl in dead_letter[:20]:
            logger.warning(f"    - {dl}")
    logger.info("=" * 50)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="London Air Data Pipeline")
    parser.add_argument("--sensors-only", action="store_true", help="Phase 1 only")
    parser.add_argument("--data-only", action="store_true", help="Phase 2 only")
    parser.add_argument("--backfill", action="store_true", help="Fast backfill via direct Postgres (requires DATABASE_URL)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without writing")
    parser.add_argument("--sites", type=str, help="Comma-separated site codes (e.g. RI1,WA7)")
    parser.add_argument("--start-date", type=str, help="Override start date (YYYY-MM-DD)")
    args = parser.parse_args()

    mode = "DRY RUN" if args.dry_run else ("BACKFILL" if args.backfill else "LIVE")
    logger.info("=" * 60)
    logger.info("London Air Data Pipeline (async)")
    logger.info(f"  Mode:        {mode}")
    logger.info(f"  Supabase:    {SUPABASE_URL}")
    if args.backfill:
        logger.info(f"  Concurrency: API={API_CONCURRENCY} PG={PG_CONCURRENCY} (direct Postgres)")
    else:
        logger.info(f"  Concurrency: API={API_CONCURRENCY} DB={DB_CONCURRENCY}")
    logger.info("=" * 60)

    if args.backfill and not DATABASE_URL:
        logger.error("DATABASE_URL environment variable is required for --backfill mode")
        sys.exit(1)

    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        sys.exit(1)

    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    api_sites = fetch_site_species()

    # Phase 1
    if not args.data_only:
        sensors = sync_sensors(supabase, api_sites, args.dry_run)
    else:
        sensors = {}
        for site in api_sites:
            row = api_site_to_row(site)
            if row:
                sensors[row["site_code"]] = row

    # Phase 2
    site_filter = set(args.sites.split(",")) if args.sites else None
    if site_filter:
        logger.info(f"Filtering to sites: {', '.join(sorted(site_filter))}")

    if not args.sensors_only:
        items = build_work_items(sensors, api_sites, site_filter, args.start_date)
        logger.info(f"Built {len(items)} work items (sensor x pollutant)")

        if args.dry_run:
            total_chunks = sum(len(generate_yearly_chunks(i["start_date"], i["end_date"])) for i in items)
            logger.info(f"[DRY RUN] {len(items)} items = {total_chunks} API chunks")
            for item in items[:20]:
                c = len(generate_yearly_chunks(item["start_date"], item["end_date"]))
                logger.info(f"  {item['site_code']}/{item['pollutant']}: {item['start_date']} -> {item['end_date']} ({c} chunks)")
        elif args.backfill:
            asyncio.run(run_backfill_pipeline(items))
        else:
            asyncio.run(run_async_pipeline(items))

    logger.info(f"Pipeline completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
