import os
import pandas as pd
import requests
from datetime import datetime, timedelta, date
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import Optional
import logging

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API URLs
ANNUAL_MONTHLY_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringReport/SiteCode={}/Year={}/json"
HOURLY_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Data/Site/SiteCode={}/StartDate={}/EndDate={}/Json"

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

def load_sensors(supabase: Client) -> pd.DataFrame:
    logger.info("Loading sensors from Supabase")
    result = supabase.table('sensors').select('*')\
        .is_('end_date', 'null')\
        .eq('sensor_type', 'Automatic')\
        .in_('borough', ['Wandsworth', 'Richmond', 'Merton'])\
        .execute()
    if not result.data:
        logger.warning("No active sensors found")
        return pd.DataFrame()
    
    df = pd.DataFrame(result.data)
    df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
    df = df[df['start_date'].notna()]
    df = df[df['site_code'].str.match(r'^[A-Za-z]+[0-9]*$')]
    return df

def get_latest_period(supabase: Client, id_site: str, pollutant: str, table: str) -> Optional[dict]:
    try:
        if table == 'annual_averages':
            result = supabase.table(table).select('year')\
                .eq('id_site', id_site).eq('pollutant', pollutant)\
                .order('year', desc=True).limit(1).execute()
            if result.data:
                return {'year': result.data[0]['year']}
        elif table == 'monthly_averages':
            result = supabase.table(table).select('year, month')\
                .eq('id_site', id_site).eq('pollutant', pollutant)\
                .order('year', desc=True).order('month', desc=True).limit(1).execute()
            if result.data:
                return {'year': result.data[0]['year'], 'month': result.data[0]['month']}
        elif table == 'daily_averages':
            result = supabase.table(table).select('date')\
                .eq('id_site', id_site).eq('pollutant', pollutant)\
                .order('date', desc=True).limit(1).execute()
            if result.data:
                return {'date': pd.to_datetime(result.data[0]['date']).date()}
        return None
    except Exception as e:
        logger.error(f"Error querying {table} for {id_site}/{pollutant}: {e}")
        return None

def determine_missing_periods(sensor_row: pd.Series, pollutant: str, supabase: Client) -> dict:
    id_site = sensor_row['id_site']
    site_code = sensor_row['site_code']
    start_date = sensor_row['start_date']
    today = datetime.now()
    
    latest_annual = get_latest_period(supabase, id_site, pollutant, 'annual_averages')
    latest_monthly = get_latest_period(supabase, id_site, pollutant, 'monthly_averages')
    latest_daily = get_latest_period(supabase, id_site, pollutant, 'daily_averages')
    
    missing = {
        'annual_years': [],
        'monthly_periods': [],
        'daily_dates': []
    }
    
    start_year = start_date.year
    current_year = today.year
    last_complete_year = current_year - 1
    
    annual_start = latest_annual['year'] + 1 if latest_annual else start_year
    missing['annual_years'] = list(range(annual_start, last_complete_year + 1))
    
    monthly_start = datetime(latest_monthly['year'], latest_monthly['month'], 1) + timedelta(days=32) if latest_monthly else start_date.replace(day=1)
    last_complete_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    month_cursor = monthly_start
    while month_cursor <= last_complete_month:
        missing['monthly_periods'].append({'year': month_cursor.year, 'month': month_cursor.month})
        month_cursor = (month_cursor + timedelta(days=32)).replace(day=1)
    
    daily_start = latest_daily['date'] + timedelta(days=1) if latest_daily else start_date.date()
    yesterday = (today - timedelta(days=1)).date()
    day_cursor = daily_start
    while day_cursor <= yesterday:
        missing['daily_dates'].append(day_cursor)
        day_cursor += timedelta(days=1)
    
    return missing

def fetch_annual_and_monthly(site_code: str, year: int, pollutant: str):
    """Fetch both annual and monthly data in one API call"""
    api_pollutant = "PM25" if pollutant.upper() == "PM2.5" else pollutant
    try:
        url = ANNUAL_MONTHLY_API_URL.format(site_code, year)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        site_report = data.get("SiteReport", {})
        report_items = site_report.get("ReportItem", [])
        for item in report_items:
            if item.get("@SpeciesCode") == api_pollutant and item.get("@ReportItem") == "7":
                # Annual
                annual_value = item.get("@Annual")
                annual_result = float(annual_value) if annual_value and annual_value != "-999" else None
                # Monthly
                monthly_results = []
                for month in range(1, 13):
                    month_value = item.get(f"@Month{month}")
                    if month_value and month_value != "-999":
                        monthly_results.append({'year': year, 'month': month, 'value': float(month_value)})
                return annual_result, monthly_results
        return None, []
    except Exception as e:
        logger.error(f"Error fetching annual/monthly data for {site_code}/{pollutant}/{year}: {e}")
        return None, []

def fetch_hourly_and_calculate_daily(site_code: str, start_date: date, end_date: date, pollutant: str):
    api_pollutant = "PM25" if pollutant.upper() == "PM2.5" else pollutant
    try:
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        url = HOURLY_API_URL.format(site_code, start_str, end_str)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        records = []
        for item in data.get("AirQualityData", {}).get("Data", []):
            if item.get("@SpeciesCode") != api_pollutant:
                continue
            ts = item.get("@MeasurementDateGMT")
            val = item.get("@Value")
            if ts and val and val != "-999":
                records.append({'timestamp': pd.to_datetime(ts), 'value': float(val)})
        if not records:
            return []
        df = pd.DataFrame(records)
        df['date'] = df['timestamp'].dt.date
        daily_averages = []
        for date_val, group in df.groupby('date'):
            if len(group) >= 18:  # >=75% coverage
                daily_averages.append({'date': date_val, 'value': group['value'].mean()})
        return daily_averages
    except Exception as e:
        logger.error(f"Error fetching daily data for {site_code}/{pollutant} {start_date} to {end_date}: {e}")
        return []

def upload_to_supabase(supabase: Client, table: str, records: list):
    if not records:
        return
    try:
        supabase.table(table).insert(records).execute()
    except Exception as e:
        logger.error(f"Error uploading to {table}: {e}")

def process_sensor_pollutant(sensor_row: pd.Series, pollutant: str, supabase: Client):
    logger.info(f"Processing {sensor_row['site_code']} - {pollutant}")
    if pd.isna(sensor_row['start_date']):
        return
    missing = determine_missing_periods(sensor_row, pollutant, supabase)
    
    # Annual + Monthly
    for year in missing['annual_years'] + list(set(p['year'] for p in missing['monthly_periods'])):
        annual_val, monthly_vals = fetch_annual_and_monthly(sensor_row['site_code'], year, pollutant)
        if annual_val is not None and year in missing['annual_years']:
            upload_to_supabase(supabase, 'annual_averages', [{
                'id_site': sensor_row['id_site'],
                'pollutant': pollutant,
                'value': annual_val,
                'year': year,
                'date': f"{year}-01-01",
                'averaging_period': 'annual'
            }])
        for m in monthly_vals:
            if any(mp['year'] == m['year'] and mp['month'] == m['month'] for mp in missing['monthly_periods']):
                upload_to_supabase(supabase, 'monthly_averages', [{
                    'id_site': sensor_row['id_site'],
                    'pollutant': pollutant,
                    'value': m['value'],
                    'year': m['year'],
                    'month': m['month'],
                    'date': f"{m['year']}-{m['month']:02d}-01",
                    'averaging_period': 'monthly'
                }])
    
    # Daily
    all_dates = missing['daily_dates']
    i = 0
    while i < len(all_dates):
        start_date = all_dates[i]
        chunk_end = start_date + timedelta(days=90)
        end_idx = i
        while end_idx < len(all_dates) and all_dates[end_idx] <= chunk_end:
            end_idx += 1
        end_idx = min(end_idx, len(all_dates)) - 1
        end_date = all_dates[end_idx]
        daily_results = fetch_hourly_and_calculate_daily(sensor_row['site_code'], start_date, end_date, pollutant)
        daily_records = []
        for r in daily_results:
            d = r['date']
            daily_records.append({
                'id_site': sensor_row['id_site'],
                'pollutant': pollutant,
                'value': r['value'],
                'year': d.year,
                'month': d.month,
                'day': d.day,
                'date': str(d),
                'averaging_period': 'daily'
            })
        if daily_records:
            upload_to_supabase(supabase, 'daily_averages', daily_records)
        i = end_idx + 1

def main():
    logger.info("Starting Air Quality Pipeline")
    supabase = get_supabase_client()
    sensors = load_sensors(supabase)
    
    tasks = []
    for _, sensor in sensors.iterrows():
        pollutants = sensor['pollutants_measured']
        if isinstance(pollutants, list):
            for p in pollutants:
                p = p.replace('.', '')
                tasks.append((sensor, p))
    
    logger.info(f"Total tasks: {len(tasks)}")
    for sensor, pollutant in tasks:
        process_sensor_pollutant(sensor, pollutant, supabase)
    logger.info("Pipeline completed")

if __name__ == "__main__":
    main()
