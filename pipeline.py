import os
import pandas as pd
import requests
from datetime import datetime, timedelta
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

ANNUAL_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringReport/SiteCode={}/Year={}/json"
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
    logger.info("Loading sensor configuration from Supabase sensors table")
    
    result = supabase.table('sensors').select('*')\
        .is_('end_date', 'null')\
        .eq('sensor_type', 'Automatic')\
        .in_('borough', ['Wandsworth', 'Richmond', 'Merton'])\
        .execute()
    
    if not result.data:
        logger.warning("No active automatic sensors found")
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
                return {'date': result.data[0]['date']}
        return None
    except Exception as e:
        logger.error(f"Error querying {table} for {id_site}/{pollutant}: {e}")
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

def fetch_annual_monthly_data(site_code: str, year: int, pollutant: str) -> (Optional[float], list):
    """Fetch annual and monthly data in a single API call"""
    try:
        url = ANNUAL_API_URL.format(site_code, year)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        site_report = data.get("SiteReport", {})
        report_items = site_report.get("ReportItem", [])
        pollutant_code = "PM25" if pollutant.upper() in ["PM25", "PM2.5"] else pollutant
        
        annual_value = None
        monthly_results = []
        
        for item in report_items:
            if item.get("@SpeciesCode") == pollutant_code and item.get("@ReportItem") == "7":
                # Annual
                val = item.get("@Annual")
                if val and val != "-999":
                    try:
                        annual_value = float(val)
                    except ValueError:
                        pass
                # Monthly
                for m in range(1, 13):
                    m_val = item.get(f"@Month{m}")
                    if m_val and m_val != "-999":
                        try:
                            monthly_results.append({'year': year, 'month': m, 'value': float(m_val)})
                        except ValueError:
                            continue
                break
        return annual_value, monthly_results
    except Exception as e:
        logger.error(f"Error fetching annual/monthly data for {site_code}/{pollutant}/{year}: {e}")
        return None, []

def fetch_hourly_and_calculate_daily(site_code: str, start_date, end_date, pollutant: str) -> list:
    try:
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        url = HOURLY_API_URL.format(site_code, start_str, end_str)
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        hourly_data = []
        for item in data.get("AirQualityData", {}).get("Data", []):
            if item.get("@SpeciesCode") == pollutant:
                ts = item.get("@MeasurementDateGMT")
                val = item.get("@Value")
                if ts and val and val != "-999":
                    try:
                        hourly_data.append({'timestamp': pd.to_datetime(ts), 'value': float(val)})
                    except:
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
    except Exception as e:
        logger.error(f"Error fetching hourly data {site_code}/{pollutant} {start_date}-{end_date}: {e}")
        return []

def upload_to_supabase(supabase: Client, table: str, records: list):
    if not records:
        return
    try:
        supabase.table(table).insert(records).execute()
    except Exception as e:
        logger.error(f"Error uploading {table}: {e}")

def process_sensor_pollutant(sensor_row: pd.Series, pollutant: str, supabase: Client):
    logger.info(f"Processing {sensor_row['site_code']} - {pollutant}")
    
    missing = determine_missing_periods(sensor_row, pollutant, supabase)
    
    # Annual & Monthly
    annual_records = []
    monthly_records = []
    for year in missing['annual_years']:
        annual_val, monthly_vals = fetch_annual_monthly_data(sensor_row['site_code'], year, pollutant)
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
    upload_to_supabase(supabase, 'annual_averages', annual_records)
    upload_to_supabase(supabase, 'monthly_averages', monthly_records)
    
    # Daily averages only if monthly data exists
    monthly_result = supabase.table('monthly_averages')\
        .select('year, month')\
        .eq('id_site', sensor_row['id_site']).eq('pollutant', pollutant)\
        .order('year', asc=True).order('month', asc=True).limit(1).execute()
    
    if monthly_result.data:
        first_month = monthly_result.data[0]
        first_month_date = datetime(first_month['year'], first_month['month'], 1)
        daily_cutoff_start = (first_month_date - pd.DateOffset(months=2)).date()
        daily_dates_to_fetch = [d for d in missing['daily_dates'] if d >= daily_cutoff_start]
        
        i = 0
        while i < len(daily_dates_to_fetch):
            start_date = daily_dates_to_fetch[i]
            chunk_end = start_date + timedelta(days=90)
            end_idx = i
            while end_idx < len(daily_dates_to_fetch) and daily_dates_to_fetch[end_idx] <= chunk_end:
                end_idx += 1
            end_idx = min(end_idx, len(daily_dates_to_fetch)) - 1
            end_date = daily_dates_to_fetch[end_idx]
            daily_results = fetch_hourly_and_calculate_daily(sensor_row['site_code'], start_date, end_date, pollutant)
            records = []
            for r in daily_results:
                d = r['date']
                records.append({
                    'id_site': sensor_row['id_site'],
                    'pollutant': pollutant,
                    'value': r['value'],
                    'year': d.year,
                    'month': d.month,
                    'day': d.day,
                    'date': str(d),
                    'averaging_period': 'daily'
                })
            upload_to_supabase(supabase, 'daily_averages', records)
            i = end_idx + 1

def main():
    logger.info("Starting Air Quality Pipeline")
    supabase = get_supabase_client()
    sensors_df = load_sensors(supabase)
    tasks = []
    for _, sensor in sensors_df.iterrows():
        pollutants = sensor['pollutants_measured']
        if isinstance(pollutants, list):
            for pollutant in pollutants:
                tasks.append((sensor, pollutant.replace('.', '')))
    for sensor, pollutant in tasks:
        process_sensor_pollutant(sensor, pollutant, supabase)
    logger.info("Pipeline completed")

if __name__ == "__main__":
    main()
