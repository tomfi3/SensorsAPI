import os
import pandas as pd
import requests
from datetime import datetime, timedelta, date
from supabase import create_client, Client
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import logging
import json

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

ANNUAL_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Annual/MonitoringReport/SiteCode={}/Year={}/json"
HOURLY_API_URL = "https://api.erg.ic.ac.uk/AirQuality/Data/Site/SiteCode={}/StartDate={}/EndDate={}/Json"

def get_supabase_client() -> Client:
    """Initialize and return Supabase client"""
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    
    if not url or not key:
        database_url = os.environ.get("DATABASE_URL")
        if database_url and "supabase" in database_url:
            parts = database_url.split("@")[1].split(".")
            project_ref = parts[0]
            url = f"https://{project_ref}.supabase.co"
            logger.warning("SUPABASE_URL and SUPABASE_KEY not found. Using DATABASE_URL for connection.")
            logger.warning("For full Supabase functionality, please set SUPABASE_URL and SUPABASE_KEY")
        else:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required")
    
    return create_client(url, key)

def load_sensors(supabase: Client) -> pd.DataFrame:
    """Load sensor configuration from Supabase sensors table"""
    logger.info("Loading sensor configuration from Supabase sensors table")
    
    try:
        # Query active automatic sensors only (not diffusion tubes)
        # Only automatic sensors are compatible with the UK Air Quality API
        result = supabase.table('sensors').select('*')\
            .is_('end_date', 'null')\
            .eq('sensor_type', 'Automatic')\
            .in_('borough', ['Wandsworth', 'Richmond'])\
            .execute()
        
        if not result.data:
            logger.warning("No active automatic sensors found in Supabase sensors table")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(result.data)
        
        # The pollutants_measured column is already an array from Supabase
        # No need for ast.literal_eval
        
        # Convert date columns
        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
        
        # Filter out sensors without start_date
        df = df[df['start_date'].notna()]
        
        # Filter for valid site codes (alphabetic codes only, not pure numbers)
        df = df[df['site_code'].str.match(r'^[A-Za-z]+[0-9]*$')]
        
        logger.info(f"Loaded {len(df)} active automatic sensors from Supabase")
        return df
    
    except Exception as e:
        logger.error(f"Error loading sensors from Supabase: {e}")
        raise

def get_latest_period(supabase: Client, id_site: str, pollutant: str, table: str) -> Optional[dict]:
    """Query Supabase to find the latest complete period for a sensor-pollutant combination"""
    try:
        if table == 'annual_averages':
            result = supabase.table(table).select('year').eq('id_site', id_site).eq('pollutant', pollutant).order('year', desc=True).limit(1).execute()
            if result.data:
                return {'year': result.data[0]['year']}
        elif table == 'monthly_averages':
            result = supabase.table(table).select('year, month').eq('id_site', id_site).eq('pollutant', pollutant).order('year', desc=True).order('month', desc=True).limit(1).execute()
            if result.data:
                return {'year': result.data[0]['year'], 'month': result.data[0]['month']}
        elif table == 'daily_averages':
            result = supabase.table(table).select('date').eq('id_site', id_site).eq('pollutant', pollutant).order('date', desc=True).limit(1).execute()
            if result.data:
                return {'date': result.data[0]['date']}
        
        return None
    except Exception as e:
        logger.error(f"Error querying {table} for {id_site}/{pollutant}: {e}")
        return None

def determine_missing_periods(sensor_row: pd.Series, pollutant: str, supabase: Client) -> dict:
    """Determine which periods are missing for a sensor-pollutant combination"""
    id_site = sensor_row['id_site']
    site_code = sensor_row['site_code']
    start_date = sensor_row['start_date']
    
    current_date = datetime.now()
    
    latest_annual = get_latest_period(supabase, id_site, pollutant, 'annual_averages')
    latest_monthly = get_latest_period(supabase, id_site, pollutant, 'monthly_averages')
    latest_daily = get_latest_period(supabase, id_site, pollutant, 'daily_averages')
    
    missing = {
        'id_site': id_site,
        'site_code': site_code,
        'pollutant': pollutant,
        'annual_years': [],
        'monthly_periods': [],
        'daily_dates': []
    }
    
    start_year = start_date.year
    current_year = current_date.year
    
    last_complete_year = current_year - 1
    if current_date.month == 12 and current_date.day == 31:
        last_complete_year = current_year
    
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

def fetch_annual_data(site_code: str, year: int, pollutant: str) -> Optional[dict]:
    """Fetch annual data from UK Air Quality API"""
    try:
        url = ANNUAL_API_URL.format(site_code, year)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        site_report = data.get("SiteReport", {})
        report_items = site_report.get("ReportItem", [])
        
        pollutant_code = "PM2.5" if pollutant == "PM25" else pollutant
        
        for item in report_items:
            if (item.get("@SpeciesCode") == pollutant_code and 
                item.get("@ReportItem") == "7" and
                item.get("@ReportItemName", "").startswith("Mean:")):
                
                annual_value = item.get("@Annual")
                if annual_value and annual_value != "-999":
                    try:
                        return {'value': float(annual_value), 'year': year}
                    except ValueError:
                        continue
        
        return None
    except Exception as e:
        logger.error(f"Error fetching annual data for {site_code}/{pollutant}/{year}: {e}")
        return None

def fetch_monthly_data(site_code: str, year: int, pollutant: str) -> list:
    """Fetch monthly data from UK Air Quality API"""
    try:
        url = ANNUAL_API_URL.format(site_code, year)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        site_report = data.get("SiteReport", {})
        report_items = site_report.get("ReportItem", [])
        
        pollutant_code = "PM2.5" if pollutant == "PM25" else pollutant
        
        for item in report_items:
            if (item.get("@SpeciesCode") == pollutant_code and 
                item.get("@ReportItem") == "7" and
                item.get("@ReportItemName", "").startswith("Mean:")):
                
                monthly_results = []
                for month in range(1, 13):
                    month_value = item.get(f"@Month{month}")
                    if month_value and month_value != "-999":
                        try:
                            monthly_results.append({
                                'value': float(month_value),
                                'year': year,
                                'month': month
                            })
                        except ValueError:
                            continue
                
                return monthly_results
        
        return []
    except Exception as e:
        logger.error(f"Error fetching monthly data for {site_code}/{pollutant}/{year}: {e}")
        return []

def fetch_hourly_and_calculate_daily(site_code: str, start_date: date, end_date: date, pollutant: str) -> list:
    """Fetch hourly data for date range and calculate daily averages for days with >=75% coverage"""
    try:
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        logger.info(f"Fetching hourly data from {start_date_str} to {end_date_str}...")
        url = HOURLY_API_URL.format(site_code, start_date_str, end_date_str)
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        air_quality_data = data.get("AirQualityData", {})
        data_items = air_quality_data.get("Data", [])
        
        hourly_data = []
        for item in data_items:
            if item.get("@SpeciesCode") == pollutant:
                timestamp = item.get("@MeasurementDateGMT")
                value = item.get("@Value")
                if timestamp and value and value != "-999":
                    try:
                        hourly_data.append({
                            'timestamp': pd.to_datetime(timestamp),
                            'value': float(value)
                        })
                    except (ValueError, TypeError):
                        continue
        
        if not hourly_data:
            logger.warning(f"No hourly data found for {site_code}/{pollutant}")
            return []
        
        df = pd.DataFrame(hourly_data)
        df['date'] = df['timestamp'].dt.date
        
        daily_averages = []
        for date_val, group in df.groupby('date'):
            num_readings = len(group)
            if num_readings >= 18:
                daily_avg = group['value'].mean()
                daily_averages.append({
                    'value': daily_avg,
                    'date': date_val,
                    'num_readings': num_readings
                })
        
        logger.info(f"Calculated {len(daily_averages)} daily averages from {len(hourly_data)} hourly readings")
        return daily_averages
        
    except Exception as e:
        logger.error(f"Error fetching hourly data for {site_code}/{pollutant}/{start_date} to {end_date}: {e}")
        return []

def upload_to_supabase(supabase: Client, table: str, records: list):
    """Upload records to Supabase table"""
    if not records:
        return
    
    try:
        logger.info(f"Uploading {len(records)} records to {table}")
        supabase.table(table).insert(records).execute()
        logger.info(f"Successfully uploaded {len(records)} records to {table}")
    except Exception as e:
        logger.error(f"Error uploading to {table}: {e}")

def process_sensor_pollutant(sensor_row: pd.Series, pollutant: str, supabase: Client):
    """Process a single sensor-pollutant combination"""
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing {sensor_row['site_code']} ({sensor_row['id_site']}) - {pollutant}")
    logger.info(f"{'='*60}")
    
    if pd.isna(sensor_row['start_date']):
        logger.warning(f"Skipping {sensor_row['site_code']} - {pollutant}: No start_date defined")
        return
    
    missing = determine_missing_periods(sensor_row, pollutant, supabase)
    
    logger.info(f"Missing annual years: {missing['annual_years']}")
    logger.info(f"Missing monthly periods: {len(missing['monthly_periods'])} months")
    logger.info(f"Missing daily dates: {len(missing['daily_dates'])} days")
    
    annual_records = []
    for year in missing['annual_years']:
        logger.info(f"Fetching annual data for {year}...")
        result = fetch_annual_data(sensor_row['site_code'], year, pollutant)
        if result:
            annual_records.append({
                'id_site': sensor_row['id_site'],
                'pollutant': pollutant,
                'value': result['value'],
                'year': year,
                'date': f"{year}-01-01",
                'averaging_period': 'annual'
            })
    
    if annual_records:
        upload_to_supabase(supabase, 'annual_averages', annual_records)
    
    monthly_years = set(p['year'] for p in missing['monthly_periods'])
    monthly_records = []
    
    for year in monthly_years:
        logger.info(f"Fetching monthly data for {year}...")
        results = fetch_monthly_data(sensor_row['site_code'], year, pollutant)
        for result in results:
            if any(p['year'] == result['year'] and p['month'] == result['month'] 
                   for p in missing['monthly_periods']):
                monthly_records.append({
                    'id_site': sensor_row['id_site'],
                    'pollutant': pollutant,
                    'value': result['value'],
                    'year': result['year'],
                    'month': result['month'],
                    'date': f"{result['year']}-{result['month']:02d}-01",
                    'averaging_period': 'monthly'
                })
    
    if monthly_records:
        upload_to_supabase(supabase, 'monthly_averages', monthly_records)
    
    if missing['daily_dates']:
        # Chunk into 6-month periods to avoid API timeouts
        daily_records = []
        all_dates = missing['daily_dates']
        
        i = 0
        while i < len(all_dates):
            start_date = all_dates[i]
            # Find end date: 6 months from start or last date in list
            chunk_end = start_date + timedelta(days=180)  # ~6 months
            
            # Find the actual end index for this chunk
            end_idx = i
            while end_idx < len(all_dates) and all_dates[end_idx] <= chunk_end:
                end_idx += 1
            end_idx = min(end_idx, len(all_dates)) - 1
            end_date = all_dates[end_idx]
            
            logger.info(f"Fetching chunk: {start_date} to {end_date} ({end_idx - i + 1} days)")
            
            daily_results = fetch_hourly_and_calculate_daily(
                sensor_row['site_code'], 
                start_date, 
                end_date, 
                pollutant
            )
            
            for result in daily_results:
                result_date = result['date']
                daily_records.append({
                    'id_site': sensor_row['id_site'],
                    'pollutant': pollutant,
                    'value': result['value'],
                    'year': result_date.year,
                    'month': result_date.month,
                    'day': result_date.day,
                    'date': str(result_date),
                    'averaging_period': 'daily'
                })
            
            i = end_idx + 1
        
        if daily_records:
            upload_to_supabase(supabase, 'daily_averages', daily_records)
    
    logger.info(f"Completed processing {sensor_row['site_code']} - {pollutant}")

def main():
    """Main pipeline execution"""
    logger.info("Starting Air Quality Data Pipeline")
    
    try:
        supabase = get_supabase_client()
        logger.info("Successfully connected to Supabase")
    except Exception as e:
        logger.error(f"Failed to connect to Supabase: {e}")
        return
    
    sensors_df = load_sensors(supabase)
    
    tasks = []
    for _, sensor in sensors_df.iterrows():
        pollutants = sensor['pollutants_measured']
        if isinstance(pollutants, list):
            for pollutant in pollutants:
                pollutant = pollutant.replace('.', '')
                tasks.append((sensor, pollutant))
    
    logger.info(f"Total tasks to process: {len(tasks)}")
    
    for sensor, pollutant in tasks:
        process_sensor_pollutant(sensor, pollutant, supabase)
    
    logger.info("Pipeline execution completed")

if __name__ == "__main__":
    main()
