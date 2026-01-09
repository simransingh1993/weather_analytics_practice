import os
import requests
import time
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# CONFIGURATION
API_KEY = os.getenv("OPENWEATHER_API_KEY")
PROJECT_ID = "calm-suprstate-481612-s3"
DATASET_ID = "raw_weather_data"
TABLE_ID = "amsterdam_historical_weather_2"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
KEY_PATH = "weather_analytics_key.json"
LAT, LON = 52.3676, 4.9041

def fetch_historical_day(target_date):
    timestamp = int(target_date.timestamp())
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={LAT}&lon={LON}&dt={timestamp}&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data_list = response.json().get('data', [])
        if not data_list:
            return None
        
        day_data = data_list[0]
        return {
            "date": target_date.strftime('%Y-%m-%d'),
            "timestamp": timestamp,
            "temp": day_data.get('temp'),
            "humidity": day_data.get('humidity'),
            "pressure": day_data.get('pressure'),
            "cloudiness": day_data.get('clouds'),
            "wind_speed": day_data.get('wind_speed'),
            "wind_deg": day_data.get('wind_deg'),
            "rain_1h": day_data.get('rain', {}).get('1h', 0.0),
            "snow_1h": day_data.get('snow', {}).get('1h', 0.0),
            "weather_main": day_data['weather'][0]['main'],
            "weather_description": day_data['weather'][0]['description'],
            "ingested_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        print(f"❌ Failed for {target_date.date()}: {e}")
        return None

def get_next_missing_date(client):
    """Finds the most recent date in BQ and returns the following day."""
    query = f"SELECT MAX(date) as newest_date FROM `{TABLE_FULL_ID}`"
    try:
        query_job = client.query(query)
        result = query_job.to_dataframe()
        newest_date = result['newest_date'].iloc[0]
        
        if newest_date is not None:
            # Start 1 day AFTER the newest record we have
            return datetime.combine(newest_date, datetime.min.time(), tzinfo=timezone.utc) + timedelta(days=1)
    except Exception as e:
        print(f"Table empty or error: {e}")
    
    # Default if table is empty: start from 1993 or a safe default
    return datetime(1993, 1, 1, tzinfo=timezone.utc)

def upload_to_bq(rows):
    client = bigquery.Client.from_service_account_json(KEY_PATH)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    try:
        load_job = client.load_table_from_json(rows, TABLE_FULL_ID, job_config=job_config)
        load_job.result()
        print(f"🚀 SUCCESS: Loaded {load_job.output_rows} rows.")
    except Exception as e:
        print(f"❌ BIGQUERY UPLOAD FAILED: {e}")

if __name__ == "__main__":
    if not API_KEY:
        print("Error: Missing API Key.")
        exit(1)

    client = bigquery.Client.from_service_account_json(KEY_PATH)
    
    # 1. Determine start date (The day after our last entry)
    current_target = get_next_missing_date(client)
    
    # 2. Determine end date (Yesterday, because today's historical data might not be complete)
    yesterday = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    
    all_rows = []
    print(f"Checking for missing data from {current_target.date()} to {yesterday.date()}...")

    while current_target <= yesterday:
        print(f"Fetching: {current_target.date()}")
        result = fetch_historical_day(current_target)
        if result:
            all_rows.append(result)
        
        current_target += timedelta(days=1)
        time.sleep(0.1) # Respect API limits

    if all_rows:
        upload_to_bq(all_rows)
    else:
        print("✅ Data is already up to date.")