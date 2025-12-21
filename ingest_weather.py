import os
import requests
import time # Added this for the sleep logic
from datetime import datetime, timezone, timedelta  # Added timedelta here
from google.cloud import bigquery
from dotenv import load_dotenv
import pandas as pd

# load environment variables
load_dotenv()


# 1. CONFIGURATION
API_KEY = os.getenv("OPENWEATHER_API_KEY")
PROJECT_ID = "calm-suprstate-481612-s3"
DATASET_ID = "raw_weather_data"
TABLE_ID = "amsterdam_historical_weather"
TABLE_FULL_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
KEY_PATH = "weather_analytics_key.json"

LAT, LON = 52.3676, 4.9041

def fetch_historical_day(target_date):
    timestamp = int(target_date.timestamp())
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat={LAT}&lon={LON}&dt={timestamp}&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        day_data = response.json()['data'][0]
        
        # Safety Logic: Get rain/snow if they exist, otherwise default to 0.0
        # The API returns these as dictionaries: {"1h": 1.5}
        rain = day_data.get('rain', {}).get('1h', 0.0)
        snow = day_data.get('snow', {}).get('1h', 0.0)
        
        return {
            "date": target_date.strftime('%Y-%m-%d'),
            "timestamp": timestamp,
            "temp": day_data.get('temp'),
            "humidity": day_data.get('humidity'),
            "pressure": day_data.get('pressure'),
            "cloudiness": day_data.get('clouds'),
            "wind_speed": day_data.get('wind_speed'),
            "wind_deg": day_data.get('wind_deg'),
            "rain_1h": rain,
            "snow_1h": snow,
            "weather_main": day_data['weather'][0]['main'],
            "weather_description": day_data['weather'][0]['description'],
            "ingested_at": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        print(f"❌ Failed for {target_date.date()}: {e}")
        return None

def upload_to_bq(rows):
    client = bigquery.Client.from_service_account_json(KEY_PATH)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("timestamp", "INTEGER"),
            bigquery.SchemaField("temp", "FLOAT"),
            bigquery.SchemaField("humidity", "INTEGER"),
            bigquery.SchemaField("pressure", "INTEGER"),
            bigquery.SchemaField("cloudiness", "INTEGER"),
            bigquery.SchemaField("wind_speed", "FLOAT"),
            bigquery.SchemaField("wind_deg", "INTEGER"),
            bigquery.SchemaField("rain_1h", "FLOAT"),
            bigquery.SchemaField("snow_1h", "FLOAT"),
            bigquery.SchemaField("weather_main", "STRING"),
            bigquery.SchemaField("weather_description", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ],
        write_disposition="WRITE_APPEND",
    )
    
    load_job = client.load_table_from_json(rows, TABLE_FULL_ID, job_config=job_config)
    load_job.result()
    print(f"🚀 Successfully uploaded {len(rows)} rows with full metrics.")

def get_start_date(client):
    """Checks BigQuery for the oldest date to determine where to start backfilling."""
    # We look for the MIN (earliest) date in your table
    query = f"SELECT MIN(date) as oldest_date FROM `{TABLE_FULL_ID}`"
    try:
        query_job = client.query(query)
        result = query_job.to_dataframe()
        oldest_date = result['oldest_date'].iloc[0]
        
        if oldest_date is not None:
            # If we found a date, convert it to a datetime object and start at the day before it
            # Using .date() to ensure we are comparing date to date
            return datetime.combine(oldest_date, datetime.min.time(), tzinfo=timezone.utc) - timedelta(days=1)
    except Exception as e:
        print(f"Starting fresh or table doesn't exist yet: {e}")
    
    # If the table is empty, start from yesterday
    return datetime.now(timezone.utc) - timedelta(days=1)

if __name__ == "__main__":
    if not API_KEY:
        print("Error: Missing API Key.")
    else:
        # Initialize BigQuery client once
        client = bigquery.Client.from_service_account_json(KEY_PATH)
        
        # 1. Determine where to start (either yesterday or 1 day before your oldest record)
        target_date = get_start_date(client)
        
        # 2. Set the goal
        end_goal = datetime(1993, 1, 1, tzinfo=timezone.utc)
        
        # 3. Define how many days to fetch in THIS run (e.g., 900 days for the slow burn)
        days_to_fetch = 900 
        
        all_rows = []
        print(f"Starting backfill from {target_date.date()} going backwards...")

        for i in range(days_to_fetch):
            # Stop if we hit our 1993 target
            if target_date < end_goal:
                print(f"🎯 Reached the historical goal of {end_goal.date()}!")
                break
                
            result = fetch_historical_day(target_date)
            if result:
                all_rows.append(result)
                # Optional: Print progress every 50 days so you know it's working
                if (i + 1) % 50 == 0:
                    print(f"🔄 Progress: Fetched {i + 1} days...")
            
            # Move target date back by 1 day for the next loop iteration
            target_date -= timedelta(days=1)
            
            # API etiquette: small sleep to avoid hitting rate limits
            time.sleep(0.05)

        # 4. Upload the chunk of days to BigQuery
        if all_rows:
            upload_to_bq(all_rows)
        else:
            print("No new data to upload.")