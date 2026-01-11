# Amsterdam Weather Analytics

This project is a data pipeline and analytics solution for processing and analyzing historical and live weather data for Amsterdam. It leverages the OpenWeather API to ingest weather data, stores it in Google BigQuery, and uses dbt (Data Build Tool) to transform and model the data for analysis.

---

## Features

- **Data Ingestion**: Python scripts (`ingest_weather.py` and `ingest_weather_2.py`) fetch historical and live weather data from the OpenWeather API and load it into BigQuery.
- **Data Transformation**: dbt models transform raw weather data into structured datasets for analysis, including staging, intermediate, and mart layers.
- **Analytics**: Prebuilt SQL models calculate monthly and seasonal weather statistics, rolling averages, and wind direction counts.
- **Automation**: GitHub Actions workflows automate daily data ingestion and backfilling of historical data.

---

## Project Structure


---

## Data Pipeline

1. **Ingestion**:
   - `ingest_weather.py`: Backfills historical weather data starting from 1993.
   - `ingest_weather_2.py`: Fetches daily weather data for the previous day.
   - Data is loaded into the `raw_weather_data.amsterdam_historical_weather_2` table in BigQuery.

2. **Transformation**:
   - dbt models transform raw data into structured datasets:
     - **Staging**: Cleans and standardizes raw data.
     - **Intermediate**: Adds derived fields like wind direction categories and seasons.
     - **Mart**: Aggregates data for analytics, including monthly and seasonal statistics.

3. **Automation**:
   - GitHub Actions workflows:
     - `daily_backfill.yml`: Automates historical data backfill.
     - `daily_backfill2.yml`: Automates daily data ingestion.

---

## Key dbt Models

- **Staging**:
  - `stg_weather_data.sql`: Cleans raw weather data and ensures data quality.

- **Intermediate**:
  - `int_winddirection_season.sql`: Categorizes wind direction and assigns seasons to weather data.

- **Mart**:
  - `monthly_stats.sql`: Calculates monthly weather statistics.
  - `seasonal_stats.sql`: Calculates seasonal weather statistics.
  - `rolling_avg.sql`: Computes rolling averages for weather metrics.
  - `winddirectioncount_permonth.sql`: Counts occurrences of wind directions per month.
  - `weathercondition_permonth.sql`: Counts occurrences of weather conditions per month.

---

Analytics Outputs
- Monthly Statistics: Average temperature, humidity, pressure, and precipitation per month.
- Seasonal Statistics: Aggregated weather metrics by season.
- Rolling Averages: 7-day and 30-day rolling averages for weather metrics.
- Wind Direction Analysis: Monthly counts of wind direction categories.
- Weather Condition Analysis: Monthly counts of weather conditions.

Automation
- Historical Backfill:
Scheduled daily at midnight UTC via daily_backfill.yml.
- Daily Ingestion:
Scheduled daily at 06:00 UTC via daily_backfill2.yml.

Some visualizations can be found here:
https://lookerstudio.google.com/reporting/8486db1d-7d16-4a5e-900b-5bf5dae7f162

License
This project is for practice purposes and is not licensed for production use. ```