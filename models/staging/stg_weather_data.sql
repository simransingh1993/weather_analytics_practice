WITH raw_data AS (
    SELECT * FROM {{ source('raw_weather_data', 'amsterdam_historical_weather_2') }}
),

final AS (
    SELECT
        CAST(date AS DATE) as weather_date,
        temp AS temp_celsius,
        humidity,
        pressure,
        cloudiness,
        wind_speed,
        wind_deg AS wind_direction_in_degrees,
        rain_1h AS rain_mm,
        snow_1h AS snow_mm,
        weather_main AS weather_condition,
        weather_description,
        -- This logic ensures we only keep one record per day if the script accidentally ran twice
        ROW_NUMBER() OVER (PARTITION BY date ORDER BY ingested_at DESC) as dns_rank
    FROM raw_data
)

SELECT * FROM final WHERE dns_rank = 1