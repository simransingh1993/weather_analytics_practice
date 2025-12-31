SELECT
    FORMAT_DATE('%Y-%m', weather_date) AS year_month
    , ROUND(AVG(temp_celsius), 2) AS avg_temperature_c
    , ROUND(AVG(humidity), 2) AS avg_humidity
    , ROUND(AVG(pressure), 2) AS avg_pressure
    , ROUND(AVG(cloudiness), 2) AS avg_cloudiness
    , ROUND(AVG(wind_speed), 2) AS avg_windspeed
    , ROUND(AVG(rain_mm), 2) AS avg_rain_mm
    , SUM(rain_mm) AS total_rain_mm
    , ROUND(AVG(snow_mm), 2) AS avg_snow_mm
    , SUM(snow_mm) AS total_snow_mm 


FROM {{ ref('int_winddirection') }}
GROUP BY 1
ORDER BY year_month DESC