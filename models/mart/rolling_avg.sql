/* this file will be useful only after all data since 1993 has been loaded into bigquery and 
the API call will be adjusted to retrieve live data from openweather */

SELECT
    weather_date
    , ROUND(AVG(temp_celsius) OVER (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rolling_7day_avg_temp
    , ROUND(AVG(temp_celsius) OVER (ORDER BY weather_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS rolling_30day_avg_temp
    , AVG(humidity) OVER (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg_humidity
    , AVG(humidity) OVER (ORDER BY weather_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_avg_humidity
    , ROUND(AVG(pressure) OVER (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rolling_7day_avg_pressure
    , ROUND(AVG(pressure) OVER (ORDER BY weather_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS rolling_30day_avg_pressure
    , ROUND(AVG(cloudiness) OVER (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rolling_7day_avg_cloudiness
    , ROUND(AVG(cloudiness) OVER (ORDER BY weather_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS rolling_30day_avg_cloudiness
    , ROUND(AVG(wind_speed) OVER (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rolling_7day_avg_windspeed
    , ROUND(AVG(wind_speed) OVER (ORDER BY weather_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS rolling_30day_avg_windspeed
    , ROUND(AVG(rain_mm) OVER (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS rolling_7day_avg_rain
    , ROUND(AVG(rain_mm) OVER (ORDER BY weather_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2) AS rolling_30day_avg_rain
    , AVG(snow_mm) OVER (ORDER BY weather_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7day_avg_snow
    , AVG(snow_mm) OVER (ORDER BY weather_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_30day_avg_snow




FROM {{ ref('int_winddirection') }}
