/*WITH staging AS (
    SELECT * FROM {{ ref('stg_weather_data') }}
)*/

SELECT
    *
    , CASE 
    WHEN wind_direction_in_degrees BETWEEN 337.5 AND 360 OR wind_direction_in_degrees BETWEEN 0 AND 22.5 THEN 'north'
    WHEN wind_direction_in_degrees BETWEEN 22.5 AND 67.5 THEN 'northeast'
    WHEN wind_direction_in_degrees BETWEEN 67.5 AND 112.5 THEN 'east'
    WHEN wind_direction_in_degrees BETWEEN 112.5 AND 157.5 THEN 'southeast'
    WHEN wind_direction_in_degrees BETWEEN 157.5 AND 202.5 THEN 'south'
    WHEN wind_direction_in_degrees BETWEEN 202.5 AND 247.5 THEN 'southwest'
    WHEN wind_direction_in_degrees BETWEEN 247.5 AND 292.5 THEN 'west'
    WHEN wind_direction_in_degrees BETWEEN 292.5 AND 337.5 THEN 'northwest'
    ELSE 'unknown'
    END AS wind_direction_category

FROM {{ ref('stg_weather_data') }}

