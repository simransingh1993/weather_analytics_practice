SELECT
  EXTRACT(YEAR FROM weather_date) AS year,
  COUNTIF(temp_celsius > 20) AS hot_days,
  COUNTIF(temp_celsius < 0) AS cold_days,

FROM {{ ref('int_winddirection_season') }}
GROUP BY 1
ORDER BY 1
