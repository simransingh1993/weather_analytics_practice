SELECT
  FORMAT_DATE('%Y-%m', weather_date) AS year_month
  , wind_direction_category
  , COUNT(*) as occurrence_count
FROM {{ ref('int_winddirection_season') }}
GROUP BY 1, 2
ORDER BY year_month DESC