SELECT
  FORMAT_DATE('%Y-%m', weather_date) AS year_month
  , weather_condition
  , COUNT(*) as occurrence_count
FROM {{ ref('int_winddirection') }}
GROUP BY 1, 2
ORDER BY year_month DESC, occurrence_count DESC