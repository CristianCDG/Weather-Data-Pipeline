{{ config(
    materialized='table',
    schema='gold',
) }}

WITH joined_data AS (
    SELECT
        m.forecast_date::date AS forecast_date,
        ROUND(a.temperature::numeric, 2) AS temperature,
        a.humidity,
        r.rainfall_volume_3h,
        w.condition_description,
        wi.wind_speed
    FROM {{ ref('weather_metadata') }} m
    LEFT JOIN {{ ref('atmospheric_conditions') }} a 
        ON m.forecast_date = a.forecast_date
    LEFT JOIN {{ ref('weather_conditions') }} w 
        ON m.forecast_date = w.forecast_date
    LEFT JOIN {{ ref('wind_conditions') }} wi 
        ON m.forecast_date = wi.forecast_date
    LEFT JOIN {{ ref('rainfall_volume') }} r 
        ON m.forecast_date = r.forecast_date
),

weather_mode AS (
    SELECT forecast_date, condition_description
    FROM (
        SELECT
            forecast_date,
            condition_description,
            COUNT(*) AS freq,
            ROW_NUMBER() OVER (PARTITION BY forecast_date ORDER BY COUNT(*) DESC) AS rn
        FROM joined_data
        GROUP BY forecast_date, condition_description
    ) ranked
    WHERE rn = 1
)

SELECT
    jd.forecast_date,
    ROUND(MIN(jd.temperature::numeric), 2) AS min_temp_k,
    ROUND(MAX(jd.temperature::numeric), 2) AS max_temp_k,
    ROUND(AVG(jd.temperature::numeric), 2) AS avg_temp_k,
    ROUND(SUM(jd.rainfall_volume_3h::numeric), 2) AS total_rain_mm,
    ROUND(AVG(jd.humidity::numeric), 2) AS avg_humidity_percent,
    MAX(jd.wind_speed) AS max_wind_speed_mps,
    wm.condition_description AS most_frequent_condition
FROM joined_data jd
LEFT JOIN weather_mode wm
    ON jd.forecast_date = wm.forecast_date
GROUP BY jd.forecast_date, wm.condition_description
ORDER BY jd.forecast_date
