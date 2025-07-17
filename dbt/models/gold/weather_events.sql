{{ config(
    materialized = 'table',
    schema = 'gold'
) }}

WITH joined_data AS (
    SELECT
        m.forecast_date::date AS forecast_date,
        r.rainfall_volume_3h,
        wi.wind_speed,
        a.temperature
    FROM {{ ref('weather_metadata') }} m
    LEFT JOIN {{ ref('rainfall_volume') }} r 
        ON m.forecast_date = r.forecast_date
    LEFT JOIN {{ ref('wind_conditions') }} wi 
        ON m.forecast_date = wi.forecast_date
    LEFT JOIN {{ ref('atmospheric_conditions') }} a 
        ON m.forecast_date = a.forecast_date
),

classified_events AS (
    SELECT
        forecast_date,
        CASE 
            WHEN rainfall_volume_3h >= 10 THEN 'Heavy Rain'
            WHEN rainfall_volume_3h BETWEEN 5 AND 9.99 THEN 'Moderate Rain'
            ELSE NULL
        END AS rain_event,
        CASE 
            WHEN wind_speed >= 10 THEN 'Strong Winds'
            WHEN wind_speed BETWEEN 5 AND 9.99 THEN 'Moderate Winds'
            ELSE NULL
        END AS wind_event,
        CASE 
            WHEN temperature >= 310 THEN 'Extreme Heat'
            WHEN temperature <= 273 THEN 'Freezing Temps'
            ELSE NULL
        END AS temperature_event
    FROM joined_data
),

union_events AS (
    SELECT forecast_date, rain_event AS event_type
    FROM classified_events
    WHERE rain_event IS NOT NULL

    UNION ALL

    SELECT forecast_date, wind_event
    FROM classified_events
    WHERE wind_event IS NOT NULL

    UNION ALL

    SELECT forecast_date, temperature_event
    FROM classified_events
    WHERE temperature_event IS NOT NULL
)

SELECT
    forecast_date,
    event_type,
    COUNT(*) AS event_count
FROM union_events
GROUP BY forecast_date, event_type
ORDER BY forecast_date, event_type
