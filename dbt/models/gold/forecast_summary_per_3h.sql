{{ config(
    materialized='table',
    schema='gold',
) }}

WITH forecast_summary AS (
    SELECT
        m.forecast_date,
        m.visibility_meters,
        m.precipitation_probability,
        m.part_of_day,
        
        a.temperature,
        a.feels_like_temperature,
        a.min_temperature,
        a.max_temperature,
        a.humidity,
        a.pressure,
        a.sea_level_pressure,
        a.ground_level_pressure,
        
        w.condition_group,
        w.condition_description,
        w.condition_icon,
        
        c.cloud_coverage_percentage,
        
        wi.wind_speed,
        wi.wind_direction,
        wi.wind_gust,
        
        r.rainfall_volume_3h,
        
        m.ingestion_timestamp

    FROM {{ ref('weather_metadata') }} m
    LEFT JOIN {{ ref('atmospheric_conditions') }} a 
        ON m.forecast_date = a.forecast_date
    LEFT JOIN {{ ref('weather_conditions') }} w 
        ON m.forecast_date = w.forecast_date
    LEFT JOIN {{ ref('cloud_coverage') }} c 
        ON m.forecast_date = c.forecast_date
    LEFT JOIN {{ ref('wind_conditions') }} wi 
        ON m.forecast_date = wi.forecast_date
    LEFT JOIN {{ ref('rainfall_volume') }} r 
        ON m.forecast_date = r.forecast_date
)

SELECT * FROM forecast_summary