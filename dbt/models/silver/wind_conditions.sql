{{ config(
    materialized='table',
    schema='silver',
) }}

WITH wind_conditions AS (
    SELECT
        to_timestamp(dt) AS forecast_date,
        (wind::json->>'speed')::FLOAT AS wind_speed,
        (wind::json->>'deg')::INTEGER AS wind_direction,
        (wind::json->>'gust')::FLOAT AS wind_gust,
        ingestion_timestamp
    FROM {{ source('public_bronze', 'bronze_weather') }}
)

SELECT * FROM wind_conditions