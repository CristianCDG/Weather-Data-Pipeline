{{ config(
    materialized='table',
    schema='silver',
) }}

WITH cloud_coverage AS (
    SELECT
        to_timestamp(dt) AS forecast_date,
        (clouds::json->>'all')::INTEGER AS cloud_coverage_percentage,
        ingestion_timestamp
    FROM {{ source('public_bronze', 'bronze_weather') }}
)

SELECT * FROM cloud_coverage