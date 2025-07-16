{{ config(
    materialized='table',
    schema='silver',
) }}

WITH rainfall_volume AS (
    SELECT
        to_timestamp(dt) AS forecast_date,

        CASE
            WHEN rain = 'NaN' OR rain IS NULL THEN 0.0
            WHEN rain::json->>'3h' IS NULL THEN 0.0
            ELSE (rain::json->>'3h')::FLOAT
        END AS rainfall_volume_3h,

        ingestion_timestamp
    FROM {{ source('public_bronze', 'bronze_weather') }}
)

SELECT * FROM rainfall_volume