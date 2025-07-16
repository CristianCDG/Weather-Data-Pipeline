{{ config(
    materialized='table',
    schema='silver',
) }}

WITH metadata_cast AS (
    SELECT
        to_timestamp(dt) AS forecast_date,
        pop AS precipitation_probability,

        CASE
            WHEN sys::json->>'pod' = 'd' THEN 'Day'
            WHEN sys::json->>'pod' = 'n' THEN 'Night'
            ELSE sys::json->>'pod'
        END as part_of_day,

        visibility AS visibility_meters,
        ingestion_timestamp

    FROM {{ source('public_bronze', 'bronze_weather') }}
)

SELECT * FROM metadata_cast