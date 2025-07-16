{{ config(
    materialized='table',
    schema='silver',
) }}

WITH weather_conditions AS (
    SELECT
        (json_array_elements(weather::json)->>'id')::INTEGER AS weather_condition_id,
        json_array_elements(weather::json)->>'main' AS condition_group,
        INITCAP(json_array_elements(weather::json)->>'description') AS condition_description,
        json_array_elements(weather::json)->>'icon' AS condition_icon,
        to_timestamp(dt) AS forecast_date,
        ingestion_timestamp
    FROM {{ source('public_bronze', 'bronze_weather') }}
)

SELECT * FROM weather_conditions