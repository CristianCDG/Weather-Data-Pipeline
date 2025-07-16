{{ config(
    materialized='table',
    schema='silver',
) }}

WITH atmospheric_conditions AS (
    SELECT 
        to_timestamp(dt) AS forecast_date,
        ingestion_timestamp,
        (main::json->>'temp')::FLOAT AS temperature,
        (main::json->>'feels_like')::FLOAT AS feels_like_temperature,
        (main::json->>'temp_min')::FLOAT AS min_temperature,
        (main::json->>'temp_max')::FLOAT AS max_temperature,
        (main::json->>'pressure')::INTEGER AS pressure,
        (main::json->>'sea_level')::INTEGER AS sea_level_pressure,
        (main::json->>'grnd_level')::INTEGER AS ground_level_pressure,
        (main::json->>'humidity')::INTEGER AS humidity,
        (main::json->>'temp_kf')::FLOAT AS termical_correction_factor
    FROM {{ source('public_bronze', 'bronze_weather') }}
)

SELECT * FROM atmospheric_conditions