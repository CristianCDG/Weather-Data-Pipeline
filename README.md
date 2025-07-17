<h2 align="center">Data Pipeline for OpenweatherAPI</h2>

###

<div align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" height="40" alt="python logo"  />
  <img width="12" />
  <img src="https://cdn.simpleicons.org/docker/2496ED" height="40" alt="docker logo"  />
  <img width="12" />
  <img src="https://cdn.simpleicons.org/postgresql/4169E1" height="40" alt="postgresql logo"  />
</div>

###

<h3 align="center">Bronze Layer: Weather Data Ingestion and Storage</h3>

###

<p align="left">This module implements the Bronze Layer of the Weather Data Pipeline using Apache Airflow.<br>It consists of a DAG that extracts weather forecast data from the OpenWeatherMap API and stores the raw results in a PostgreSQL database.</p>

###

<h3 align="left">Key features</h3>

###

<p align="left">
- Loads configuration from environment variables for API and database connection.<br>
- Extracts weather data directly from the API and loads it into the bronze table in PostgreSQL.<br>
- Transforms nested fields to JSON strings for SQL compatibility.<br>
- Loads the processed data into the bronze_weather table within the bronze schema.<br>
- Includes error handling and logging for each step.<br>
- Automatically triggers the silver layer DAG after successful ingestion.
</p>

###

<h3 align="center">Silver Layer: Weather Data Normalization</h3>

###

<p align="left">The silver layer consists of dbt models that normalize and clean the raw weather data ingested in the bronze layer.<br>Each model extracts relevant fields from nested JSON structures, casts them to appropriate SQL types, and handles missing or invalid values.</p>

###

<h3 align="left">Key models</h3>

###

<p align="left">- atmospheric_conditions: Extracts and normalizes temperature, pressure, humidity, and related metrics.<br>- weather_conditions: Flattens weather array details such as condition ID, group, description, and icon.<br>- cloud_coverage: Normalizes cloud coverage percentage for each forecast.<br>- rainfall_volume: Extracts rainfall data, handling missing or invalid values.</p>

###

<h3 align="center">Gold Layer: Weather Data Aggregation & Analytics</h3>

###

<p align="left">
The gold layer contains dbt models that aggregate, summarize, and enrich the normalized weather data from the silver layer. These models are designed for advanced analytics, reporting, and business intelligence.

**Key Models:**
<p align="left">
- daily_weather_summary: Aggregates daily weather metrics such as minimum, maximum, and average temperature, total rainfall, average humidity, maximum wind speed, and most frequent weather condition.<br>
- forecast_summary_per_3h: Provides a comprehensive 3-hourly weather forecast, joining all relevant silver models for detailed visibility, precipitation, temperature, wind, cloud coverage, and weather conditions.<br>
- weather_events: Detects and summarizes extreme weather events (e.g., heavy rain, strong winds, extreme temperatures) for each day.
</p>

The gold layer enables high-level insights and supports decision-making by transforming granular weather data into analytics-ready tables.
</p>
