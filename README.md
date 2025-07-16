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

<p align="left">- Loads configuration from environment variables for API and database connection.<br>- Extracts weather data and saves it as a local JSON file.<br>- Transforms nested fields to JSON strings for SQL compatibility.<br>- Loads the processed data into the bronze_weather table within the bronze schema.<br>- Includes error handling and logging for each step.</p>

###

<h3 align="center">Silver Layer: Weather Data Normalization</h3>

###

<p align="left">The silver layer consists of dbt models that normalize and clean the raw weather data ingested in the bronze layer.<br>Each model extracts relevant fields from nested JSON structures, casts them to appropriate SQL types, and handles missing or invalid values.</p>

###

<h3 align="left">Key models</h3>

###

<p align="left">- atmospheric_conditions: Extracts and normalizes temperature, pressure, humidity, and related metrics.<br>- weather_conditions: Flattens weather array details such as condition ID, group, description, and icon.<br>- cloud_coverage: Normalizes cloud coverage percentage for each forecast.<br>- rainfall_volume: Extracts rainfall data, handling missing or invalid values.</p>

###
