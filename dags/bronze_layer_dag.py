import requests
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv("/opt/airflow/.env")

# Local path to store the weather data file
LOCAL_FILE_PATH = os.environ.get("LOCAL_FILE_PATH")
# Database schema name
SCHEMA_NAME = "public_bronze"

def get_db_engine():
    # Create the database engine using environment variables
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    host = os.environ.get("POSTGRES_HOST")
    db = os.environ.get("POSTGRES_DB")
    return create_engine(f'postgresql://{user}:{password}@{host}:5432/{db}')

def extract_and_load_weather_data():
    # Fetch weather data from API and load directly into the database
    try:
        api_url = os.environ.get("URL_WEATHER_API")
        response = requests.get(api_url)
        response.raise_for_status()

        weather_data = response.json()
        weather_list = weather_data.get('list', [])
        df = pd.DataFrame(weather_list)

        # Convert dict/list columns to JSON strings for SQL compatibility
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df[col] = df[col].apply(json.dumps)

        # Add ingestion timestamp column
        df['ingestion_timestamp'] = datetime.now()

        engine = get_db_engine()
        # Create schema if it doesn't exist
        with engine.connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")

        # Insert DataFrame into bronze_weather table
        df.to_sql(
            name="bronze_weather",
            schema=SCHEMA_NAME,
            con=engine,
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000,
        )

        print(f"Data fetched from API and loaded directly to bronze table. {len(df)} rows inserted.")
        return True

    except Exception as e:
        print(f"Error fetching or loading data: {e}")
        return False

# Default DAG configuration
default_args = {
    "owner": "cristiandominguezgutierrez",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Main DAG definition
dag = DAG(
    dag_id="bronze_layer_dag",
    default_args=default_args,
    description="DAG for extracting weather data from API and storing in raw layer",
    schedule_interval=None,
    catchup=False,
)

# Task to extract and load data from API directly to bronze
extract_and_load_task = PythonOperator(
    task_id="extract_and_load_weather_data",
    python_callable=extract_and_load_weather_data,
    dag=dag,
)

#Run silver DAG after bronze layer
trigger_silver_dag = TriggerDagRunOperator(
    task_id="trigger_silver_layer_dag",
    trigger_dag_id="silver_layer_dag", 
    dag=dag,
)

# Set task dependencies
extract_and_load_task >> trigger_silver_dag