from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "cristiandominguezgutierrez",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="silver_layer_dag",
    default_args=default_args,
    description="Execute dbt silver model",
    schedule_interval=None,
    catchup=False,
)


run_dbt_silver_task = BashOperator(
    task_id="run_dbt_silver_model",
    bash_command="cd /opt/airflow/dbt && dbt run --models silver",
    dag=dag,
)

#Run gold DAG after silver layer
trigger_gold_dag = TriggerDagRunOperator(
    task_id="trigger_gold_layer_dag",
    trigger_dag_id="gold_layer_dag", 
    dag=dag,
)

# Set task dependencies
run_dbt_silver_task >> trigger_gold_dag