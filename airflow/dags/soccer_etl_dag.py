from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "andres",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="soccer_raw_ingestion",
    default_args=default_args,
    description="Load raw soccer CSV data into Postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    health_check = BashOperator(
        task_id="health_check",
        bash_command="bash /opt/airflow/scripts/health_checks.sh"
    )


    def print_status():
        print("CSV files ready for ingestion")

    
    check_files = PythonOperator(
        task_id="check_files",
        python_callable=print_status
    )


health_check >> check_files