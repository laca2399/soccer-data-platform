from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

import sys
sys.path.append("/opt/airflow/scripts")

from load_analytics_to_postgres import load_analytics_tables


default_args = {
    "owner": "andres",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="soccer_analytics_loader",
    default_args=default_args,
    description="Create analytics schema and load processed soccer data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger for now
    catchup=False,
    template_searchpath=["/opt/airflow/sql/postgres"]
) as dag:
    

    health_check = BashOperator(
        task_id="health_check",
        bash_command="bash -c '/opt/airflow/scripts/health_checks.sh'"
    )


    create_schema = PostgresOperator(
        task_id="create_schema",
        postgres_conn_id="soccer_postgres",
        sql="01_create_schema.sql"
    )


    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="soccer_postgres",
        sql="02_create_tables.sql"
    )

    load_analytics = PythonOperator(
        task_id="load_analytics_tables",
        python_callable=load_analytics_tables
    )

    health_check >> create_schema >> create_tables >>load_analytics



