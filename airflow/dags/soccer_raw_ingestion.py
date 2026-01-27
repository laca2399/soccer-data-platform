from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


from scripts.load_raw_to_postgres import load_raw_tables


default_args = {
    "owner": "andres",
    "retries": 1,
    "retry_delay": timedelta(minutes=3)
}

with DAG(
    dag_id="soccer_raw_ingestion",
    default_args=default_args,
    description="RAW Kaggle CSV ingestion into Postgres",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    template_searchpath=["/opt/airflow/sql/postgres"]
) as dag:

    health_check = BashOperator(
    task_id="health_check",
    bash_command="bash -c '/opt/airflow/scripts/health_checks.sh'"
)

    create_raw_schema = PostgresOperator(
        task_id="create_raw_schema",
        postgres_conn_id="soccer_postgres",
        sql="00_create_raw_schema.sql"
    )

    create_raw_tables = PostgresOperator(
        task_id="create_raw_tables",
        postgres_conn_id="soccer_postgres",
        sql="03_create_raw_tables.sql"
    )

    load_raw_tables_task = PythonOperator(
        task_id="load_raw_tables",
        python_callable=load_raw_tables
    )

    health_check >> create_raw_schema >> create_raw_tables >> load_raw_tables_task
