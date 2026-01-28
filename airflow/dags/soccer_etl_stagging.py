from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "andres",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="soccer_etl_staging",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Transform RAW soccer data into staging layer",
    template_searchpath=["/opt/airflow/sql/postgres"]
) as dag:

    create_schema = PostgresOperator(
        task_id="create_staging_schema",
        postgres_conn_id="soccer_postgres",
        sql="10_create_staging_schema.sql"
    )

    create_tables = PostgresOperator(
        task_id="create_staging_tables",
        postgres_conn_id="soccer_postgres",
        sql="11_create_staging_tables.sql"
    )

    transform_leagues = PostgresOperator(
    task_id="transform_leagues",
    postgres_conn_id="soccer_postgres",
    sql="20_transform_leagues.sql"
    )

    transform_teams = PostgresOperator(
    task_id="transform_teams",
    postgres_conn_id="soccer_postgres",
    sql="21_transform_teams.sql"
    )

    transform_players = PostgresOperator(
    task_id="transform_players",
    postgres_conn_id="soccer_postgres",
    sql="22_transform_players.sql"
    )

    transform_matches = PostgresOperator(
    task_id="transform_matches",
    postgres_conn_id="soccer_postgres",
    sql="23_transform_matches.sql"
    )


    create_schema >> create_tables >> transform_leagues >> transform_teams >> transform_players >> transform_matches
