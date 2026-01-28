from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import pandas as pd
from datetime import datetime

POSTGRES_CONN_ID = "soccer_postgres"
SNOWFLAKE_CONN_ID = "snowflake_default"

SOURCE_SCHEMA = "soccer_raw"
TARGET_SCHEMA = "RAW"

TABLES = [
    "country",
    "league",
    "team",
    "player",
    "player_attributes",
    "team_attributes",
    "match"
]

# Optimized chunk size for Docker + Snowflake stability
CHUNK_SIZE = 20000


def transfer_table(table_name):

    print(f"Starting Snowflake transfer: {table_name}")

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    pg_engine = pg_hook.get_sqlalchemy_engine()
    sf_engine = sf_hook.get_sqlalchemy_engine()

    query = f"SELECT * FROM {SOURCE_SCHEMA}.{table_name}"

    chunk_counter = 0
    total_rows = 0

    for chunk in pd.read_sql(query, pg_engine, chunksize=CHUNK_SIZE):

        # Snowflake prefers uppercase columns
        chunk.columns = [c.upper() for c in chunk.columns]

        chunk.to_sql(
            name=table_name.upper(),
            con=sf_engine,
            schema=TARGET_SCHEMA,
            if_exists="append",
            index=False,
            method="multi"
        )

        rows = len(chunk)
        total_rows += rows
        chunk_counter += 1

        print(f"{table_name}: chunk {chunk_counter} loaded ({rows} rows)")

    print(f"{table_name} completed. Total rows loaded: {total_rows}")


default_args = {
    "owner": "andres",
    "retries": 1,
    "retry_delay": pd.Timedelta(minutes=2)
}

with DAG(
    dag_id="soccer_snowflake_raw_loader",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="ELT pipeline: Load Postgres RAW into Snowflake RAW"
) as dag:

    for table in TABLES:
        PythonOperator(
            task_id=f"load_{table}_to_snowflake",
            python_callable=transfer_table,
            op_args=[table]
        )
