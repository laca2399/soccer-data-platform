import os
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

BASE_PATH = "/opt/airflow/datasets/raw"

TABLES = {
    "country": "country.csv",
    "league": "league.csv",
    "team": "team.csv",
    "team_attributes": "team_attributes.csv",
    "player": "player.csv",
    "player_attributes": "player_attributes.csv",
    "match": "match.csv"
}

CHUNK_SIZE = 20000   # Safe for large match.csv


def load_table(pg_hook, table_name, file_name):
    file_path = f"{BASE_PATH}/{file_name}"

    print(f"Loading RAW file: {file_path}")

    engine = pg_hook.get_sqlalchemy_engine()

    chunk_count = 0
    total_rows = 0
    first_chunk = True

    for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):

        # Normalize column names for Postgres compatibility
        chunk.columns = [c.lower() for c in chunk.columns]

        # First chunk creates table automatically
        if first_chunk:
            if_exists_mode = "replace"
            first_chunk = False
            print(f"Creating RAW table soccer_raw.{table_name}")
        else:
            if_exists_mode = "append"

        chunk.to_sql(
            name=table_name,
            con=engine,
            schema="soccer_raw",
            if_exists=if_exists_mode,
            index=False,
        )

        rows = len(chunk)
        total_rows += rows
        chunk_count += 1

        print(f"Inserted chunk {chunk_count} ({rows} rows)")

    print(f"Finished loading {table_name}: {total_rows} rows")


def load_raw_tables():
    print("Starting RAW ingestion pipeline")

    pg_hook = PostgresHook(postgres_conn_id="soccer_postgres")

    for table_name, file_name in TABLES.items():
        load_table(pg_hook, table_name, file_name)

    print("RAW ingestion completed successfully")
