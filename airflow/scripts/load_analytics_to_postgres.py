import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "soccer_postgres"

BASE_PATH = "/opt/airflow/datasets/processed"

TABLES = {
    "dim_league": "dim_league.csv",
    "dim_team": "dim_team.csv",
    "fact_matches": "fact_matches.csv",
}

def get_engine():
    """
    Get SQLAlchemy engine using Airflow connection
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    return engine

def load_table(engine, table_name, file_name):
    """
    Load a single CSV file into Postgres table
    """
    file_path = f"{BASE_PATH}/{file_name}"

    print(f"Loading file: {file_path}")

    df = pd.read_csv(file_path)

    df.to_sql(
        name=table_name,
        con=engine,
        schema="soccer_analytics",
        if_exists="append",
        index=False,
        method="multi"
    )

    print(f"Loaded table: {table_name} ({len(df)} rows)")


def load_analytics_tables():
    """
    Main function executed by Airflow PythonOperator
    """
    print("Starting analytics layer load...")

    engine = get_engine()

    for table_name, file_name in TABLES.items():
        load_table(engine, table_name, file_name)

    print("All analytics tables loaded successfully")