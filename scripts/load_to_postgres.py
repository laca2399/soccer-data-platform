import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"

print(POSTGRES_USER, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB)


engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

BASE_PATH = "datasets/processed"

TABLES = {
    "dim_league": "dim_league.csv",
    "dim_team": "dim_team.csv",
    "fact_matches": "fact_matches.csv",
}

def load_table(table_name, file_name):
    df = pd.read_csv(f"{BASE_PATH}/{file_name}")
    df.to_sql(
        table_name,
        engine,
        schema="soccer_analytics",
        if_exists="append",
        index=False
    )
    print(f"Loaded {table_name}")

if __name__ == "__main__":
    for table, file in TABLES.items():
        load_table(table, file)

    print("All tables loaded successfully")