import sqlite3
import pandas as pd
from pathlib import Path

#Paths
BASE_DIR = Path(__file__).resolve().parent.parent
SQLITE_DB_PATH = BASE_DIR / "datasets" / "kaggle" / "database.sqlite"
RAW_DATA_DIR = BASE_DIR / "datasets" / "raw"

#Tables
TABLES = [
    "country",
    "league",
    "match",
    "player",
    "player_attributes",
    "team",
    "team_attributes",
]

def extract_tables():
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(SQLITE_DB_PATH)

    try:
        for table in TABLES:
            print(f"Extracting table: {table}")

            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)

            output_path = RAW_DATA_DIR / f"{table}.csv"
            df.to_csv(output_path, index=False)

            print(f"  -> {len(df)} rows written to {output_path}")
        
    finally:
        conn.close()
        print("SQLite connection closed.")

if __name__ == "__main__":
    extract_tables()