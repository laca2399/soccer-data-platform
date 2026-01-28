# Soccer Data Engineering Platform

End-to-end data engineering project using soccer data.

This project implements a full production-style data pipeline that ingests soccer data from Kaggle, processes it using PostgreSQL, orchestrates transformations with Apache Airflow, and loads analytics-ready star schema tables into Snowflake.

The platform demonstrates modern ELT architecture, orchestration, data warehousing, and data quality validation.

Technology Stack:

| Layer            | Technology                  |
| ---------------- | --------------------------- |
| Ingestion        | Python, Pandas              |
| Orchestration    | Apache Airflow 2.8 (Docker) |
| Database         | PostgreSQL 15               |
| Warehouse        | Snowflake                   |
| Transformations  | SQL (Postgres + Snowflake)  |
| Containerization | Docker Compose              |
| Version Control  | GitHub                      |

Pipeline Phases
Phase 1 — Raw Ingestion

Kaggle SQLite dataset exported to CSV

Chunk-based ingestion into PostgreSQL

Health check BashOperator

Memory-safe processing of large files (269MB+)

DAG: soccer_raw_ingestion

---

Phase 2 — ETL Staging

Clean null values

Normalize teams and players

Deduplicate records

Prepare staging tables

DAG: soccer_etl_staging

---

Phase 3 — Snowflake ELT

Load Postgres data into Snowflake RAW layer

Build analytics star schema

Full-refresh dimension and fact tables

Apply business transformations

DAGs:

raw_loader

analytics_builder

---

Phase 4 — Data Quality Validation

Row count checks

Duplicate detection

Data integrity validation

DAG: data_quality

---

                ┌──────────────┐
                │ Kaggle SQLite │
                └──────┬───────┘
                       │
              CSV Export / Preprocessing
                       │
                ┌──────▼───────┐
                │ Local CSVs    │
                │ (datasets)   │
                └──────┬───────┘
                       │
        ┌──────────────▼──────────────┐
        │ Airflow DAG 1: Raw Ingestion │
        │ - health_checks.sh           │
        │ - Chunked CSV Load           │
        └──────────────┬──────────────┘
                       │
               ┌───────▼────────┐
               │ PostgreSQL RAW │
               └───────┬────────┘
                       │
        ┌──────────────▼──────────────┐
        │ Airflow DAG 2: ETL Staging   │
        │ - Cleaning                   │
        │ - Deduplication              │
        │ - Normalization              │
        └──────────────┬──────────────┘
                       │
              ┌────────▼────────┐
              │ PostgreSQL STG   │
              └────────┬────────┘
                       │
        ┌──────────────▼──────────────┐
        │ Airflow DAG 3: Snowflake ELT │
        │ - RAW Loader                 │
        │ - Analytics Builder          │
        └──────────────┬──────────────┘
                       │
               ┌───────▼────────┐
               │ Snowflake RAW  │
               └───────┬────────┘
                       │
               ┌───────▼────────┐
               │ Snowflake DWH  │
               │ STAR SCHEMA    │
               └────────────────┘

## DAG 1 — soccer_raw_ingestion

health_check
↓
load_raw_tables

## DAG 2 — soccer_etl_staging

transform_leagues
↓
transform_teams
↓
transform_players
↓
transform_matches

## DAG 3 — soccer_snowflake_raw_loader

copy_postgres_to_snowflake_raw

## DAG 4 — soccer_snowflake_analytics_builder

create_dim_league
↓
create_dim_team
↓
create_dim_player
↓
create_fact_matches

## DAG 5 — data_quality

row_count_check
null_check
duplicate_check

                 DIM_LEAGUE
                ┌──────────┐
                │ league_id│
                │ name      │
                └─────┬────┘
                      │

DIM_TEAM FACT_MATCHES DIM_PLAYER
┌─────────┐ ┌────────────┐ ┌──────────┐
│ team_id │◄────│ home_team │────►│ player_id│
│ name │ │ away_team │ │ name │
└─────────┘ │ league_id │ └──────────┘
│ goals │
│ season │
└────────────┘

Results

Fully automated pipeline
Star schema ready for BI tools
Replayable and reproducible pipeline
Handles large datasets safely
Cloud warehouse integration

## Pipeline Screenshots

![DAG List](screenshots/airflow_dag_list.png)

![Analytics Builder](screenshots/airflow_analytics_graph.png)

### Snowflake Warehouse

![Warehouse Tables](screenshots/snowflake_tables.png)

![Fact Count](screenshots/snowflake_fact_count.png)
