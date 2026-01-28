from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    "owner": "andres",
    "retries": 1
}

with DAG(
    dag_id="soccer_snowflake_analytics_builder",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Build Snowflake Analytics Star Schema"
) as dag:

    create_dim_league = SnowflakeOperator(
        task_id="create_dim_league",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        CREATE OR REPLACE TABLE ANALYTICS.DIM_LEAGUE AS
        SELECT
            l.id AS league_id,
            l.name AS league_name,
            c.name AS country_name
        FROM RAW.LEAGUE l
        LEFT JOIN RAW.COUNTRY c
            ON l.country_id = c.id;
        """
    )

    create_dim_team = SnowflakeOperator(
        task_id="create_dim_team",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        CREATE OR REPLACE TABLE ANALYTICS.DIM_TEAM AS
        SELECT
            team_api_id AS team_id,
            team_long_name,
            team_short_name
        FROM RAW.TEAM;
        """
    )

    create_dim_player = SnowflakeOperator(
    task_id="create_dim_player",
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql="""
    CREATE OR REPLACE TABLE ANALYTICS.DIM_PLAYER AS
    SELECT
        player_api_id AS player_id,

        -- Remove shirt number after comma
        TRIM(SPLIT(player_name, ',')[0]) AS player_name,

        birthday,
        height,
        weight
    FROM RAW.PLAYER;
    """
    )


    create_fact_matches = SnowflakeOperator(
        task_id="create_fact_matches",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        CREATE OR REPLACE TABLE ANALYTICS.FACT_MATCHES AS
        SELECT
            m.id AS match_id,
            m.league_id,
            m.home_team_api_id AS home_team_id,
            m.away_team_api_id AS away_team_id,
            m.home_team_goal AS home_goals,
            m.away_team_goal AS away_goals,
            m.date::DATE AS match_date,
            m.season,

            (m.home_team_goal - m.away_team_goal) AS goal_diff,

            CASE WHEN m.home_team_goal > m.away_team_goal THEN 1 ELSE 0 END AS home_win,
            CASE WHEN m.away_team_goal > m.home_team_goal THEN 1 ELSE 0 END AS away_win,
            CASE WHEN m.home_team_goal = m.away_team_goal THEN 1 ELSE 0 END AS draw

        FROM RAW.MATCH m;
        """
    )

    create_dim_league >> create_dim_team >> create_dim_player >> create_fact_matches
