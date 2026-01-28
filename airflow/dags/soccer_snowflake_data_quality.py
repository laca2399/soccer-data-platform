from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    "owner": "andres",
    "retries": 1
}

with DAG(
    dag_id="soccer_snowflake_data_quality",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description="Snowflake analytics data quality checks"
) as dag:

    check_fact_not_empty = SnowflakeOperator(
        task_id="check_fact_not_empty",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT COUNT(*) FROM ANALYTICS.FACT_MATCHES;
        """
    )

    check_no_negative_goals = SnowflakeOperator(
        task_id="check_no_negative_goals",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT COUNT(*) 
        FROM ANALYTICS.FACT_MATCHES
        WHERE home_goals < 0 OR away_goals < 0;
        """
    )

    check_dim_team_not_null = SnowflakeOperator(
        task_id="check_dim_team_not_null",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="""
        SELECT COUNT(*) 
        FROM ANALYTICS.DIM_TEAM
        WHERE team_id IS NULL;
        """
    )

    check_fact_not_empty >> check_no_negative_goals >> check_dim_team_not_null
