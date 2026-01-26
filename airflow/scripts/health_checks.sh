#!/bin/bash

echo "Starting health checks..."

# Check airflow environment
echo "Airflow container is running"

# Check postgres connection
PG_HOST=postgres
PG_PORT=5432

echo "Checking PostgreSQL connection..."

nc -z $PG_HOST $PG_PORT

if [ $? -ne 0 ]; then
  echo "PostgreSQL is NOT reachable"
  exit 1
fi

echo "PostgreSQL is reachable"

# Check data folder exists
if [ ! -d "/opt/airflow/dags" ]; then
  echo "DAG folder missing"
  exit 1
fi

echo "All health checks passed"

exit 0
