# dags/breweries_pipeline.py
# This DAG defines the data pipeline for the BEES technical case,
# ingesting data from the Open Brewery DB API and transforming it
# into a Medallion Architecture data lake (Bronze, Silver, Gold).

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="breweries_pipeline",
    start_date=pendulum.datetime(2025, 8, 16, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["bees", "data-engineering", "data-pipeline"],
) as dag:
    # Task to run the data ingestion script
    # This task will call a Python script to extract data from the API
    # and save it to the Bronze layer.
    extract_data_bronze = BashOperator(
        task_id="extract_data_bronze",
        # Usando o caminho absoluto do contêiner Docker para o diretório de scripts.
        bash_command="python /opt/airflow/src/ingestion.py",
    )

    # Task to run the data transformation script
    # This task will read data from the Bronze layer, clean it, and
    # save it to the Silver layer in Parquet format, partitioned by state.
    transform_data_silver = BashOperator(
        task_id="transform_data_silver",
        # Usando o caminho absoluto do contêiner Docker para o diretório de scripts.
        bash_command="python /opt/airflow/src/transformation.py",
    )

    # Task to run the data aggregation script
    # This task will read data from the Silver layer, aggregate it
    # to count breweries per type and state, and save it to the Gold layer.
    aggregate_data_gold = BashOperator(
        task_id="aggregate_data_gold",
        # Usando o caminho absoluto do contêiner Docker para o diretório de scripts.
        bash_command="python /opt/airflow/src/aggregation.py",
    )

    # Define the task dependencies
    # This specifies the order of execution:
    # 1. extract_data_bronze -> 2. transform_data_silver -> 3. aggregate_data_gold
    extract_data_bronze >> transform_data_silver >> aggregate_data_gold
