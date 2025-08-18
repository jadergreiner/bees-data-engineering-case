# dags/breweries_pipeline_test_mode.py
# This is a test version of the DAG to demonstrate the failure callback.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Import the ingestion, transformation, and aggregation functions.
# Assuming they are defined in separate files.
from src.ingestion import main as extract_data_to_bronze
from src.transformation import main as transform_data_to_silver
from src.aggregation import main as aggregate_data_to_gold


def on_failure_callback(context):
    """
    A callback function to send an email on DAG failure.
    """
    subject = f"Airflow DAG Failure: {context['dag'].dag_id}"
    html_content = f"""
    <h3>Airflow Task Failure</h3>
    <p>DAG: {context['dag'].dag_id}</p>
    <p>Task: {context['task_instance'].task_id}</p>
    <p>Error Details: {context['exception']}</p>
    <p>Check the logs here: <a href="{context['task_instance'].log_url}">Link to Logs</a></p>
    """
    send_email(
        to=["jadergreiner@gmail.com"],
        subject=subject,
        html_content=html_content
    )


def run_data_quality_checks():
    """
    This function simulates data quality validation checks.
    For this test version, it will always pass to ensure the pipeline proceeds.
    """
    print("Starting data quality checks on the Silver layer...")
    print("Data quality checks passed. Proceeding to the Gold layer...")
    return True


def simulate_failure_task():
    """
    This function intentionally raises an error to test the failure callback.
    """
    raise ValueError("This is a simulated failure to test the on_failure_callback.")


with DAG(
    dag_id="breweries_pipeline_test_mode", # Use a different DAG ID for this test
    start_date=pendulum.datetime(2025, 8, 16, tz="UTC"),
    schedule=None,  # Disable the schedule for manual testing
    catchup=False,
    tags=["bees", "data-engineering", "data-pipeline", "test"],
    default_args={
        'retries': 0,  # Set retries to 0 for a quick failure test
        'on_failure_callback': on_failure_callback,
    }
) as dag:
    # 1. Extraction to the Bronze layer
    extract_bronze = PythonOperator(
        task_id="extract_data_to_bronze",
        python_callable=extract_data_to_bronze,
    )

    # 2. Transformation to the Silver layer
    transform_silver = PythonOperator(
        task_id="transform_data_to_silver",
        python_callable=transform_data_to_silver,
    )

    # 3. Data Quality Validation
    check_data_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=run_data_quality_checks,
    )

    # 4. Aggregation to the Gold layer
    aggregate_gold = PythonOperator(
        task_id="aggregate_data_to_gold",
        python_callable=aggregate_data_to_gold,
    )
    
    # 5. A new task to simulate a failure at the very end
    test_failure = PythonOperator(
        task_id="test_failure_callback",
        python_callable=simulate_failure_task,
        retries=0, # This task should fail immediately
    )

    # Define the task execution order
    extract_bronze >> transform_silver >> check_data_quality >> aggregate_gold >> test_failure