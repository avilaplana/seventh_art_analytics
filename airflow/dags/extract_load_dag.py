from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sys

# Add extract src dir to path
sys.path.append("/opt/airflow/extract/src")
import extract_to_s3

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "daily_etl",
    default_args=default_args,
    description="Extract to S3 -> Load to Iceberg (sequential, clean)",
    schedule_interval=None,  # ← disable automatic scheduling
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:

    # Step 1: Extract
    extract_task = PythonOperator(
        task_id="extract_to_s3",
        python_callable=extract_to_s3.main,
    )
    extract_task
