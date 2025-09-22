from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sys

# Add both extract and load src dirs to path
sys.path.append("/opt/airflow/extract/src")

import upload_to_s3  # from extract/src/upload_to_s3.py

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "daily_etl",
    default_args=default_args,
    description="Extract to S3 -> Load to Iceberg",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_upload_to_s3",
        python_callable=upload_to_s3.main,
    )

    load_task = SparkSubmitOperator(
        task_id="load_to_iceberg",
        application="/opt/airflow/load/src/upload_to_iceberg.py",
        conn_id="spark_default",  # configure in Airflow UI
        verbose=True,
    )

    extract_task >> load_task
