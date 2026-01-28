from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount



from datetime import datetime, timedelta
import sys
import os

def build_spark_submit(job):
    return f"""\
spark-submit \
--master spark://spark-master:7077 \
--deploy-mode client \
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--conf spark.sql.defaultCatalog=demo \
--conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
--conf spark.sql.catalog.demo.uri=http://rest:8181 \
--conf spark.sql.catalog.demo.warehouse=s3://warehouse/ \
--conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.catalog.demo.s3.endpoint=http://minio:9000 \
--conf spark.sql.catalog.demo.s3.path-style-access=true \
--conf spark.sql.catalog.demo.s3.access-key-id=admin \
--conf spark.sql.catalog.demo.s3.secret-access-key=password \
--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
--conf spark.hadoop.fs.s3a.path.style.access=true \
--conf spark.hadoop.fs.s3a.access.key=admin \
--conf spark.hadoop.fs.s3a.secret.key=password \
--conf spark.driver.extraJavaOptions="-Daws.region=eu-west-2" \
--conf spark.executor.extraJavaOptions="-Daws.region=eu-west-2" \
--packages \
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,\
org.apache.iceberg:iceberg-aws-bundle:1.8.1,\
org.apache.hadoop:hadoop-aws:3.3.4 \
{job}
"""

# Add extract src dir to path
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "test_etl",
    default_args=default_args,
    description="Extract to S3 -> Load to Iceberg (parallel Spark jobs)",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:

    # Step 1: Extract all IMDB raw files to S3/MinIO
    EXTRACT_SCRIPT_DIR = "/opt/airflow/extract/src/"
    extract_scripts = [
        "extract_name_basics_to_s3",
        ]

    extract_tasks = []
    sys.path.append(EXTRACT_SCRIPT_DIR)

    for script in extract_scripts:
        extract_task = PythonOperator(
            task_id=f"{script}",
            python_callable=lambda script=script: __import__(script).main(),
        )
        extract_tasks.append(extract_task)

    # Step 2: Load each raw file from S3/MinIO to Iceberg using Spark jobs
    # Directory containing Spark job scripts
    SPARK_JOBS_DIR = "/opt/airflow/load/src/"
    pyspark_jobs = [
        "create_tables",
        "load_to_iceberg_name_basics",        
        ]
    spark_tasks = []
    
    for job in pyspark_jobs:
        spark_task_id = f"spark_{job}"
        spark_task = BashOperator(
            retries=0,          # fail fast on Spark job
            task_id=spark_task_id,
            bash_command=build_spark_submit(f"{SPARK_JOBS_DIR}{job}.py")
        )
        spark_tasks.append(spark_task)

    projects_dir = os.environ["PROJECTS_DIR"]

    dbt_silver_layer = DockerOperator(
        task_id="data_modelling_silver_layer",
        image="dbt-spark:f5bf2ec",
        command="build --profiles-dir /usr/app/dbt --target local --project-dir /usr/app/dbt/silver --select silver.person",
        mounts=[
                Mount(
                    source=f"{projects_dir}/seventh_art_analytics/transform",
                    target="/usr/app/dbt",
                    type="bind",
                )
            ],
        network_mode="seventh_art_analytics_iceberg_net",
        docker_url="unix://var/run/docker.sock",
        auto_remove=True,
        tty=True,
        mount_tmp_dir=False,
    )
        
    # Step 3: Set dependencies (extract -> Spark jobs sequentially)
    extract_tasks >> spark_tasks[0] >> spark_tasks[1] >> dbt_silver_layer
    