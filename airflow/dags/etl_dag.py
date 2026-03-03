from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from docker.types import Mount
from spark_utils import build_spark_submit
from datetime import datetime, timedelta
from datetime import datetime, timezone
import sys
import os

# Get current UTC datetime
now_utc = datetime.now(timezone.utc)

# Current date in YYYY-MM-DD format
snapshot_date = now_utc.date().isoformat()  # e.g., "2026-03-02"

# Current UTC timestamp in YYYY-MM-DD HH:MM:SS format
ingested_at_timestamp = now_utc.strftime("%Y-%m-%d %H:%M:%S")  # e.g., "2026-03-02 14:23:45"

# Add extract src dir to path
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

MAX_DAG_RETRIES = 3
RETRY_COUNTER_VAR = "daily_prod_etl_medallion_retry_count"

def should_rerun_dag(**context):
    retries = int(Variable.get(RETRY_COUNTER_VAR, default_var=0))

    if retries >= MAX_DAG_RETRIES:
        raise AirflowSkipException("Max DAG retries reached")

    Variable.set(RETRY_COUNTER_VAR, retries + 1)

def reset_dag_retry_counter(**context):
    Variable.set(RETRY_COUNTER_VAR, 0)

##################################################
# FEATURE FLAG
##################################################

GLOBAL_EXTRACT_FLAG = "ENABLE_EXTRACT_STAGE"

##################################################
# FEATURE FLAG HELPER
##################################################

def skip_if_disabled(flag_name: str):
    enabled = Variable.get(flag_name, "true").lower() == "true"
    if not enabled:
        raise AirflowSkipException(f"{flag_name} disabled")

##################################################
# TASK CALLABLE
##################################################

def run_extract(script_name: str):
    # One global switch for ALL extract tasks
    skip_if_disabled(GLOBAL_EXTRACT_FLAG)

    module = __import__(script_name)
    module.main()

##################################################
# DAG
##################################################

with DAG(
    "daily_prod_etl_medallion",
    default_args=default_args,
    description="Extract to S3 -> Load to Iceberg (parallel Spark jobs)",
    schedule_interval=None,
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:

    ##################################################
    # Step 1: Extract all IMDB raw files to S3/MinIO #
    ##################################################
    EXTRACT_SCRIPT_DIR = "/opt/airflow/extract/src/"
    extract_raw_scripts = [
        "extract_name_basics_to_s3",
        "extract_title_principals_to_s3",
        "extract_title_akas_to_s3",
        "extract_title_basics_to_s3",
        "extract_title_crew_to_s3",
        "extract_title_episode_to_s3",
        "extract_title_ratings_to_s3"
        ]

    extract_tasks = []
    sys.path.append(EXTRACT_SCRIPT_DIR)

    for script in extract_raw_scripts:
        extract_task = PythonOperator(
            task_id=f"{script}",
            python_callable=run_extract,
            op_kwargs={"script_name": script},
        )
        extract_tasks.append(extract_task)

    ########################################################################
    # Step 2: Load each raw file from S3/MinIO to Iceberg using Spark jobs #
    ########################################################################
    SPARK_JOBS_DIR = "/opt/airflow/load/src/"
    load_bronze_jobs = [
        "create_tables",
        "load_to_iceberg_name_basics",
        "load_to_iceberg_title_akas",
        "load_to_iceberg_title_basics",
        "load_to_iceberg_title_crew",
        "load_to_iceberg_title_episode",
        "load_to_iceberg_title_principals",
        "load_to_iceberg_title_ratings"
        ]
    spark_bronze_tasks = []

    for job in load_bronze_jobs:
        spark_task_id = f"load_SPARK_stage_bronze_{job}"
        spark_task = BashOperator(
            retries=0,          # fail fast on Spark job
            task_id=spark_task_id,
            bash_command=build_spark_submit(f"{SPARK_JOBS_DIR}{job}.py", snapshot_date, ingested_at_timestamp)
        )
        spark_bronze_tasks.append(spark_task)

    ############################################
    # Step 3: Install DBT dependencies #
    ############################################
    projects_dir = os.environ["PROJECTS_DIR"]
    
    dbt_deps_command = """deps
    --project-dir /usr/app/dbt/silver
    """
    
    dbt_deps_task = DockerOperator(
        task_id="transform_DBT_stage_deps",
        image="dbt-spark:f5bf2ec",
        command=dbt_deps_command,
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

    ############################################
    # Step 4: Install DBT seed #
    ############################################
    projects_dir = os.environ["PROJECTS_DIR"]

    dbt_seed_command = """seed
    --project-dir /usr/app/dbt/silver
    """

    dbt_seed_task = DockerOperator(
        task_id="transform_DBT_stage_seed",
        image="dbt-spark:f5bf2ec",
        command=dbt_seed_command,
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


    ############################################
    # Step 5: Transform Medallion Silver layer #
    ############################################
    dbt_silver_run_command = """run 
    --profiles-dir /usr/app/dbt 
    --target local 
    --project-dir /usr/app/dbt/silver
    """
    
    dbt_silver_run_task = DockerOperator(
        task_id="transform_DBT_stage_silver_layer",
        image="dbt-spark:f5bf2ec",
        command=dbt_silver_run_command,
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

     ############################################
    # Step 6: Data Validation Silver layer #
    ############################################
    dbt_silver_validation_command = """test
    --profiles-dir /usr/app/dbt 
    --target local 
    --project-dir /usr/app/dbt/silver
    """
    
    dbt_silver_validation_task = DockerOperator(
        task_id="transform_DBT_data_quality_check_stage_silver_layer",
        image="dbt-spark:f5bf2ec",
        command=dbt_silver_validation_command,
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

    ############################################
    # Step 7: Retry whole DAG on validation failure
    ############################################

    wait_30_minutes = TimeDeltaSensor(
        task_id="wait_30_minutes_before_dag_retry",
        delta=timedelta(minutes=30),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    check_retry_limit = PythonOperator(
        task_id="check_dag_retry_limit",
        python_callable=should_rerun_dag,
    )

    restart_dag = TriggerDagRunOperator(
        task_id="restart_entire_dag",
        trigger_dag_id="daily_prod_etl_medallion",
        wait_for_completion=False,
        reset_dag_run=True,
    )

    reset_retry_counter = PythonOperator(
    task_id="reset_dag_retry_counter",
    python_callable=reset_dag_retry_counter,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)
   
    extract_tasks >> spark_bronze_tasks[0]

    spark_bronze_tasks[0].trigger_rule = TriggerRule.NONE_FAILED

    spark_bronze_tasks[0] >> spark_bronze_tasks[1:8] >> \
        dbt_deps_task >> \
        dbt_seed_task >> \
        dbt_silver_run_task >> \
        dbt_silver_validation_task

    # Success path → reset retry counter
    dbt_silver_validation_task >> reset_retry_counter

    # Failure path → wait → retry DAG
    dbt_silver_validation_task >> wait_30_minutes
    wait_30_minutes >> check_retry_limit >> restart_dag