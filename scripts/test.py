from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import boto3
import datetime

# ── Config ─────────────────────────────────────────────────────────────
BUCKET_NAME = "lab3-bucket"
RAW_PREFIX = "raw/streams/"
ARCHIVE_PREFIX = "archive/streams/"
GLUE_CLEAN_JOB = "Transformation_job"
GLUE_KPI_JOB = "dailykpis"
AWS_REGION = "eu-north-1"
DEFAULT_STREAM_FILE = "streams1.csv"  # fallback

# ── Utility: Find the newest S3 file ────────────────────────────────────
def get_latest_s3_file(bucket, prefix):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    all_files = response.get("Contents", [])
    if not all_files:
        raise ValueError("No files found in S3.")
    latest_file = max(all_files, key=lambda x: x["LastModified"])
    return latest_file["Key"]

# ── Archive function ────────────────────────────────────────────────────
def archive_stream_file(**kwargs):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    input_file_key = kwargs['ti'].xcom_pull(task_ids='detect_latest_file')

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    archive_key = f"{ARCHIVE_PREFIX}{today}/{input_file_key.split('/')[-1]}"

    # Move: copy + delete
    s3.copy_object(Bucket=BUCKET_NAME, CopySource={"Bucket": BUCKET_NAME, "Key": input_file_key}, Key=archive_key)
    s3.delete_object(Bucket=BUCKET_NAME, Key=input_file_key)

# ── DAG definition ──────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
}

with DAG(
    dag_id="glue_stream_etl_pipeline",
    default_args=default_args,
    # start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["glue", "etl", "kpis", "dynamodb"],
) as dag:

    # Step 1: Wait for a stream file to land in S3
    wait_for_stream_file = S3KeySensor(
        task_id="wait_for_stream_file",
        bucket_name=BUCKET_NAME,
        bucket_key=f"{RAW_PREFIX}*.csv",
        wildcard_match=True,
        aws_conn_id="aws_default",
        timeout=60 * 60,
        poke_interval=60,
        mode="poke",
    )

    # Step 2: Get the latest stream file key from S3
    def push_latest_file_path(ti, **kwargs):
        latest_file_key = get_latest_s3_file(BUCKET_NAME, RAW_PREFIX)
        ti.xcom_push(key="stream_file_key", value=latest_file_key)
        return latest_file_key

    detect_latest_file = PythonOperator(
        task_id="detect_latest_file",
        python_callable=push_latest_file_path,
        provide_context=True,
    )

    # Step 3: Trigger Glue job to clean & process the file
    run_clean_glue_job = GlueJobOperator(
        task_id="run_clean_glue_job",
        job_name=GLUE_CLEAN_JOB,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--JOB_NAME": GLUE_CLEAN_JOB,
            "--INPUT_FILE_PATH": "{{ task_instance.xcom_pull(task_ids='detect_latest_file') | replace(' ', '') | string | join('') | string }}"
        }
    )

    # Step 4: Trigger Glue job to compute and load KPIs
    run_kpi_glue_job = GlueJobOperator(
        task_id="run_kpi_glue_job",
        job_name=GLUE_KPI_JOB,
        aws_conn_id="aws_default",
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={"--JOB_NAME": GLUE_KPI_JOB},
    )

    # Step 5: Archive the file after processing
    archive_stream_data = PythonOperator(
        task_id="archive_stream_data",
        python_callable=archive_stream_file,
        provide_context=True,
    )

    # DAG dependencies
    wait_for_stream_file >> detect_latest_file >> run_clean_glue_job >> run_kpi_glue_job >> archive_stream_data
