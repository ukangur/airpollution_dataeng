import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
import boto3
import duckdb
import time
from cryptography.fernet import Fernet
import os

def save_encrypted_parquet_to_s3():
    encryption_key = os.getenv("ENCRYPTION_KEY")
    if not encryption_key:
        raise ValueError("ENCRYPTION_KEY is not set in the environment variables.")

    cipher = Fernet(encryption_key)

    db_path = "/opt/airflow/duckdb_data/airpolandweather.db"
    conn = duckdb.connect(database=db_path, read_only=True)

    query = "SELECT * FROM main.Observation"
    combined_data = conn.execute(query).fetchdf()
    print(combined_data.head(1))
    conn.close()

    temp_parquet_path = "/tmp/observation.parquet"
    combined_data.to_parquet(temp_parquet_path, index=False)

    with open(temp_parquet_path, "rb") as file:
        encrypted_data = cipher.encrypt(file.read())

    encrypted_parquet_path = "/tmp/encrypted_observation.parquet"

    with open(encrypted_parquet_path, "wb") as file:
        file.write(encrypted_data)

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY"),
    )

    bucket_name = "encrypted-data"
    object_name = "observation.parquet"

    try:
        s3.create_bucket(Bucket=bucket_name)
    except s3.exceptions.BucketAlreadyOwnedByYou:
        pass

    s3.upload_file(encrypted_parquet_path, bucket_name, object_name)

    print(f"Encrypted Parquet file uploaded to S3 bucket '{bucket_name}' as '{object_name}'.")

    os.remove(temp_parquet_path)
    os.remove(encrypted_parquet_path)


default_args_dict = {
    "start_date": airflow.utils.dates.days_ago(0),
    "concurrency": 1,
    "schedule_interval": None,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

transform_dag = DAG(
    dag_id="transform_dag",
    default_args=default_args_dict,
    catchup=False,
)

deal_with_missing_observations_task = BashOperator(
    task_id="deal_with_missing_observations",
    dag=transform_dag,
    bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt run --profiles-dir /opt/airflow/dbt_data --select missing_observations --full-refresh
    """,
)

make_dbt_snapshot_task = BashOperator(
    task_id="make_dbt_snapshot",
    dag=transform_dag,
    bash_command="""
        cd /opt/airflow/dbt_project && \
        dbt snapshot --profiles-dir /opt/airflow/dbt_data
    """,
)

save_encrypted_parquet_to_s3_task = PythonOperator(
    task_id="save_encrypted_parquet_to_s3",
    python_callable=save_encrypted_parquet_to_s3,
    dag=transform_dag,
)

deal_with_missing_observations_task >> make_dbt_snapshot_task >> save_encrypted_parquet_to_s3_task