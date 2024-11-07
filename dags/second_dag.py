import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
from pymongo import MongoClient
import time
from pymongo.errors import ServerSelectionTimeoutError

def funcone():
    return None
def functwo():
    return None
def functhree():
    return None

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

first_dag = DAG(
    dag_id='second_dag',
    default_args=default_args_dict,
    catchup=False,
)

task_one = PythonOperator(
        task_id='funcone',
        python_callable=funcone,
        dag=first_dag
    )

task_two = PythonOperator(
        task_id='functwo',
        python_callable=functwo,
        dag=first_dag
    )

task_three = PythonOperator(
        task_id='functhree',
        python_callable=functhree,
        dag=first_dag
    )

task_one >> task_two >> task_three
# MongoDB Client Setup
def get_mongo_client():
    client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
    return client

# Define Schema Initialization
def initialize_collections():
    client = get_mongo_client()
    db = client["weather_data"]
    
    # Define collections with their fields
    collections = {
        "Date": {
            "fields": ["id", "Aasta", "Kuu", "Päev", "Kell (UTC)"]
        },
        "Temperature": {
            "fields": ["id", "date_id", "temp", "temp_min", "temp_max"]
        },
        "Wind": {
            "fields": ["id", "date_id", "wind_dir", "wind_speed", "wind_max_speed"]
        },
        "Weather": {
            "fields": ["id", "date_id", "solar_radiation", "sea_level_pressure", "station_pressure", "precipitation", "humidity"]
        },
        "Air_Pollution": {
            "fields": ["id", "date_id", "pm25_10", "pm25", "pm10", "so2", "no2", "o3"]
        }
    }
    
    # Initialize collections
    for name, meta in collections.items():
        db.create_collection(name)
        for field in meta["fields"]:
            db[name].create_index(field)

# Example ETL Functions
def extract_data():
    # Placeholder: Extract raw data from sources (e.g., CSV, API)
    print("Extracting data...")
    return {"date_data": {}, "temp_data": {}, "wind_data": {}, "weather_data": {}, "pollution_data": {}}

def transform_data(data):
    # Placeholder: Transform extracted data to fit schema
    print("Transforming data...")
    transformed_data = {}
    return transformed_data

def load_data(data):
    # Placeholder: Load transformed data into MongoDB
    print("Loading data to MongoDB...")
    client = get_mongo_client()
    db = client["weather_data"]
    for collection, records in data.items():
        if records:
            db[collection].insert_many(records)

# Airflow Default Args
default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define DAG
first_dag = DAG(
    dag_id='second_dag',
    default_args=default_args_dict,
    catchup=False,
)

# Initialize MongoDB Collections
initialize_task = PythonOperator(
    task_id='initialize_collections',
    python_callable=initialize_collections,
    dag=first_dag
)

# ETL Tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=first_dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="extract_data") }}'},
    dag=first_dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=first_dag
)

# Task Dependencies
initialize_task >> extract_task >> transform_task >> load_task