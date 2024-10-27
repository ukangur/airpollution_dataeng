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

def move_to_mongodb_weather():
    file_path = '/opt/airflow/dags/weather.xlsx'
    data = pd.read_excel(file_path, skiprows=2)

    column_rename_map = {
        'Aasta': 'year',
        'Kuu': 'month',
        'Päev': 'day',
        'Kell (UTC)': 'time',
        'Tunni keskmine summaarne kiirgus W/m²': 'solar_radiation',
        'Õhurõhk merepinna kõrgusel hPa': 'sea_level_pressure',
        'Õhurõhk jaama kõrgusel hPa': 'station_pressure',
        'Tunni sademete summa mm': 'precipitation',
        'Suhteline õhuniiskus %': 'humidity',
        'Õhutemperatuur °C': 'temp',
        'Tunni miinimum õhutemperatuur °C': 'temp_min',
        'Tunni maksimum õhutemperatuur °C': 'temp_max',
        '10 minuti keskmine tuule suund °': 'wind_dir',
        '10 minuti keskmine tuule kiirus m/s': 'wind_speed',
        'Tunni maksimum tuule kiirus m/s': 'wind_max_speed'
    }

    data = data.rename(columns=column_rename_map)
    
    data_dict = data.to_dict("records")

    for record in data_dict:
        for key, value in record.items():
            if isinstance(value, datetime.time):
                record[key] = value.strftime('%H:%M')
            elif pd.isna(value):
                record[key] = None
    
    mongo = "mongo"
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            print("Attempting to connect to MongoDB... Attempt", attempt + 1)
            client = MongoClient(
                "mongodb://" + mongo + ":27017/",
                username='admin',
                password='admin'
            )

            print("Connected to MongoDB!")

            db = client.airweather_database
            
            print("created database in MongoDB!")

            collection = db.weather_data

            print("created collection in MongoDB!")

            collection.insert_many(data_dict)
            print("Data successfully inserted into MongoDB!")

            client.close()
            break

        except ServerSelectionTimeoutError as e:
            print(f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    else:
        print("Failed to connect to MongoDB after multiple attempts.")


def move_to_mongodb_airpollution():
    file_path = '/opt/airflow/dags/airpol.xlsx'
    data = pd.read_excel(file_path)

    column_rename_map = {
        'Kuupäev': 'date',
        'PM2.5-10, µg/m3': 'pm25_10',
        'PM2.5, µg/m3': 'pm25',
        'PM10, µg/m3': 'pm10',
        'SO2,  µg/m3': 'so2',
        'NO2,  µg/m3': 'no2',
        'O3,  µg/m3': 'o3'
    }

    data = data.rename(columns=column_rename_map)
    
    data_dict = data.to_dict("records")

    for record in data_dict:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
    
    mongo = "mongo"
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            print("Attempting to connect to MongoDB... Attempt", attempt + 1)
            client = MongoClient(
                "mongodb://" + mongo + ":27017/",
                username='admin',
                password='admin'
            )

            print("Connected to MongoDB!")

            db = client.airweather_database
            
            print("created database in MongoDB!")

            collection = db.airpollution_data

            print("created collection in MongoDB!")

            collection.insert_many(data_dict)
            print("Data successfully inserted into MongoDB!")

            client.close()
            break

        except ServerSelectionTimeoutError as e:
            print(f"Connection attempt {attempt + 1} failed. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    else:
        print("Failed to connect to MongoDB after multiple attempts.")

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

first_dag = DAG(
    dag_id='first_dag',
    default_args=default_args_dict,
    catchup=False,
)

task_one = BashOperator(
    task_id='get_weatherdata',
    dag=first_dag,
    bash_command="curl https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tallinn-Harku-2004-juuni-2024.xlsx --output /opt/airflow/dags/weather.xlsx",
)

task_two = PythonOperator(
        task_id='move_to_mongodb_weather',
        python_callable=move_to_mongodb_weather,
        dag=first_dag
    )

task_three = PythonOperator(
        task_id='move_to_mongodb_airpollution',
        python_callable=move_to_mongodb_airpollution,
        dag=first_dag
    )

task_one >> task_two >> task_three
