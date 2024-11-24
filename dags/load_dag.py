import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
from pymongo import MongoClient
import duckdb
import os

def get_mongo_client(mongo):

    client = MongoClient(
                "mongodb://" + mongo + ":27017/",
                username='admin',
                password='admin'
            )
    return client

def initialize_duckdb_tables():
    print("Creating DuckDB table...")

    db_path = "/opt/airflow/duckdb_data/airpolandweather.db"

    con = duckdb.connect(db_path)

    # Create the Date table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Date (
        id INTEGER PRIMARY KEY,
        year INTEGER NOT NULL,    -- Year
        month INTEGER NOT NULL,   -- Month
        day INTEGER NOT NULL,     -- Day
        time_utc TIME NOT NULL    -- Time (UTC)
    );
    """)

    # Create the Temperature table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Temperature (
        id INTEGER PRIMARY KEY,
        date_id INTEGER NOT NULL REFERENCES Date(id),
        air_temperature FLOAT,     -- Air Temperature (°C)
        min_temperature FLOAT,     -- Hourly Minimum Temperature (°C)
        max_temperature FLOAT      -- Hourly Maximum Temperature (°C)
    );
    """)

    # Create the Wind table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Wind (
        id INTEGER PRIMARY KEY,
        date_id INTEGER NOT NULL REFERENCES Date(id),
        wind_direction FLOAT,      -- 10-minute Average Wind Direction (°)
        avg_wind_speed FLOAT,      -- 10-minute Average Wind Speed (m/s)
        max_wind_speed FLOAT       -- Hourly Maximum Wind Speed (m/s)
    );
    """)

    # Create the Weather table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Weather (
        id INTEGER PRIMARY KEY,
        date_id INTEGER NOT NULL REFERENCES Date(id),
        solar_radiation FLOAT,              -- Hourly Sum Solar Radiation (W/m²)
        sea_level_pressure FLOAT,           -- Sea Level Air Pressure (hPa)
        station_level_pressure FLOAT,       -- Station-Level Air Pressure (hPa)
        precipitation FLOAT,                -- Hourly Precipitation (mm)
        relative_humidity FLOAT             -- Relative Humidity (%)
    );
    """)

    # Create the Air_Pollution table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Airpollution (
        id INTEGER PRIMARY KEY,
        date_id INTEGER NOT NULL REFERENCES Date(id),
        pm25_10 FLOAT,      -- PM2.5-10 (µg/m³)
        pm25 FLOAT,         -- PM2.5 (µg/m³)
        pm10 FLOAT,         -- PM10 (µg/m³)
        so2 FLOAT,          -- SO2 (µg/m³)
        no2 FLOAT,          -- NO2 (µg/m³)
        o3 FLOAT            -- O3 (µg/m³)
    );
    """)

    print("DuckDB tables initialized successfully!")

def extract_data(**kwargs):
    try:
        print("Trying to connect to mongoDB...")
        client = get_mongo_client("mongo")
        db = client.airweather_database
        print("Successfully connected to mongodb collection airweather!")

        def clean_mongo_document(doc):
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            return doc

        weather_data = [clean_mongo_document(doc) for doc in db.weather_data.find()]
        airpollution_data = [clean_mongo_document(doc) for doc in db.airpollution_data.find()]


        client.close()

        return {'weather': weather_data, 'air_pollution': airpollution_data}
    except Exception as e:
        print(f"Error while extracting data: {e}")
        return None

def load_and_transform_data(**kwargs):
    try:

        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract_data')

        print("Loaded MongoDB data")

        db_path = "/opt/airflow/duckdb_data/airpolandweather.db"
        con = duckdb.connect(db_path)

        print("Connected to DuckDB database")

        weather_data = pd.DataFrame(data['weather'])
        print(weather_data.head(1))
        airpollution_data = pd.DataFrame(data['air_pollution'])
        print(airpollution_data.head(1))

        # Process Date table
        if not weather_data.empty or not airpollution_data.empty:
            print("Loading Date data into DuckDB...")

            if not weather_data.empty:
                weather_data['time_utc'] = weather_data['time'].apply(
                    lambda x: x if isinstance(x, str) and ':' in x else "00:00"
                )

            if not airpollution_data.empty:
                airpollution_data['date'] = airpollution_data['date'].fillna("").astype(str)

            date_data = pd.concat(
                [
                    weather_data[['year', 'month', 'day', 'time_utc']],
                    airpollution_data['date'].str.split('-', expand=True).rename(
                        columns={0: 'year', 1: 'month', 2: 'day'}
                    ).assign(time_utc='00:00')
                ],
                ignore_index=True
            )

            date_data['year'] = date_data['year'].astype(int)
            date_data['month'] = date_data['month'].astype(int)
            date_data['day'] = date_data['day'].astype(int)
            date_data['time_utc'] = date_data['time_utc'].astype(str)
            date_data = date_data.drop_duplicates(subset=['year', 'month', 'day', 'time_utc'])
            date_data['id'] = range(1, len(date_data) + 1)

            date_data = date_data[['id', 'year', 'month', 'day', 'time_utc']]

            print(date_data.head(2))

            con.sql("INSERT INTO Date SELECT * FROM date_data")

        # Process Temperature table
        if not weather_data.empty:
            print("Loading Temperature data into DuckDB...")

            weather_data['year'] = weather_data['year'].astype(int)
            weather_data['month'] = weather_data['month'].astype(int)
            weather_data['day'] = weather_data['day'].astype(int)
            weather_data['time_utc'] = weather_data['time_utc'].astype(str)

            weather_data = weather_data.merge(
                date_data[['year', 'month', 'day', 'time_utc', 'id']],
                on=['year', 'month', 'day', 'time_utc'],
                how='left'
            ).rename(columns={'id': 'date_id'})

            temperature_data = weather_data[['date_id', 'temp', 'temp_min', 'temp_max']].rename(
                columns={'temp': 'air_temperature', 'temp_min': 'min_temperature', 'temp_max': 'max_temperature'}
            )

            temperature_data['id'] = range(1, len(temperature_data) + 1)
            temperature_data = temperature_data[['id', 'date_id', 'air_temperature', 'min_temperature', 'max_temperature']]
            print(temperature_data.head(1))

            con.sql("INSERT INTO Temperature SELECT * FROM temperature_data")

        # Process Wind table
        if not weather_data.empty:
            print("Loading Wind data into DuckDB...")
            wind_data = weather_data[['date_id', 'wind_dir', 'wind_speed', 'wind_max_speed']].rename(
                columns={'wind_dir': 'wind_direction', 'wind_speed': 'avg_wind_speed', 'wind_max_speed': 'max_wind_speed'}
            )

            wind_data['id'] = range(1, len(wind_data) + 1)
            wind_data = wind_data[['id', 'date_id', 'wind_direction', 'avg_wind_speed', 'max_wind_speed']]
            print(wind_data.head(1))

            con.sql("INSERT INTO Wind SELECT * FROM wind_data")

        # Process Weather table
        if not weather_data.empty:
            print("Loading Weather data into DuckDB...")
            weather_table_data = weather_data[['date_id', 'solar_radiation', 'sea_level_pressure', 'station_pressure',
                                               'precipitation', 'humidity']].rename(
                columns={'station_pressure': 'station_level_pressure', 'humidity': 'relative_humidity'}
            )

            weather_table_data['id'] = range(1, len(weather_table_data) + 1)
            weather_table_data = weather_table_data[['id', 'date_id', 'solar_radiation', 'sea_level_pressure', 'station_level_pressure', 'precipitation', 'relative_humidity']]

            print(weather_table_data.head(1))
            con.sql("INSERT INTO Weather SELECT * FROM weather_table_data")

        # Process Air Pollution table
        if not airpollution_data.empty:
            print("Loading Air Pollution data into DuckDB...")
            airpollution_data[['year', 'month', 'day']] = airpollution_data['date'].str.split('-', expand=True)
            airpollution_data['year'] = airpollution_data['year'].astype(int)
            airpollution_data['month'] = airpollution_data['month'].astype(int)
            airpollution_data['day'] = airpollution_data['day'].astype(int)
            airpollution_data['time_utc'] = '00:00'

            airpollution_data = airpollution_data.merge(
                date_data[['year', 'month', 'day', 'time_utc', 'id']],
                on=['year', 'month', 'day', 'time_utc'],
                how='left'
            ).rename(columns={'id': 'date_id'})

            airpollution_data['id'] = range(1, len(airpollution_data) + 1)
            pollution_data = airpollution_data[['id', 'date_id', 'pm25_10', 'pm25', 'pm10', 'so2', 'no2', 'o3']]
            print(pollution_data.head(1))

            con.sql("INSERT INTO Airpollution SELECT * FROM pollution_data")

        print("All data successfully loaded into DuckDB!")
    except Exception as e:
        print(f"Error while loading data: {e}")

def transform_data():
    return None

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

load_dag = DAG(
    dag_id='load_dag',
    default_args=default_args_dict,
    catchup=False,
)

initalize_duckdb_task = PythonOperator(
    task_id='initialize_duckdb_tables',
    python_callable=initialize_duckdb_tables,
    dag=load_dag
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=load_dag
)

load_and_transform_task = PythonOperator(
    task_id='load_and_transform_data',
    python_callable=load_and_transform_data,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=load_dag
)

initalize_duckdb_task >> extract_task >> load_and_transform_task