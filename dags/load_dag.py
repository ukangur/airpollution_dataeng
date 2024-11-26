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
                username=os.getenv("MONGO_INITDB_ROOT_USERNAME"),
                password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
            )
    return client

def initialize_duckdb_tables():
    print("Creating DuckDB tables...")

    db_path = "/opt/airflow/duckdb_data/airpolandweather.db"

    con = duckdb.connect(db_path)

    # Create the Location table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Location (
        id INTEGER PRIMARY KEY,       -- Primary key
        street TEXT,                  -- Street
        city TEXT,                    -- City
        county TEXT,                  -- County
        country TEXT,                 -- Country
        latitude TEXT,                -- Latitude
        longitude TEXT,               -- Longitude
        elevation_m FLOAT             -- Elevation above sea level (m)
    );
    """)

    # Create the Station table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Station (
        id INTEGER PRIMARY KEY,       -- Primary key
        name TEXT NOT NULL,           -- Station name
        location_id INTEGER,          -- Foreign key to Location
        FOREIGN KEY(location_id) REFERENCES Location(id)
    );
    """)

    # Create the Radar table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Radar (
        id INTEGER PRIMARY KEY,           -- Primary key
        station_id INTEGER NOT NULL,      -- Foreign key to Station
        radar_radius_km FLOAT,            -- Radar radius in km
        radar_frequency_band TEXT,        -- Radar frequency band
        FOREIGN KEY(station_id) REFERENCES Station(id)
    );
    """)

    # Create the Date table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Date (
        id INTEGER PRIMARY KEY,       -- Primary key
        year INTEGER NOT NULL,        -- Year
        month INTEGER NOT NULL,       -- Month
        day INTEGER NOT NULL          -- Day
    );
    """)

    # Create the Time table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Time (
        id INTEGER PRIMARY KEY,       -- Primary key
        hours INTEGER NOT NULL,       -- Hour
        minutes INTEGER NOT NULL,     -- Minutes
        seconds INTEGER NOT NULL      -- Seconds
    );
    """)

    # Create the Observation table
    con.execute("""
    CREATE TABLE IF NOT EXISTS Observation (
        id INTEGER PRIMARY KEY,                -- Primary key
        radar_id INTEGER NOT NULL,             -- Foreign key to Radar
        date_id INTEGER NOT NULL,              -- Foreign key to Date
        time_id INTEGER NOT NULL,              -- Foreign key to Time

        air_temperature_c FLOAT,               -- Air Temperature (°C)
        min_temperature_c FLOAT,               -- Hourly Minimum Temperature (°C)
        max_temperature_c FLOAT,               -- Hourly Maximum Temperature (°C)

        wind_direction_deg FLOAT,              -- 10-minute Average Wind Direction (°)
        avg_wind_speed_m_s FLOAT,              -- 10-minute Average Wind Speed (m/s)
        max_wind_speed_m_s FLOAT,              -- Hourly Maximum Wind Speed (m/s)

        solar_radiation_w_m2 FLOAT,            -- Hourly Sum Solar Radiation (W/m²)
        sea_level_pressure_hPa FLOAT,          -- Sea Level Air Pressure (hPa)
        station_level_pressure_hPa FLOAT,      -- Station-Level Air Pressure (hPa)
        precipitation_mm FLOAT,                -- Hourly Precipitation (mm)
        relative_humidity_pct FLOAT,           -- Relative Humidity (%)

        pm25_10_ug_m3 FLOAT,                   -- PM2.5-10 (µg/m³)
        pm25_ug_m3 FLOAT,                      -- PM2.5 (µg/m³)
        pm10_ug_m3 FLOAT,                      -- PM10 (µg/m³)
        so2_ug_m3 FLOAT,                       -- SO2 (µg/m³)
        no2_ug_m3 FLOAT,                       -- NO2 (µg/m³)
        o3_ug_m3 FLOAT,                        -- O3 (µg/m³)

        FOREIGN KEY(radar_id) REFERENCES Radar(id),
        FOREIGN KEY(date_id) REFERENCES Date(id),
        FOREIGN KEY(time_id) REFERENCES Time(id)
    );
    """)

    print("DuckDB tables initialized successfully!")

def extract_data(**kwargs):
    try:
        print("Trying to connect to MongoDB...")
        client = get_mongo_client("mongo")
        db = client.raw_observation
        print("Successfully connected to MongoDB collection airweather!")

        def clean_mongo_document(doc):
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            return doc

        weather_data = [clean_mongo_document(doc) for doc in db.weather_data.find()]
        print(f"Fetched {len(weather_data)} weather data documents.")

        airpollution_data = [clean_mongo_document(doc) for doc in db.airpollution_data.find()]
        print(f"Fetched {len(airpollution_data)} air pollution data documents.")

        station_data = [clean_mongo_document(doc) for doc in db.station_data.find()]
        print(f"Fetched {len(station_data)} station data documents.")

        client.close()

        return {
            'weather': weather_data,
            'air_pollution': airpollution_data,
            'stations': station_data
        }
    except Exception as e:
        print(f"Error while extracting data: {e}")
        return None

def load_to_duckdb(**kwargs):
    try:
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='extract_data')

        print("Loaded MongoDB data")

        db_path = "/opt/airflow/duckdb_data/airpolandweather.db"
        con = duckdb.connect(db_path)

        print("Connected to DuckDB database")

        weather_data = pd.DataFrame(data['weather'])
        airpollution_data = pd.DataFrame(data['air_pollution'])
        station_data = pd.DataFrame(data['stations'])

        print(weather_data.head(1))
        print(airpollution_data.head(1))
        print(station_data.head(1))

        # Process Location table
        if not station_data.empty:
            print("Loading Location data into DuckDB...")
            location_data = station_data[['street', 'city', 'county', 'country', 'latitude', 'longitude', 'elevation_m']]
            location_data = location_data.drop_duplicates()
            location_data['id'] = range(1, len(location_data) + 1)
            location_data = location_data[['id','street', 'city', 'county', 'country', 'latitude', 'longitude', 'elevation_m']]

            print(location_data.head(1))
            con.execute("INSERT INTO Location SELECT * FROM location_data")

        # Process Station table
        if not station_data.empty:
            print("Loading Station data into DuckDB...")
            station_data = station_data.merge(
                location_data[['id', 'street', 'city', 'county', 'country', 'latitude', 'longitude', 'elevation_m']],
                on=['street', 'city', 'county', 'country', 'latitude', 'longitude', 'elevation_m'],
                how='left'
            ).rename(columns={'id': 'location_id'})

            station_table_data = station_data[['name', 'location_id']].drop_duplicates()
            station_table_data['id'] = range(1, len(station_table_data) + 1)
            station_table_data = station_table_data[['id','name', 'location_id']]

            print(station_table_data.head(1))
            con.execute("INSERT INTO Station SELECT * FROM station_table_data")

        # Process Radar table
        if not station_data.empty:
            print("Loading Radar data into DuckDB...")
            station_data = station_data.merge(
            station_table_data[['id', 'name']],
            left_on='name',
            right_on='name',
            how='left'
            ).rename(columns={'id': 'station_id'})

            radar_data = station_data[['station_id', 'radar_radius_km', 'radar_frequency_band']]
            radar_data = radar_data.drop_duplicates()
            radar_data['id'] = range(1, len(radar_data) + 1)
            radar_data = radar_data[['id', 'station_id', 'radar_radius_km', 'radar_frequency_band']]

            print(radar_data.head(1))
            con.register('radar_data', radar_data)
            con.execute("INSERT INTO Radar SELECT * FROM radar_data")
        # Process Date table
        if not weather_data.empty or not airpollution_data.empty:
            print("Loading Date data into DuckDB...")

            airpollution_data['date'] = airpollution_data['date'].astype(str)
            airpollution_dates = airpollution_data['date'].str.split('-', expand=True).rename(
                columns={0: 'year', 1: 'month', 2: 'day'}
            )
            airpollution_dates['year'] = airpollution_dates['year'].astype(int)
            airpollution_dates['month'] = airpollution_dates['month'].astype(int)
            airpollution_dates['day'] = airpollution_dates['day'].astype(int)

            date_data = pd.concat(
                [
                    weather_data[['year', 'month', 'day']],
                    airpollution_dates[['year', 'month', 'day']]
                ],
                ignore_index=True
            )
            date_data = date_data.drop_duplicates().reset_index(drop=True)
            date_data['id'] = range(1, len(date_data) + 1)

            date_data = date_data[['id', 'year', 'month', 'day']]

            print(date_data.head(1))
            con.register('date_data', date_data)
            con.execute("INSERT INTO Date SELECT * FROM date_data")

        # Process Time table
        if not weather_data.empty or not airpollution_data.empty:
            print("Loading Time data into DuckDB...")

            airpollution_times = pd.DataFrame({
                'hours': [0],
                'minutes': [0],
                'seconds': [0]
            })

            weather_data['time_utc'] = weather_data['time_utc'].fillna("00:00")
            weather_times = weather_data['time_utc'].str.split(':', expand=True).rename(
                columns={0: 'hours', 1: 'minutes'}
            )
            weather_times['seconds'] = 0
            weather_times['hours'] = weather_times['hours'].astype(int)
            weather_times['minutes'] = weather_times['minutes'].astype(int)
            weather_times['seconds'] = weather_times['seconds'].astype(int)

            time_data = pd.concat([weather_times, airpollution_times], ignore_index=True)
            time_data = time_data.drop_duplicates().reset_index(drop=True)
            time_data['id'] = range(1, len(time_data) + 1)

            time_data = time_data[['id', 'hours', 'minutes', 'seconds']]

            print(time_data.head(1))
            con.register('time_data', time_data)
            con.execute("INSERT INTO Time SELECT * FROM time_data")

        # Process Observation table
        if not weather_data.empty or not airpollution_data.empty:
            print("Loading Observation data into DuckDB...")

            time_data['time_key'] = time_data['hours'].astype(str).str.zfill(2) + ":" + \
                                    time_data['minutes'].astype(str).str.zfill(2) + ":" + \
                                    time_data['seconds'].astype(str).str.zfill(2)

            weather_data['time_key'] = weather_data['time_utc'] + ":00"

            weather_data = weather_data.merge(
                time_data[['time_key', 'id']].rename(columns={'id': 'time_id'}),
                on='time_key',
                how='left'
            )

            weather_data = weather_data.merge(
                date_data[['year', 'month', 'day', 'id']].rename(columns={'id': 'date_id'}),
                on=['year', 'month', 'day'],
                how='left'
            )

            airpollution_data['year'], airpollution_data['month'], airpollution_data['day'] = zip(
                *airpollution_data['date'].str.split('-').apply(lambda x: (int(x[0]), int(x[1]), int(x[2])))
            )

            airpollution_data = airpollution_data.merge(
                date_data[['year', 'month', 'day', 'id']].rename(columns={'id': 'date_id'}),
                on=['year', 'month', 'day'],
                how='left'
            )

            default_time = time_data[(time_data['hours'] == 0) & (time_data['minutes'] == 0) & (time_data['seconds'] == 0)]
            if not default_time.empty:
                default_time_id = default_time['id'].iloc[0]
                airpollution_data['time_id'] = default_time_id
            else:
                raise ValueError("Default time_id for 00:00:00 not found in Time table.")

            combined_data = pd.concat(
                [
                    weather_data[['date_id', 'time_id', 'air_temperature_c', 'min_temperature_c',
                                'max_temperature_c', 'wind_direction_deg', 'avg_wind_speed_m_s', 'max_wind_speed_m_s',
                                'solar_radiation_w_m2', 'sea_level_pressure_hPa', 'station_level_pressure_hPa',
                                'precipitation_mm', 'relative_humidity_pct']],
                    airpollution_data[['date_id', 'time_id', 'pm25_10_ug_m3', 'pm25_ug_m3', 'pm10_ug_m3', 'so2_ug_m3',
                                    'no2_ug_m3', 'o3_ug_m3']]
                ],
                ignore_index=True
            )

            combined_data['radar_id'] = 1

            observation_data = combined_data[[
                'date_id', 'time_id', 'radar_id',
                'air_temperature_c', 'min_temperature_c', 'max_temperature_c',
                'wind_direction_deg', 'avg_wind_speed_m_s', 'max_wind_speed_m_s',
                'solar_radiation_w_m2', 'sea_level_pressure_hPa', 'station_level_pressure_hPa',
                'precipitation_mm', 'relative_humidity_pct',
                'pm25_10_ug_m3', 'pm25_ug_m3', 'pm10_ug_m3', 'so2_ug_m3', 'no2_ug_m3', 'o3_ug_m3'
            ]]

            observation_data['id'] = range(1, len(observation_data) + 1)

            observation_data = observation_data[[
                'id', 'radar_id', 'date_id', 'time_id',
                'air_temperature_c', 'min_temperature_c', 'max_temperature_c',
                'wind_direction_deg', 'avg_wind_speed_m_s', 'max_wind_speed_m_s',
                'solar_radiation_w_m2', 'sea_level_pressure_hPa', 'station_level_pressure_hPa',
                'precipitation_mm', 'relative_humidity_pct',
                'pm25_10_ug_m3', 'pm25_ug_m3', 'pm10_ug_m3', 'so2_ug_m3', 'no2_ug_m3', 'o3_ug_m3'
            ]]

            print(observation_data.head(1))
            con.register('observation_data', observation_data)
            con.execute("INSERT INTO Observation SELECT * FROM observation_data")

        print("All data successfully loaded into DuckDB!")
    except Exception as e:
        print(f"Error while loading data: {e}")

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

load_to_duckdb_task = PythonOperator(
    task_id='load_to_duckdb',
    python_callable=load_to_duckdb,
    op_kwargs={'data': '{{ task_instance.xcom_pull(task_ids="transform_data") }}'},
    dag=load_dag
)

initalize_duckdb_task >> extract_task >> load_to_duckdb_task