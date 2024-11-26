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
import csv
from bs4 import BeautifulSoup
import requests
import os

def get_mongo_client(mongo):

    client = MongoClient(
                "mongodb://" + mongo + ":27017/",
                username=os.getenv("MONGO_INITDB_ROOT_USERNAME"),
                password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
            )
    return client

def parse_coordinates(coordinate_str):
    direction = coordinate_str[0]
    coord_parts = coordinate_str[1:].strip().split("°")
    degrees = float(coord_parts[0])
    minutes, seconds = map(float, coord_parts[1].replace("´´", "").split("´"))
    decimal = degrees + minutes / 60 + seconds / 3600
    
    if direction in "SW":
        decimal = -decimal
    return decimal

def get_country_from_coordinates(lat, lon):
    api_key = os.getenv("GEOAPIFY_KEY")
    if not api_key:
        raise ValueError("GEOAPIFY_KEY environment variable is not set.")
    geocode_url = f'https://api.geoapify.com/v1/geocode/reverse?lat={lat}&lon={lon}&apiKey={api_key}'
    response = requests.get(geocode_url)
    response.raise_for_status()
    data = response.json()
    country = data['features'][0]['properties'].get('country', 'Unknown')
    return country

def get_stationdata():
    url = "https://www.ilmateenistus.ee/meist/vaatlusvork/tallinn-harku-aeroloogiajaam/"

    response = requests.get(url)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')

    name = soup.find('h2', class_='mb-1').get_text(strip=True)

    address_paragraph = soup.find('p', class_='has-very-light-gray-background-color has-background')
    if not address_paragraph:
        raise ValueError("Address paragraph not found in the HTML.")

    address_lines = address_paragraph.get_text(separator="\n").split("\n")

    street, city, county = address_lines[0].split(", ")

    latitude_line = next((line for line in address_lines if "Laius" in line), None)
    longitude_line = next((line for line in address_lines if "Pikkus" in line), None)

    if latitude_line:
        latitude_str = latitude_line.split(":")[1].strip() or address_lines[address_lines.index(latitude_line) + 1].strip()
    else:
        raise ValueError("Latitude information is missing.")

    if longitude_line:
        longitude_str = longitude_line.split(":")[1].strip() or address_lines[address_lines.index(longitude_line) + 1].strip()
    else:
        raise ValueError("Longitude information is missing.")

    latitude = parse_coordinates(latitude_str)
    longitude = parse_coordinates(longitude_str)

    country = get_country_from_coordinates(latitude, longitude)

    elevation_line_index = next((i for i, line in enumerate(address_lines) if "Vaatlusväljaku kõrgus" in line), None)
    if elevation_line_index is None or elevation_line_index + 1 >= len(address_lines):
        raise ValueError("Elevation information is missing or improperly formatted.")

    elevation_line = address_lines[elevation_line_index + 1]
    elevation_m = float(elevation_line.split(" ")[0].replace(",", "."))

    radar_paragraph = soup.find('p', text="Radar")
    if not radar_paragraph:
        raise ValueError("Radar information not found.")
    radar_info_lines = radar_paragraph.find_next_sibling('p').get_text(separator="\n").split("\n")
    radar_radius_km = float(radar_info_lines[1].split(" ")[0])
    radar_frequency_band = radar_info_lines[3].strip()

    data = {
    "street": [street],
    "city": [city],
    "county": [county],
    "country": [country],
    "latitude": [latitude],
    "longitude": [longitude],
    "elevation_m": [elevation_m],
    "radar_radius_km": [radar_radius_km],
    "radar_frequency_band": [radar_frequency_band],
    "name": [name]
    }

    df = pd.DataFrame(data)

    csv_file_path = "/opt/airflow/dags/station_data.csv"

    df.to_csv(csv_file_path, index=False, encoding='utf-8')

    print(f"Data successfully saved to {csv_file_path}")

def move_to_mongodb_weather():
    file_path = '/opt/airflow/dags/weather.xlsx'
    data = pd.read_excel(file_path, skiprows=2)

    column_rename_map = {
        'Aasta': 'year',
        'Kuu': 'month',
        'Päev': 'day',
        'Kell (UTC)': 'time_utc',
        'Tunni keskmine summaarne kiirgus W/m²': 'solar_radiation_w_m2',
        'Õhurõhk merepinna kõrgusel hPa': 'sea_level_pressure_hPa',
        'Õhurõhk jaama kõrgusel hPa': 'station_level_pressure_hPa',
        'Tunni sademete summa mm': 'precipitation_mm',
        'Suhteline õhuniiskus %': 'relative_humidity_pct',
        'Õhutemperatuur °C': 'air_temperature_c',
        'Tunni miinimum õhutemperatuur °C': 'min_temperature_c',
        'Tunni maksimum õhutemperatuur °C': 'max_temperature_c',
        '10 minuti keskmine tuule suund °': 'wind_direction_deg',
        '10 minuti keskmine tuule kiirus m/s': 'avg_wind_speed_m_s',
        'Tunni maksimum tuule kiirus m/s': 'max_wind_speed_m_s'
    }

    data = data.rename(columns=column_rename_map)
    
    data_dict = data.to_dict("records")

    for record in data_dict:
        for key, value in record.items():
            if isinstance(value, datetime.time):
                record[key] = value.strftime('%H:%M')
            elif pd.isna(value):
                record[key] = None
    
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            print("Attempting to connect to MongoDB... Attempt", attempt + 1)
            client = get_mongo_client("mongo")

            print("Connected to MongoDB!")

            db = client.raw_observation
            
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
        'PM2.5-10, µg/m3': 'pm25_10_ug_m3',
        'PM2.5, µg/m3': 'pm25_ug_m3',
        'PM10, µg/m3': 'pm10_ug_m3',
        'SO2,  µg/m3': 'so2_ug_m3',
        'NO2,  µg/m3': 'no2_ug_m3',
        'O3,  µg/m3': 'o3_ug_m3'
    }

    data = data.rename(columns=column_rename_map)
    
    data_dict = data.to_dict("records")

    for record in data_dict:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
    
    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            print("Attempting to connect to MongoDB... Attempt", attempt + 1)
            client = get_mongo_client("mongo")
            print("Connected to MongoDB!")

            db = client.raw_observation
            
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

def move_stationdata_to_mongo():
    file_path = "/opt/airflow/dags/station_data.csv"
    data = pd.read_csv(file_path)
    
    data_dict = data.to_dict("records")

    max_retries = 5
    retry_delay = 10

    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to MongoDB... Attempt {attempt + 1}")
            client = get_mongo_client("mongo")

            print("Connected to MongoDB!")

            db = client.raw_observation
            collection = db.station_data

            print("Created database and collection in MongoDB!")

            collection.insert_many(data_dict)
            print("Station data successfully inserted into MongoDB!")

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

extract_dag = DAG(
    dag_id='extract_dag',
    default_args=default_args_dict,
    catchup=False,
)

get_weatherdata_task = BashOperator(
    task_id='get_weatherdata',
    dag=extract_dag,
    bash_command="curl https://www.ilmateenistus.ee/wp-content/uploads/2024/07/Tallinn-Harku-2004-juuni-2024.xlsx --output /opt/airflow/dags/weather.xlsx",
)

get_stationdata_task = PythonOperator(
        task_id='get_stationdata',
        python_callable=get_stationdata,
        dag=extract_dag
    )

move_weather_to_mongo_task = PythonOperator(
        task_id='move_to_mongodb_weather',
        python_callable=move_to_mongodb_weather,
        dag=extract_dag
    )

move_airpol_to_mongo_task = PythonOperator(
        task_id='move_to_mongodb_airpollution',
        python_callable=move_to_mongodb_airpollution,
        dag=extract_dag
    )

move_stationdata_to_mongo_task = PythonOperator(
        task_id='move_stationdata_to_mongo',
        python_callable=move_stationdata_to_mongo,
        dag=extract_dag
    )

[get_weatherdata_task, get_stationdata_task] >> move_weather_to_mongo_task
move_weather_to_mongo_task >> move_airpol_to_mongo_task
move_airpol_to_mongo_task >> move_stationdata_to_mongo_task
