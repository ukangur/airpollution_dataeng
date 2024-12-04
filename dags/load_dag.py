import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import pandas as pd
from pymongo import MongoClient
from datetime import date, timedelta
import calendar
import duckdb
import os

def get_mongo_client(mongo):

    client = MongoClient(
                "mongodb://" + mongo + ":27017/",
                username=os.getenv("MONGO_INITDB_ROOT_USERNAME"),
                password=os.getenv("MONGO_INITDB_ROOT_PASSWORD")
            )
    return client

def get_wind_id(speed):
    if speed < 3:
        return 1  # Calm
    elif 3 <= speed < 8:
        return 2  # Moderate
    else:
        return 3  # Windy

def get_rain_id(precipitation):
    if pd.isna(precipitation) or precipitation < 1:
        return 1  # No rain
    elif 1 <= precipitation < 5:
        return 2  # Light rain
    else:
        return 3  # Heavy rain

def get_pressure_id(pressure):
    if pressure >= 1015:
        return 1  # Stable
    elif 1005 <= pressure < 1015:
        return 2  # Low pressure
    else:
        return 3  # Very low pressure

def get_temperature_id(temp):
    if temp > 20:
        return 1  # Warm
    elif 0 <= temp <= 20:
        return 2  # Mild
    else:
        return 3  # Cold

def get_radiation_id(radiation):
    if pd.isna(radiation) or radiation < 30:
        return 1  # Low
    elif 30 <= radiation < 150:
        return 2  # Moderate
    else:
        return 3  # High

def get_humidity_id(humidity):
    if humidity < 70:
        return 1  # Comfortable
    elif 70 <= humidity < 90:
        return 2  # Humid
    else:
        return 3  # Very humid

def get_airpollution(row):
    thresholds = {
        'pm25': [12, 35.4, 55.4, 150.4, 250.4],
        'pm10': [54, 154, 254, 354, 424],
        'so2': [35, 75, 185, 304, 604],
        'no2': [53, 100, 360, 649, 1249],
        'o3': [54, 70, 85, 105, 200],
    }

    scores = {
        pollutant: score_pollutant(row.get(pollutant, 0), limits)
        for pollutant, limits in thresholds.items()
    }

    combined_score = sum(scores.values())
    dominant_pollutant = max(scores, key=scores.get)

    visibility_km = row.get('visibility_km', 300)

    return {
        'danger_level': classify_combined_danger(combined_score),
        'weather_state': classify_combined_score(combined_score),
        'dominant_pollutant': dominant_pollutant,
        'visibility_km': visibility_km
    }

def get_visibility_score(visibility_km):
    if pd.isna(visibility_km) or visibility_km >= 300:
        return 1  # Excellent visibility
    elif 200 <= visibility_km < 300:
        return 2  # Good visibility
    elif 100 <= visibility_km < 200:
        return 3  # Moderate visibility
    elif 50 <= visibility_km < 100:
        return 4  # Poor visibility
    else:
        return 5  # Very poor visibility

def score_pollutant(value, thresholds):
        for i, threshold in enumerate(thresholds):
            if value <= threshold:
                return i + 1
        return len(thresholds) + 1

# Based on AQI
def classify_combined_score(score):
    if score <= 5:
        return 'Good'
    elif score <= 10:
        return 'Moderate'
    elif score <= 15:
        return 'Unhealthy for Sensitive Groups'
    elif score <= 20:
        return 'Unhealthy'
    elif score <= 25:
        return 'Very Unhealthy'
    else:
        return 'Hazardous'

def classify_combined_danger(score):
        if score <= 10:
            return 1  # Low danger
        elif score <= 15:
            return 2  # Moderate danger
        elif score <= 20:
            return 3  # High danger
        elif score <= 25:
            return 4  # Very high danger
        else:
            return 5  # Severe hazard

def get_estonian_holidays(year):
    static_holidays = [
        date(year, 1, 1),  # New Year's Day
        date(year, 2, 24),  # Independence Day
        date(year, 5, 1),  # Spring Day
        date(year, 6, 23),  # Victory Day
        date(year, 6, 24),  # Midsummer Day
        date(year, 8, 20),  # Day of Restoration of Independence
        date(year, 12, 24),  # Christmas Eve
        date(year, 12, 25),  # Christmas Day
        date(year, 12, 26)  # Boxing Day
    ]

    # Moveable holidays: Good Friday and Easter
    easter = calculate_easter_date(year)
    good_friday = easter - timedelta(days=2)
    pentecost = easter + timedelta(days=49)  # 7 weeks after Easter

    moveable_holidays = [good_friday, easter, pentecost]

    return static_holidays + moveable_holidays

def calculate_easter_date(year):
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)

def is_holiday(date, all_holidays):
    return date in all_holidays

def is_workday(date):
    return date.weekday() < 5 and not is_holiday(date)

def initialize_duckdb_tables():
    print("Creating DuckDB tables...")

    db_path = "/opt/airflow/duckdb_data/airpolandweather.db"

    con = duckdb.connect(db_path)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Location (
        id INTEGER PRIMARY KEY,        -- Primary key
        station_name VARCHAR(100) NOT NULL,    -- Station name
        street VARCHAR(100),                   -- Street
        city VARCHAR(100),                     -- City
        county VARCHAR(100),                   -- County
        country VARCHAR(100),                  -- Country
        latitude VARCHAR(100),                 -- Latitude
        longitude VARCHAR(100),                -- Longitude
        elevation_m FLOAT,              -- Elevation above sea level (m)
        radar_radius_km FLOAT,         -- Radar radius (km)
        radar_frequency_band VARCHAR(100),     -- Radar frequency band
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Date (
        id INTEGER PRIMARY KEY,        -- Primary key
        year INTEGER NOT NULL,         -- Year
        month INTEGER NOT NULL,        -- Month
        day INTEGER NOT NULL,          -- Day
        holiday INTEGER NOT NULL,       -- Holiday indicator
        season VARCHAR(100) NOT NULL,          -- Season
        workday INTEGER NOT NULL,       -- Workday indicator
        quater INTEGER NOT NULL,       -- Quarter
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Time (
        id INTEGER PRIMARY KEY,        -- Primary key
        hours INTEGER NOT NULL,        -- Hour
        minutes INTEGER NOT NULL,      -- Minutes
        seconds INTEGER NOT NULL,      -- Seconds
        time_of_day VARCHAR(100) NOT NULL,     -- Time of day
        office_hours INTEGER NOT NULL,   -- Office hours indicator
        traffic_hour INTEGER NOT NULL   -- Traffic hour indicator
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Wind (
        id INTEGER PRIMARY KEY,        -- Primary key
        weather_state VARCHAR(100),            -- Weather state
        danger_level INTEGER           -- Danger level
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Temperature (
        id INTEGER PRIMARY KEY,        -- Primary key
        weather_state VARCHAR(100),            -- Weather state
        danger_level INTEGER           -- Danger level
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Rain (
        id INTEGER PRIMARY KEY,        -- Primary key
        weather_state VARCHAR(100),            -- Weather state
        danger_level INTEGER           -- Danger level
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Humidity (
        id INTEGER PRIMARY KEY,        -- Primary key
        weather_state VARCHAR(100),            -- Weather state
        danger_level INTEGER           -- Danger level
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Pressure (
        id INTEGER PRIMARY KEY,        -- Primary key
        weather_state VARCHAR(100),            -- Weather state
        danger_level INTEGER           -- Danger level
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Radiation (
        id INTEGER PRIMARY KEY,        -- Primary key
        weather_state VARCHAR(100),            -- Weather state
        danger_level INTEGER           -- Danger level
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Airpollution (
        id INTEGER PRIMARY KEY,        -- Primary key
        weather_state VARCHAR(100),            -- Weather state
        visibility_km INTEGER,         -- Visibility in km
        danger_level INTEGER,          -- Danger level
        dominant_pollutant VARCHAR(100)        -- Dominant pollutant
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS Observation (
        id INTEGER PRIMARY KEY,                -- Primary key
        location_id INTEGER NOT NULL,          -- Foreign key to Location
        date_id INTEGER NOT NULL,              -- Foreign key to Date
        time_id INTEGER NOT NULL,              -- Foreign key to Time
        wind_id INTEGER,                       -- Foreign key to Wind
        rain_id INTEGER,                       -- Foreign key to Rain
        pressure_id INTEGER,                   -- Foreign key to Pressure
        temperature_id INTEGER,                -- Foreign key to Temperature
        radiation_id INTEGER,                  -- Foreign key to Radiation
        humidity_id INTEGER,                   -- Foreign key to Humidity
        airpollution_id INTEGER,               -- Foreign key to Airpollution

        air_temperature_c FLOAT,                 -- Air Temperature (°C)
        min_temperature_c FLOAT,                 -- Hourly Minimum Temperature (°C)
        max_temperature_c FLOAT,                 -- Hourly Maximum Temperature (°C)

        wind_direction_deg FLOAT,                  -- 10-minute Average Wind Direction (°)
        avg_wind_speed_m_s FLOAT,                  -- 10-minute Average Wind Speed (m/s)
        max_wind_speed_m_s FLOAT,                  -- Hourly Maximum Wind Speed (m/s)

        solar_radiation_w_m2 FLOAT,                 -- Hourly Sum Solar Radiation (W/m²)
        sea_level_pressure_hPa FLOAT,              -- Sea Level Air Pressure (hPa)
        station_level_pressure_hPa FLOAT,          -- Station-Level Air Pressure (hPa)
        precipitation_mm FLOAT,                   -- Hourly Precipitation (mm)
        relative_humidity_pct FLOAT,               -- Relative Humidity (%)

        pm25_10_ug_m3 FLOAT,                         -- PM2.5-10 (µg/m³)
        pm25_ug_m3 FLOAT,                            -- PM2.5 (µg/m³)
        pm10_ug_m3 FLOAT,                            -- PM10 (µg/m³)
        so2_ug_m3 FLOAT,                             -- SO2 (µg/m³)
        no2_ug_m3 FLOAT,                             -- NO2 (µg/m³)
        o3_ug_m3 FLOAT,                              -- O3 (µg/m³)

        FOREIGN KEY(location_id) REFERENCES Location(id),
        FOREIGN KEY(date_id) REFERENCES Date(id),
        FOREIGN KEY(time_id) REFERENCES Time(id),
        FOREIGN KEY(wind_id) REFERENCES Wind(id),
        FOREIGN KEY(rain_id) REFERENCES Rain(id),
        FOREIGN KEY(pressure_id) REFERENCES Pressure(id),
        FOREIGN KEY(temperature_id) REFERENCES Temperature(id),
        FOREIGN KEY(radiation_id) REFERENCES Radiation(id),
        FOREIGN KEY(humidity_id) REFERENCES Humidity(id),
        FOREIGN KEY(airpollution_id) REFERENCES Airpollution(id)
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

        if not station_data.empty:
            print("Loading Location data into DuckDB...")
            location_data = station_data[['station_name', 'street', 'city', 'county', 'country',
                                          'latitude', 'longitude', 'elevation_m', 'radar_radius_km',
                                          'radar_frequency_band']].drop_duplicates()
            location_data['id'] = range(1, len(location_data) + 1)
            location_data = location_data[['id','station_name', 'street', 'city', 'county', 'country',
                                          'latitude', 'longitude', 'elevation_m', 'radar_radius_km',
                                          'radar_frequency_band']]
            print(location_data.head())
            con.execute("""INSERT INTO Location SELECT * FROM location_data""")

        print("Loading Wind data into DuckDB...")
        wind_data = pd.DataFrame([
            {'id': 1, 'weather_state': 'Calm', 'danger_level': 1},
            {'id': 2, 'weather_state': 'Moderate', 'danger_level': 1},
            {'id': 3, 'weather_state': 'Windy', 'danger_level': 2}
        ])
        print(wind_data.head())
        con.execute("INSERT INTO Wind SELECT * FROM wind_data")

        print("Loading Rain data into DuckDB...")
        rain_data = pd.DataFrame([
            {'id': 1, 'weather_state': 'No Rain', 'danger_level': 1},
            {'id': 2, 'weather_state': 'Light Rain', 'danger_level': 1},
            {'id': 3, 'weather_state': 'Heavy Rain', 'danger_level': 2}
        ])
        print(rain_data.head())
        con.execute("INSERT INTO Rain SELECT * FROM rain_data")

        print("Loading Pressure data into DuckDB...")
        pressure_data = pd.DataFrame([
            {'id': 1, 'weather_state': 'Stable', 'danger_level': 1},
            {'id': 2, 'weather_state': 'Low Pressure', 'danger_level': 1},
            {'id': 3, 'weather_state': 'Very Low Pressure', 'danger_level': 2}
        ])
        print(pressure_data.head())
        con.execute("INSERT INTO Pressure SELECT * FROM pressure_data")

        print("Loading Temperature data into DuckDB...")
        temperature_data = pd.DataFrame([
            {'id': 1, 'weather_state': 'Warm', 'danger_level': 1},
            {'id': 2, 'weather_state': 'Mild', 'danger_level': 1},
            {'id': 3, 'weather_state': 'Cold', 'danger_level': 2}
        ])
        print(temperature_data.head())
        con.execute("INSERT INTO Temperature SELECT * FROM temperature_data")

        print("Loading Radiation data into DuckDB...")
        radiation_data = pd.DataFrame([
            {'id': 1, 'weather_state': 'Low', 'danger_level': 1},
            {'id': 2, 'weather_state': 'Moderate', 'danger_level': 1},
            {'id': 3, 'weather_state': 'High', 'danger_level': 2}
        ])
        print(radiation_data.head())
        con.execute("INSERT INTO Radiation SELECT * FROM radiation_data")

        print("Loading Humidity data into DuckDB...")
        humidity_data = pd.DataFrame([
            {'id': 1, 'weather_state': 'Comfortable', 'danger_level': 1},
            {'id': 2, 'weather_state': 'Humid', 'danger_level': 2},
            {'id': 3, 'weather_state': 'Very Humid', 'danger_level': 3}
        ])
        print(humidity_data.head())
        con.execute("INSERT INTO Humidity SELECT * FROM humidity_data")

        if not airpollution_data.empty:
            print("Loading Airpollution data into DuckDB...")
            airpollution_meta = airpollution_data[['pm25_ug_m3', 'pm10_ug_m3', 'so2_ug_m3', 'no2_ug_m3', 'o3_ug_m3']].drop_duplicates()

            airpollution_meta['visibility_km'] = airpollution_meta['pm25_ug_m3'].apply(
                lambda x: 300 if x == 0 or pd.isna(x) else min(1000 / (x * 0.5), 300)
            )

            airpollution_meta['airpollution_details'] = airpollution_meta.apply(
                lambda row: get_airpollution({
                    'pm25': row.get('pm25_ug_m3'),
                    'pm10': row.get('pm10_ug_m3'),
                    'so2': row.get('so2_ug_m3'),
                    'no2': row.get('no2_ug_m3'),
                    'o3': row.get('o3_ug_m3'),
                    'visibility_km': row.get('visibility_km')
                }),
                axis=1
            )

            airpollution_meta['weather_state'] = airpollution_meta['airpollution_details'].apply(lambda x: x['weather_state'])
            airpollution_meta['danger_level'] = airpollution_meta['airpollution_details'].apply(lambda x: x['danger_level'])
            airpollution_meta['dominant_pollutant'] = airpollution_meta['airpollution_details'].apply(lambda x: x['dominant_pollutant'])
            airpollution_meta['visibility_km'] = airpollution_meta['airpollution_details'].apply(lambda x: x['visibility_km'])

            airpollution_meta = airpollution_meta[['weather_state', 'visibility_km', 'danger_level', 'dominant_pollutant']].drop_duplicates()

            airpollution_meta['id'] = range(1, len(airpollution_meta) + 1)

            airpollution_meta = airpollution_meta[['id', 'weather_state', 'visibility_km', 'danger_level', 'dominant_pollutant']].drop_duplicates()

            unknown_row = pd.DataFrame([{
                'id': -1,
                'weather_state': 'Unknown',
                'visibility_km': 300.0,
                'danger_level': 1,
                'dominant_pollutant': 'Unknown'
            }])

            airpollution_meta = pd.concat([airpollution_meta, unknown_row], ignore_index=True)

            print(airpollution_meta.head())
            con.execute("INSERT INTO Airpollution SELECT * FROM airpollution_meta")

        if not weather_data.empty and not airpollution_data.empty:
            print("Processing Date and Time data...")

            weather_data['datetime'] = pd.to_datetime(weather_data['year'].astype(str) + '-' +
                                                    weather_data['month'].astype(str) + '-' +
                                                    weather_data['day'].astype(str) + ' ' +
                                                    weather_data['time_utc'], format='%Y-%m-%d %H:%M')
            weather_data['date'] = weather_data['datetime'].dt.date
            weather_data['time'] = weather_data['datetime'].dt.time

            airpollution_data['date'] = pd.to_datetime(airpollution_data['date'], dayfirst=True).dt.date
            airpollution_data['time'] = pd.to_datetime('00:00', format='%H:%M').time()

            combined_data = pd.merge(weather_data, airpollution_data, on='date', how='left')

            date_data = pd.DataFrame(combined_data['date'].unique(), columns=['date'])
            date_data['year'] = pd.to_datetime(date_data['date']).dt.year
            date_data['month'] = pd.to_datetime(date_data['date']).dt.month
            date_data['day'] = pd.to_datetime(date_data['date']).dt.day

            all_holidays = set()
            for year in range(date_data['year'].min(), date_data['year'].max() + 1):
                all_holidays.update(get_estonian_holidays(year))

            date_data['holiday'] = date_data['date'].apply(lambda x: x in all_holidays).astype(int)
            date_data['workday'] = date_data['date'].apply(lambda x: x.weekday() < 5 and x not in all_holidays).astype(int)
            date_data['season'] = date_data['month'].apply(lambda x: 'Winter' if x in [12, 1, 2] else
                                                        'Spring' if x in [3, 4, 5] else
                                                        'Summer' if x in [6, 7, 8] else 'Autumn')
            date_data['quarter'] = pd.to_datetime(date_data['date']).dt.quarter
            date_data['id'] = range(1, len(date_data) + 1)

            date_table = date_data[['id', 'year', 'month', 'day', 'holiday', 'season', 'workday', 'quarter']]

            print(date_table.head())
            con.execute("""INSERT INTO Date SELECT * FROM date_table""")

            time_data = combined_data[['datetime']].drop_duplicates()
            time_data['hours'] = time_data['datetime'].dt.hour
            time_data['minutes'] = time_data['datetime'].dt.minute
            time_data['seconds'] = time_data['datetime'].dt.second
            time_data['time_of_day'] = time_data['hours'].apply(
                lambda x: 'Morning' if 6 <= x < 12 else
                        'Afternoon' if 12 <= x < 18 else
                        'Evening' if 18 <= x < 22 else
                        'Night'
            )
            time_data['office_hours'] = time_data['hours'].between(9, 17).astype(int)
            time_data['traffic_hour'] = time_data['hours'].isin(range(7, 10)) | time_data['hours'].isin(range(17, 20))
            time_data['traffic_hour'] = time_data['traffic_hour'].astype(int)

            time_data = time_data[['hours', 'minutes', 'seconds', 'time_of_day', 'office_hours', 'traffic_hour']].drop_duplicates()
            time_data['id'] = range(1, len(time_data) + 1)

            time_table = time_data[['id', 'hours', 'minutes', 'seconds', 'time_of_day', 'office_hours', 'traffic_hour']]
            print(time_table.head())

            con.execute("""INSERT INTO Time SELECT * FROM time_table""")

        print("Loading Observation data into DuckDB...")

        weather_data['datetime'] = pd.to_datetime(
            weather_data['year'].astype(str) + '-' +
            weather_data['month'].astype(str) + '-' +
            weather_data['day'].astype(str) + ' ' +
            weather_data['time_utc'], format='%Y-%m-%d %H:%M'
        )
        weather_data['date'] = weather_data['datetime'].dt.date
        weather_data['time'] = weather_data['datetime'].dt.time

        airpollution_data['derived_details'] = airpollution_data.apply(
            lambda row: get_airpollution({
                'pm25': row.get('pm25_ug_m3'),
                'pm10': row.get('pm10_ug_m3'),
                'so2': row.get('so2_ug_m3'),
                'no2': row.get('no2_ug_m3'),
                'o3': row.get('o3_ug_m3'),
                'visibility_km': row.get('visibility_km', 300)
            }),
            axis=1
        )

        airpollution_data['weather_state'] = airpollution_data['derived_details'].apply(lambda x: x['weather_state'])
        airpollution_data['danger_level'] = airpollution_data['derived_details'].apply(lambda x: x['danger_level'])
        airpollution_data['dominant_pollutant'] = airpollution_data['derived_details'].apply(lambda x: x['dominant_pollutant'])
        airpollution_data['visibility_km'] = airpollution_data['derived_details'].apply(lambda x: x['visibility_km'])

        airpollution_data.drop(columns=['derived_details'], inplace=True)

        airpollution_full_day = airpollution_data.copy()
        airpollution_full_day = airpollution_full_day.loc[airpollution_full_day.index.repeat(24)]
        airpollution_full_day['datetime'] = (
            pd.to_datetime(airpollution_full_day['date'], format='%Y-%m-%d', errors='coerce') +
            pd.to_timedelta(airpollution_full_day.groupby('date').cumcount(), unit='h')
        )

        airpollution_full_day = pd.merge(
            airpollution_full_day,
            airpollution_meta,
            how="left",
            on=['weather_state', 'visibility_km', 'danger_level', 'dominant_pollutant']
        )

        airpollution_full_day['date'] = airpollution_full_day['datetime'].dt.date
        airpollution_full_day['time'] = airpollution_full_day['datetime'].dt.time

        combined_data = pd.merge(weather_data, airpollution_full_day, on=['date', 'time'], how='left')

        combined_data['location_id'] = location_data['id'].iloc[0]
        combined_data['date_id'] = combined_data['date'].map(date_data.set_index('date')['id'])
        combined_data['time_id'] = combined_data.apply(
            lambda row: time_data.loc[
                (time_data['hours'] == row['time'].hour) &
                (time_data['minutes'] == row['time'].minute) &
                (time_data['seconds'] == row['time'].second),
                'id'
            ].iloc[0],
            axis=1
        )

        combined_data['airpollution_id'] = combined_data.apply(
            lambda row: (
                airpollution_meta.loc[
                    (airpollution_meta['weather_state'] == row['weather_state']) &
                    (airpollution_meta['visibility_km'] == row['visibility_km']) &
                    (airpollution_meta['danger_level'] == row['danger_level']) &
                    (airpollution_meta['dominant_pollutant'] == row['dominant_pollutant']),
                    'id'
                ].iloc[0]
                if not airpollution_meta.loc[
                    (airpollution_meta['weather_state'] == row['weather_state']) &
                    (airpollution_meta['visibility_km'] == row['visibility_km']) &
                    (airpollution_meta['danger_level'] == row['danger_level']) &
                    (airpollution_meta['dominant_pollutant'] == row['dominant_pollutant'])
                ].empty
                else -1
            ),
            axis=1
        )

        combined_data['wind_id'] = combined_data['avg_wind_speed_m_s'].apply(get_wind_id)
        combined_data['rain_id'] = combined_data['precipitation_mm'].apply(get_rain_id)
        combined_data['pressure_id'] = combined_data['sea_level_pressure_hPa'].apply(get_pressure_id)
        combined_data['temperature_id'] = combined_data['air_temperature_c'].apply(get_temperature_id)
        combined_data['radiation_id'] = combined_data['solar_radiation_w_m2'].apply(get_radiation_id)
        combined_data['humidity_id'] = combined_data['relative_humidity_pct'].apply(get_humidity_id)

        observation_data = combined_data[[
            'location_id', 'date_id', 'time_id',
            'wind_id', 'rain_id', 'pressure_id', 'temperature_id',
            'radiation_id', 'humidity_id', 'airpollution_id',
            'air_temperature_c', 'min_temperature_c', 'max_temperature_c',
            'wind_direction_deg', 'avg_wind_speed_m_s', 'max_wind_speed_m_s',
            'solar_radiation_w_m2', 'sea_level_pressure_hPa', 'station_level_pressure_hPa',
            'precipitation_mm', 'relative_humidity_pct', 'pm25_10_ug_m3', 'pm25_ug_m3', 'pm10_ug_m3',
            'so2_ug_m3', 'no2_ug_m3', 'o3_ug_m3'
        ]]
        observation_data['id'] = range(1, len(observation_data) + 1)

        observation_data = observation_data[[
            'id', 'location_id', 'date_id', 'time_id', 'wind_id', 'rain_id', 'pressure_id',
            'temperature_id', 'radiation_id', 'humidity_id', 'airpollution_id',
            'air_temperature_c', 'min_temperature_c', 'max_temperature_c',
            'wind_direction_deg', 'avg_wind_speed_m_s', 'max_wind_speed_m_s',
            'solar_radiation_w_m2', 'sea_level_pressure_hPa', 'station_level_pressure_hPa',
            'precipitation_mm', 'relative_humidity_pct', 'pm25_10_ug_m3', 'pm25_ug_m3', 'pm10_ug_m3',
            'so2_ug_m3', 'no2_ug_m3', 'o3_ug_m3'
        ]]

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