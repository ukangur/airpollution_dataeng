# Airpollution Data Engineering project - Group 19

## Project concept figure
![project figure image](DE_graph.png?raw=true "Title")

## Database schema
![Database schema image](db_schema.png?raw=true "Title")

## Stage 1 - INITIALIZING: How to start the code
**learned skills used:** Docker, privacy/security (safe handling of private keys)

First you should copy the repository using git:

`git clone https://github.com/ukangur/Airpollution_data_eng.git`

In addition you need access to the .env file. This contains credentials and encryption keys. You can request access to it through here: https://drive.google.com/file/d/1Bq-CaLHvpREixpbGSRcu9iJ1-KL4fMrL/view?usp=sharing. Then download it and add it to the directory you just cloned.

Now ensure that you have Docker Desktop running (and working). After this open a command line console and move to the corresponding directory. After you have done that run in the selected directory:

`docker compose up -d`

You will find that this starts all of the required Docker containers. Be patient - airflow takes a bit longer to initialize (approx. 1-2 minutes). After loading you will find the following web interfaces:

* airflow-webserver (for apache airflow) - localhost:8080
* mongo-express (for mongodb) - localhost:8081
* minio (for apache iceberg and duckdb) - localhost:9001

 ## Stage 2 - EXTRACT: Loading the initial datasets into MongoDB
**learned skills used:** MongoDB, data governance (ensuring accuracy and timeliness)

You can now access the airflow web interface and run the dag named extract_dag. This will do the following tasks:

1) Pull the weather dataset
2) Push weather dataset into MongoDB
3) Push airpollution dataset into MongoDB

Note: we do not pull the airpollution dataset as this was sent to us directly by EKUK (Eesti Keskkonnauuringute Keskus).

## Stage 3 - LOAD: Creating a combined dataset and pushing it to DuckDB
**learned skills used:** MongoDB, DuckDB, data governance (ensuring orderliness and uniqueness)

Run the dag named load_dag. This will do the following tasks:

1) Initialize the DuckDB database with all required tables and keys (as mentioned also in the schema image)
2) Extract the raw data from MongoDB
3) Push the MongoDB raw data into our new DuckDB database tables

Since the air pollution data only provides a single measurement for each day, we duplicate that daily data across all hours of the day. This ensures that the air pollution data matches the same level of detail (hourly) as the weather data.

## Stage 4 - TRANSFORM: Solving missing value issues, saving backups.
**learned skills used:** DuckDB, dbt, Minio (S3), Apache Iceberg, privacy/security (saving encrypted backup), data governance (ensuring completeness)

Run the dag named transform_dag. This will do the following tasks:

1) Solves missing values for all Observation fact table.
2) Creates snapshot for Observation fact table. This allows us to follow changes in case we get data for more recent years in the future.
3) Saves encrypted parquet of Observation table into MinIO S3. This works as a backup in case something might happen to the main database. 

Note: We follow a modified mean substitution method for solving missing values. As weather data (i.e. temperature) varies for different months it did not make sense to just to select average over all data. Rather we looked at what was the averages for features per month and substituted missing values using their representative monthly means.
