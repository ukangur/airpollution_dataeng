# Airpollution Data Engineering project - Group 19

## Stage 1: How to start the code
First you should copy the repository using git:

`git clone https://github.com/ukangur/Airpollution_data_eng.git`

In addition you need access to the .env file. This contains credentials and encryption keys. You can request access to it through here: https://drive.google.com/file/d/1Bq-CaLHvpREixpbGSRcu9iJ1-KL4fMrL/view?usp=sharing. Then download it and add it to the directory you just cloned.

Now ensure that you have Docker Desktop running (and working). After this open a command line console and move to the corresponding directory. After you have done that run in the selected directory:

`docker compose up -d`

You will find that this starts all of the required Docker containers. Be patient - airflow takes a bit longer to initialize (approx. 1-2 minutes). After loading you will find the following web interfaces:

* airflow-webserver (for apache airflow) - localhost:8080
* mongo-express (for mongodb) - localhost:8081
* minio (for apache iceberg and duckdb) - localhost:9001

 ## Stage 2: Loading the initial datasets into MongoDB (EXTRACTING)

You can now access the airflow web interface and run the dag named extract_dag. This will do the following tasks:

1) Pull the weather dataset
2) Push weather dataset into MongoDB
3) Push airpollution dataset into MongoDB

Note: we do not pull the airpollution dataset as this was sent to us directly by EKUK (Eesti Keskkonnauuringute Keskus).

## Stage 3: Creating a combined dataset and pushing it to DuckDB (LOADING)

Run the dag named load_dag. This will do the following tasks:

1) Initialize the DuckDB database with all required tables and keys (as mentioned also in ER diagramm.docx)
2) Extract the raw data from MongoDB
3) Push the MongoDB raw data into our new DuckDB database tables

## Stage 3: Solving missing value issues, saving backups (TRANSFORMING)

Run the dag named transform_dag. This will do the following tasks:

1) Solves missing values for all DuckDB tables.
2) Creates snapshot for all DuckDB tables.
3) Combines DuckDB tables and saves encrypted parquet of it into MinIO S3.

Note: We follow a modified mean substitution method for solving missing values. As weather data (i.e. temperature) varies for different months it did not make sense to just to select average over all data. Rather we looked at what was the averages for features per month and substituted missing values using their representative monthly means.
