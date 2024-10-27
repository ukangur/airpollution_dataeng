# Airpollution Data Engineering project - Group 19

## Stage 1: How to start the code
First you should copy the repository using git:

`git clone https://github.com/ukangur/Airpollution_data_eng.git`

Now ensure that you have Docker Desktop running (and working). After this open a command line console and move to the corresponding directory. After you have done that run in the selected directory:

`docker compose up -d`

You will find that this starts all of the required Docker containers. You will find the following web interfaces:

* airflow-webserver (for apache airflow) - localhost:8080
* mongo-express (for mongodb) - localhost:8081
* minio (for apache iceberg and duckdb) - localhost:9001

 ## Stage 2: Loading the initial datasets into MongoDB

You can now access the airflow web interface and run the dag named main_dag. This will do the following tasks:

1) Pull the weather dataset
2) Push weather dataset into MongoDB
3) Push airpollution dataset into MongoDB

Note that we do not pull the airpollution dataset as this was sent to us directly by EKUK (Eesti Keskkonnauuringute Keskus).

## Stage 3: Creating a combined dataset and pushing it to DuckDB

TODO...
