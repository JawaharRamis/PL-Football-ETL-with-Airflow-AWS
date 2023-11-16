# Airflow Football ETL Project

This project uses Apache Airflow for orchestrating an ETL (Extract, Transform, Load) process related to football data. The ETL process retrieves football-related information, uploads it to an S3 bucket, and triggers a Glue job for further processing.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [DAG Overview](#dag-overview)
- [Contributing](#contributing)
- [License](#license)

## Prerequisites

Before you begin, ensure you have the following dependencies installed:

- [Apache Airflow](https://airflow.apache.org/)
- [AWS CLI](https://aws.amazon.com/cli/)
- [Python](https://www.python.org/)

## Project Structure

/airflow-football-etl
|-- dags
| -- football_etl_dag.py |-- plugins | |-- operators | | -- upload_xcom_to_s3_operator.py
| |-- sensors
| | -- custom_sensor.py | -- init.py
|-- scripts
| -- foot_api_etl.py |-- README.md |-- requirements.txt -- .gitignore


- **dags:** Contains the Airflow DAG definition file.
- **plugins:** Custom Airflow operators and sensors.
- **scripts:** External scripts or modules used in the project.
- **README.md:** Project documentation.
- **requirements.txt:** Python dependencies.

## Getting Started

To get started with this project, follow these steps:

1. Clone the repository to your local machine:

   ```bash
   git clone [(https://github.com/JawaharRamis/PL-Football-ETL-with-Airflow-AWS.git)]
   ```

2. Change to the project directory:

3. Run the Docker Compose file to set up the project environment:

   ```bash
   docker-compose up -d
   ```
