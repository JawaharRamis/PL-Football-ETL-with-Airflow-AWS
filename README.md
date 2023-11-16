# PL-Football-ETL-with-Airflow-AWS

This project uses Apache Airflow for orchestrating an ETL (Extract, Transform, Load) process related to Premier League football data. The ETL process retrieves stats and other information via an API, uploads it to an S3 bucket, and triggers a Glue job for further processing and finally visualised in AWS Quicksight.

Table of Contents
- [Prerequisites]
- [Project Structure]
- [Installation]
- [Usage]
- [DAG Overview]
- [Contributing]

Before you begin, ensure you have the following dependencies installed:

Apache Airflow
AWS CLI
Python
Project Structure
plaintext
Copy code
/airflow-football-etl
|-- dags
|   `-- football_etl_dag.py
|-- plugins
|   |-- operators
|   |   `-- upload_xcom_to_s3_operator.py
|   |-- sensors
|   |   `-- custom_sensor.py
|   `-- __init__.py
|-- scripts
|   `-- foot_api_etl.py
|-- README.md
|-- requirements.txt
`-- .gitignore
dags: Contains the Airflow DAG definition file.
plugins: Custom Airflow operators and sensors.
scripts: External scripts or modules used in the project.
README.md: Project documentation.
requirements.txt: Python dependencies.
Installation
Clone the repository:

bash
Copy code
git clone https://github.com/yourusername/airflow-football-etl.git
cd airflow-football-etl
Install the required Python packages:

bash
Copy code
pip install -r requirements.txt
Set up Airflow according to your environment. Refer to the Airflow documentation for guidance.

Usage
Configure your AWS credentials using the AWS CLI:

bash
Copy code
aws configure
Start the Airflow web server and scheduler:

bash
Copy code
airflow webserver --port 8080
airflow scheduler
Access the Airflow web interface (http://localhost:8080 by default) to trigger and monitor the DAG.

DAG Overview
The main DAG file (dags/football_etl_dag.py) orchestrates the ETL process. It consists of several tasks, including retrieving data, uploading to S3, and triggering a Glue job. The tasks are organized into task groups for clarity.

The DAG execution flow:

swift
Copy code
start_operator >> retrieve_task_group >> upload_task_group >> submit_glue_job >> end_operator
start_operator: Initializes the DAG.
retrieve_task_group: Task group for retrieving football data.
upload_task_group: Task group for uploading data to S3.
submit_glue_job: Submits a Glue job for further processing.
end_operator: Marks the end of the DAG execution.
Contributing
Contributions are welcome! Feel free to open issues or submit pull requests.
