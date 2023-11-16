from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from includes.football_api import retrieveStandings, retrieveFixtures, retrieveResults, retrieveAssister, retrieveScorer 
from includes.custom_operators.xcom_to_s3_operator import UploadXComToS3Operator
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

DAG_NAME = 'football_data_pipeline'


with DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval='@daily',
    description='Football Pipeline',
    catchup=False
) as dag:
    
    start_operator = EmptyOperator(
        task_id='start_operator',
        dag=dag,
    )

    with TaskGroup("retrieve_task_group", tooltip="task group #1") as retrieve_task_group:
    # with TaskGroup("Section_1",tooltip="TASK_GROUP_EV_description") as Section_1:
        retrieve_standings = PythonOperator(
            task_id='retrieve_standings',
            python_callable=retrieveStandings,
            do_xcom_push=True
        )

        retrieve_fixtures = PythonOperator(
            task_id='retrieve_fixtures',
            python_callable=retrieveFixtures,
            do_xcom_push=True
        )

        retrieve_results = PythonOperator(
            task_id='retrieve_results',
            python_callable=retrieveResults,
            do_xcom_push=True
        )

        retrieve_odds = PythonOperator(
            task_id='retrieve_scorers',
            python_callable=retrieveScorer,
            do_xcom_push=True
        )
        retrieve_odds = PythonOperator(
            task_id='retrieve_assisters',
            python_callable=retrieveAssister,
            do_xcom_push=True
        )

    with TaskGroup("upload_task_group", tooltip="task group #2") as upload_task_group:
        upload_standings_to_s3_task = UploadXComToS3Operator(
            task_id='upload_standings_to_s3_task',
            xcom_key='standings', 
            bucket='footballapi-data-bucket',  
            # key=f'standings/standings_{datetime.today().strftime("%Y-%m-%d")}.json',
            key=f'raw/standings/standings.json', 
            aws_conn_id='aws_conn',
        )

        upload_fixtures_to_s3_task = UploadXComToS3Operator(
            task_id='upload_fixtures_to_s3_task',
            xcom_key='fixtures', 
            bucket='footballapi-data-bucket',  
            key=f'raw/fixtures/fixtures.json', 
            aws_conn_id='aws_conn',
        )

        upload_results_to_s3_task = UploadXComToS3Operator(
            task_id='upload_results_to_s3_task',
            xcom_key='results', 
            bucket='footballapi-data-bucket',  
            key=f'raw/results/results.json', 
            aws_conn_id='aws_conn',
        )

        upload_scorers_to_s3_task = UploadXComToS3Operator(
            task_id='upload_scorers_to_s3_task',
            xcom_key='scorers', 
            bucket='footballapi-data-bucket',  
            key=f'raw/scorers/scorers.json', 
            aws_conn_id='aws_conn',
        )

        upload_assisters_to_s3_task = UploadXComToS3Operator(
            task_id='upload_assists_to_s3_task',
            xcom_key='assisters', 
            bucket='footballapi-data-bucket',  
            key=f'raw/assists/assists.json', 
            aws_conn_id='aws_conn',
        )

    submit_glue_job = GlueJobOperator(
        task_id="submit_glue_job",
        job_name="football-glue-job",
        script_location=f"s3://aws-glue-assets-521965996346-eu-central-1/scripts/foot_api_etl.py",
        s3_bucket="aws-glue-assets-521965996346-eu-central-1",
        iam_role_name="AWSGlueServiceRole-kafka-weather-glue-role",
        region_name="eu-central-1",
        aws_conn_id='aws_conn'
        create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
    )

    end_operator = EmptyOperator(
        task_id='end_operator',
        dag=dag,
    )

    start_operator>>retrieve_task_group>>upload_task_group>>submit_glue_job>>end_operator

