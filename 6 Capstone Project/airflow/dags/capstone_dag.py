from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)
#from helpers import SqlQueries


# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Julia Markina',
    'start_date': datetime(2019,1,1),
    'depends_on_past': False,
#    'retries': 0,
#    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('data_engineer_capstone',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup = False
        )

#start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)



immigration_to_redshift = StageToRedshiftOperator(
    task_id='immigration_fact',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema="public",
    table="immigration",
    s3_bucket="s3-data-engineer-capstone",
    s3_key="output-data",
    s3_prefix = "immigration.parquet",
    options = ["FORMAT AS PARQUET"]
)

country_to_redshift = StageToRedshiftOperator(
    task_id='country_dim',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema="public",
    table="country",
    s3_bucket="s3-data-engineer-capstone",
    s3_key="output-data",
    s3_prefix = "country.parquet",
    options = ["FORMAT AS PARQUET"]
)

state_to_redshift = StageToRedshiftOperator(
    task_id='state_dim',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema="public",
    table="state",
    s3_bucket="s3-data-engineer-capstone",
    s3_key="output-data",
    s3_prefix = "state.parquet",
    options = ["FORMAT AS PARQUET"]
)

date_to_redshift = StageToRedshiftOperator(
    task_id='date_dim',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    schema="public",
    table="date",
    s3_bucket="s3-data-engineer-capstone",
    s3_key="output-data",
    s3_prefix = "date.parquet",
    options = ["FORMAT AS PARQUET"]
)



run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['immigration','country','state','date']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


#start_operator >> immigration_to_redshift

immigration_to_redshift >> country_to_redshift
immigration_to_redshift >> state_to_redshift
immigration_to_redshift >> date_to_redshift

country_to_redshift >> run_quality_checks
state_to_redshift >> run_quality_checks
date_to_redshift >> run_quality_checks

run_quality_checks >> end_operator


