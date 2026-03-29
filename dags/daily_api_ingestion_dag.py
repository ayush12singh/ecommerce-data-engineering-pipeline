from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pytz,sys,os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 8, tzinfo=pytz.timezone('Asia/Kolkata')),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG('daily_api_ingestion_dag', default_args=default_args, schedule='@daily',catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    daily_ingestion = BashOperator(task_id='daily_ingestion',bash_command="/usr/local/bin/python /opt/airflow/etl_codes/ingest_daily_data.py")
    end = EmptyOperator(task_id='end')

    start >> daily_ingestion >> end