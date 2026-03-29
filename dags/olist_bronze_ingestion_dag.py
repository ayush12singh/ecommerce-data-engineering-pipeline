from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
from datetime import datetime, timedelta
import subprocess,pytz,sys,os

print(f"Current working directory: {os.listdir('/opt/airflow')}")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl_codes.upload_to_s3 import upload_data_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1, tzinfo=pytz.timezone('Asia/Kolkata')),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG('olist_bronze_ingestion_dag', default_args=default_args, schedule=None,catchup=False) as dag:

    start = EmptyOperator(task_id='start')
    
    @task
    def download_from_kaggle():
        result = subprocess.run(
            ["kaggle","datasets","download","-d","olistbr/brazilian-ecommerce","-p","/opt/airflow/data","--unzip",],check=False,capture_output=True,text=True)
        return result.returncode
    
    @task.branch
    def check_download_status(return_code):
        if return_code == 0:
            return "upload_to_s3_task"
        else:
            return "end"
    
    @task
    def upload_to_s3_task():

        # result = subprocess.run(
            # ["aws","s3","cp","/opt/airflow/data/","s3://olist-datalake-raw/olist/","--recursive"],check=True,capture_output=True,text=True)
        return upload_data_to_s3()
    
    end = EmptyOperator(task_id='end')
    
    return_code = download_from_kaggle()
    decision = check_download_status(return_code)
    upload_task  = upload_to_s3_task()
    start >> return_code >> decision
    decision >> [upload_task,end]
    upload_task >> end