from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from scripts.airline_ticket_crawling import *
from scripts.upload_to_s3 import * 
from scripts.insert_snowflake_from_s3 import *


dag = DAG(
    dag_id = 'dataCrawlingTest',
    start_date=datetime(2025, 1, 25),
    catchup= False, 
    tags = ['first_example'],
    schedule= '0 10 */3 * *'  #3일마다, 매일 10시에 
)

#data crawling
airline_ticket_crawling_task = PythonOperator(
    task_id = 'naver_data_crawling',
    python_callable= naver_airline_ticke_info_crawling,
    dag = dag
)

#data upload to s3
upload_to_bucket = PythonOperator(
    task_id = 'bucket_upload',
    python_callable= load_csv_to_bucket,
    dag = dag
)

#s3 copy into snowflake
copy_into_snowflake = PythonOperator(
    task_id = 'datawarehouse_upload',
    python_callable= upload_data_in_table,
    dag = dag
)

airline_ticket_crawling_task >> upload_to_bucket >> copy_into_snowflake

#upload_to_bucket >> copy_into_snowflake


