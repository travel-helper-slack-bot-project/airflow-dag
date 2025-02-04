from airflow import DAG
from airflow.decorators import task

from scripts.airline_ticket_crawling import *

from plugins.s3_to_snowflake import * 
from plugins.parquet_to_s3 import * 

from datetime import datetime



@task
def get_airline_crawling_data():
    return naver_airline_ticket_info_crawling()

@task
def parquet_to_s3(topic, s3_conn_id, replace, df):
    upload_parquet_to_s3(topic, s3_conn_id, replace, df)

@task
def s3_to_snowflake(topic,schema, table):
    load_s3_to_snowflake(topic, schema, table)

with DAG(
    dag_id = 'get_airline_information',
    start_date=datetime(2025, 1, 25),
    catchup= False, 
    tags = ['API'],
    schedule= '0 15 */3 * *'  
) as dag:

    topic = "airline"
    table = "slackbot_" + topic + "_info"
    schema = "raw_data"

    airline_df = get_airline_crawling_data()
    parquet_to_s3(topic,"aws_conn_id",True,airline_df) >> s3_to_snowflake(topic, schema, table)

