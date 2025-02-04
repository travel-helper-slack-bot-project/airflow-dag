from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable

from plugins.parquet_to_s3 import *
from plugins.s3_to_snowflake import *

from datetime import datetime
import pandas as pd
import logging
import requests

def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    conn = hook.get_conn()
    return conn.cursor()

@task
def get_data(key):
    url = f"https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={key}&data=AP01"
    result=requests.get(url,verify=False)
    data=result.json()
    list=[['cur_unit','cur_nm','kftc_deal_bas_r']]
    for i in data:
        cur_unit =i["cur_unit"]
        cur_nm =i["cur_nm"]
        kftc_deal_bas_r=float(i["kftc_deal_bas_r"].replace(",",""))
        list.append([cur_unit,cur_nm,kftc_deal_bas_r])
    
    df = pd.DataFrame(list[1:],columns=list[0])
    return df

@task
def parquet_to_s3(topic,s3_conn_id,replace, df):
    upload_parquet_to_s3(topic, s3_conn_id, replace, df)


@task
def s3_to_snowflake(topic,schema,table):
    load_s3_to_snowflake(topic, schema, table)

with DAG(
    dag_id = 'get_currency_information',
    start_date = datetime(2025, 1, 27),
    catchup=False,
    tags=['API'],
    schedule_interval='0 12 * * *'
) as dag:
    
    topic ="currency"
    table = "slackbot_" + topic + "_info"
    schema = "raw_data"

    currency_df = get_data(Variable.get("currency_api_key"))
    parquet_to_s3(topic, "aws_conn_id", True, currency_df) >>  s3_to_snowflake(topic, schema,table)
