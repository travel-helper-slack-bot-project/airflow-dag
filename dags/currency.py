from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
from datetime import timedelta
#from pandas import Timestamp
import pandas as pd
import logging
import requests
from airflow.operators.python import get_current_context


def get_snowflake_connection(autocommit=True):
    hook=SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()



def get_data(**context):
    today=datetime.now().strftime("%Y%m%d")
    key= context["params"]["api_key"]
    url=f"https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={key}&searchdate={today}&data=AP01"
    result=requests.get(url,verify=False)
    data=result.json()
    data=pd.DataFrame(data)
    data=data[["cur_unit", "cur_nm", "kftc_deal_bas_r"]]
    data["kftc_deal_bas_r"]=data["kftc_deal_bas_r"].str.replace(",", "")
    csv_data=data.to_csv(index=False, encoding='utf-8')
    return csv_data
    
    
def upload_to_s3(**context):
    ti=context["task_instance"]
    csv_data=ti.xcom_pull(task_ids="get_data")
    date = context['ds_nodash']
    hook = S3Hook(aws_conn_id='AWS_CONN_id')
    try:
        hook.load_string(string_data=csv_data, key=f"currency/data_{date}.csv", bucket_name="byairflow",replace=True)
        print("s3업로드 완료")
        
    except Exception as error:
        print(error)
        raise
        



def s3_to_snowflake(**context):
    AWS_ACCESS_KEY=context["params"]["AWS_ACCESS_KEY"]
    AWS_SECRET_ACCESS_KEY=context["params"]["AWS_SECRET_ACCESS_KEY"]
    schema=context["params"]["schema"]
    table=context["params"]["table"]
    cur = get_snowflake_connection()
    date = context['ds_nodash']
    try:
        cur.execute("begin;")

        
        cur.execute(f"""
        create temporary table {schema}.t(
        
        cur_unit VARCHAR(10) PRIMARY KEY,
        cur_nm VARCHAR(50),
        kftc_deal_bas_r DECIMAL(10,2)
     
        
        );
        """)
        
        cur.execute(f"""
        COPY INTO project.{schema}.t
        FROM 's3://byairflow/currency/data_{date}.csv'
        credentials=(AWS_KEY_ID='{AWS_ACCESS_KEY}' AWS_SECRET_KEY='{AWS_SECRET_ACCESS_KEY}')
        FILE_FORMAT = (type='CSV' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');
    
        """)
        
        
        cur.execute(f"""create or replace table {schema}.{table} as select cur_unit, cur_nm, kftc_deal_bas_r from {schema}.t;""")
        cur.execute("COMMIT;")
        print(f"Data copied")
    except Exception as error:
        print(error)
        cur.execute("rollback;")
        raise
        




dag = DAG(
    dag_id = 'currency_information',
    start_date = datetime(2025, 1, 27), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 2 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    tags=['API'],
    default_args = {
#        'retries': 1,
#        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
)






get_data = PythonOperator(
    task_id = 'get_data',
    python_callable = get_data,
    params = {
    'api_key': Variable.get("currency_api_key")
    
    },
    dag = dag)
    

upload_to_s3 = PythonOperator(
    task_id = 'upload_to_s3',
    python_callable = upload_to_s3,
    params = {
    },
    dag = dag)
    
    
s3_to_snowflake = PythonOperator(
    task_id = 's3_to_snowflake',
    python_callable = s3_to_snowflake,
    params = {
    'AWS_ACCESS_KEY':Variable.get("AWS_ACCESS_KEY"),
    'AWS_SECRET_ACCESS_KEY':Variable.get("AWS_SECRET_ACCESS_KEY"),
    'schema':"jaiwoo",
    'table':"currency"
    
    },
    dag = dag)
    
    
    
    



get_data >> upload_to_s3 >> s3_to_snowflake
