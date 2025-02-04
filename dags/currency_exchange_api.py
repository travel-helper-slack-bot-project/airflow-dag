import logging
import requests
import json
import csv
import time
import io
from datetime import datetime

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.operators.python import get_current_context

default_args = {
    'owner': 'eunzy'
}

@task
def extract_currency_exchange() -> str:
    category = '환전'
    url = 'https://dapi.kakao.com/v2/local/search/keyword.json'
    # airflow에 저장된 variable을 통해 key를 받아옴
    api_key = Variable.get("kakaomap_api_key")
    headers = {"Authorization": f"KakaoAK {api_key}"}
    input_csv = '/opt/airflow/data/korea_administrative_regions.csv'
    output_header = ['address_name', 'phone', 'place_name', 'place_url', 'road_address_name']


    # 메모리 내 csv 저장을 위해 StingIO 사용
    output_buffer = io.StringIO()
    writer = csv.writer(output_buffer)
    writer.writerow(output_header)


    # 행정정보 csv파일 가져오기
    with open(input_csv, 'r', encoding='utf-8') as input:
        reader = csv.reader(input)
        # 첫번째 행(열) 건너뛰기
        header = next(reader, None)
        for row in reader:
            region = [i for i in row if i]
            query = '||'.join(region) + f'||{category}'
            params = {'query': query}
            
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()

                # 데이터 가져오기
                json_data = response.json()
                documents = json_data.get('documents', [])
                
                if documents:
                    first_doc = documents[0]

                    address_name = first_doc.get('address_name', 'N/A')
                    phone = first_doc.get('phone', 'N/A')
                    place_name = first_doc.get('place_name', 'N/A')
                    place_url = first_doc.get('place_url', 'N/A')
                    road_address_name = first_doc.get('road_address_name', 'N/A')

            except requests.exceptions.RequestException as e:
                print(f'API 요청 실패: {e}')
                address_name = phone = place_name = place_url = road_address_name = 'N/A'
                continue

            output_row = [address_name, phone, place_name, place_url, road_address_name]
            writer.writerow(output_row)
            print(f"Processed: {output_row}")
            time.sleep(0.5)
    
    csv_content = output_buffer.getvalue()
    output_buffer.close()
    return csv_content    
    

@task
def upload_csv_to_s3(csv_content: str) -> str:
    # Airflow context에서 datetime object를 받아 date string으로 변환
    context = get_current_context()
    ds_nodash = context['ds_nodash']

    bucket_name = Variable.get('s3_bucket')
    bucket_object = 'project3'
    s3_key = f'{bucket_object}/{ds_nodash}/currency_exchange_{ds_nodash}.csv'

    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    s3_hook.load_string(
        string_data = csv_content,
        bucket_name = bucket_name,
        key = s3_key,
        replace=True
    )

    s3_path = f's3://{bucket_name}/{s3_key}'
    print("S3 업로드 성공")
    return s3_path


@task
def load_data_into_snowflake(s3_path: str) -> str:

    database = 'third_project'
    schema = 'raw_data'
    table_name = 'currency_exchange'
    staging_table = f'{table_name}_staging'
    aws_key_id = Variable.get('snowflake_s3_access_key')
    aws_secret_key = Variable.get('snowflake_s3_secret_access_key')
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_db')

    try:

        copy_sql = f"""
        
        CREATE DATABASE IF NOT EXISTS {database};
        CREATE SCHEMA IF NOT EXISTS {database}.{schema};
        
        BEGIN TRANSACTION;

        -- 기존 최종 테이블과 임시 테이블이 존재하면 삭제
        DROP TABLE IF EXISTS {schema}.{table_name};
        DROP TABLE IF EXISTS {schema}.{staging_table};
        
        -- 스테이징 테이블 생성 (CSV 원본 데이터를 그대로 적재)
        CREATE TABLE {schema}.{staging_table} (
            address_name VARCHAR,
            phone VARCHAR,
            place_name VARCHAR,
            place_url VARCHAR,
            road_address_name VARCHAR
        );
        
        -- S3에 있는 CSV 파일을 스테이징 테이블에 로드 (SKIP_HEADER=1 옵션으로 헤더 건너뛰기)
        COPY INTO {schema}.{staging_table}
        FROM '{s3_path}'
        CREDENTIALS=(AWS_KEY_ID='{aws_key_id}' AWS_SECRET_KEY='{aws_secret_key}')
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        );
        
        -- 스테이징 테이블에서 중복을 제거하여 최종 테이블 생성
        CREATE TABLE {schema}.{table_name} AS
        SELECT DISTINCT *
        FROM {schema}.{staging_table};
        
        -- 임시 스테이징 테이블 삭제
        DROP TABLE IF EXISTS {schema}.{staging_table};
        
        COMMIT;
        """
        
        snowflake_hook.run(copy_sql)
        print("Snowflake 적재 완료")
    except Exception as e:
        logging.info(f"Snowflake 적재 실패: {e}")
        snowflake_hook.run('ROLLBACK;')

with DAG(
    dag_id='currency_exchange',
    schedule_interval='@once', # DAG 수동 실행
    start_date=datetime(2025, 2, 3), 
    catchup=False,
    tags=['currency_exchange'],
    default_args=default_args
) as dag:
    currency_exchange_data = upload_csv_to_s3(extract_currency_exchange())
    load_data_into_snowflake(currency_exchange_data)
