from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io

import logging
from datetime import datetime

def upload_parquet_to_s3(topic, s3_conn_id, replace, df):
    """
    Upload all of the files to S3
    """
    
    # 오늘 날짜
    today_date = str(datetime.now().strftime("%Y%m%d"))

    # 저장할 S3경로 설정
    s3_bucket = "yeojun-test-bucket"
    s3_key = f"slackbot/{topic}/{today_date}/{topic}_info.parquet"

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)

    # Airflow S3Hook 객체 생성
    s3_hook = S3Hook(s3_conn_id)

    # S3에 Parquet 파일 업로드
    s3_hook.load_bytes(
        bytes_data = parquet_buffer.getvalue(),
        bucket_name = s3_bucket,
        key=s3_key,
        replace = replace
    )

    print(f"Parquet 데이터가 S3 {s3_key}경로에 저장되었습니다.")

