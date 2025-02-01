import boto3

import os
from datetime import datetime
from dotenv import load_dotenv
from airflow.models import Variable

load_dotenv()

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


def conn_to_s3():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key = AWS_SECRET_ACCESS_KEY
    )
    
    return s3_client

def load_csv_to_bucket():
    today = datetime.now().strftime("%Y-%m-%d")
    s3_client = conn_to_s3()
    file_path = f'data/airline_crawling_data_{today}.csv'
    bucket_name = 'yr-s3-project'
    #s3_key = 'thrid_project_airticket'
    object_name = f"thrid_project_airticket/airline_crawling_data_{today}.csv"
    
    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
    
    except Exception as e:
        print(f"error:{e}")
        
        
if __name__ == "__main__":
    load_csv_to_bucket()