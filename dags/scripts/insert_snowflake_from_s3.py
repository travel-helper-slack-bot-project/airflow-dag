import snowflake.connector
import pandas as pd
import logging
import os

from dotenv import load_dotenv
from io import StringIO
from datetime import datetime
from airflow.models import Variable

"""
   1. snowflake 연결
   2. 생성했던 s3 스테이지로 csv 파일 읽어오기
   3. COPY INTO로 테이블에 적재
"""

load_dotenv()

SNOWFLAKE_USER = Variable.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'AIRLINE_INFO'
SNOWFLAKE_SCHEMA = 'TICKET_INFO'


### snowflake 연결
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user = SNOWFLAKE_USER,
        password = SNOWFLAKE_PASSWORD,
        account = SNOWFLAKE_ACCOUNT,
        warehouse = SNOWFLAKE_WAREHOUSE,
        database = SNOWFLAKE_DATABASE,
        schema = SNOWFLAKE_SCHEMA
    )
    return conn


### snowfalke로 데이터 업로드 (Full Refresh 구현)
def upload_data_in_table():
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    conn = connect_to_snowflake()
    cur = conn.cursor()
    
    try:
        #트랜잭션 시작
        cur.execute("BEGIN");
        
        #만약 테이블이 있다면 테이블을 삭제
        cur.execute("DROP TABLE IF EXISTS airline_ticket;")
        
        #테이블 생성 
        sql = """
        CREATE TABLE airline_ticket(
        id INT AUTOINCREMENT,
        ticket_date DATE,
        destination VARCHAR(50),
        cityName VARCHAR(50),
        airline VARCHAR(100),
        price INT)
        """
        cur.execute(sql)
        
        #테이블에 데이터 넣기
        sql = f"""
        COPY INTO airline_ticket (ticket_date, destination, cityName ,airline,price)
                FROM @third_project_stage/airline_crawling_data_{today}.csv 
                FILE_FORMAT = (
                    TYPE = CSV,
                    FIELD_OPTIONALLY_ENCLOSED_BY='"',  
                    SKIP_HEADER=1  
                )
                ON_ERROR = CONTINUE;
        """
        
        cur.execute(sql)
        
        #트랜잭션 완료
        cur.execute("COMMIT");
        
        print("데이터 업로드 완료")
        
        conn.commit()
    
    except Exception as e:
        logging.info(f"error: {e}")
        cur.execute("ROLLBACK;")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    upload_data_in_table()
    
    

