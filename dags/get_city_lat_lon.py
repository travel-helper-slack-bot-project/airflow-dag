from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta

import requests
import logging
import json


def get_snowflake_connection():
    # autocommit is False by default
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()


@task
def et(**kwargs):
    api_key = Variable.get("open_weather_api_key")
    city_name = kwargs.get('dag_run').conf.get('city_name', 'seoul')
    print(f"Received city:{city_name}")

    # https://openweathermap.org/api/geocoding-api
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name}&appid={api_key}"
    response = requests.get(url)
    data = json.loads(response.text)

    if not data:
        raise ValueError(f"City '{city_name}' not found")

    data = data[0]

    """
    {
        "name":"London",
        "local_names":{
            "ms":"London",
            "gu":"લંડન",
            "is":"London",
            "wa":"Londe",
            "mg":"Lôndôna",
            ---
            "rm":"Londra",
            "ff":"London",
            "kk":"Лондон",
            "uk":"Лондон",
            "pt":"Londres",
            "tr":"Londra",
            "eu":"Londres",
            "ht":"Lonn",
            "ka":"ლონდონი",
            "ur":"علاقہ لندن"
        },
        "lat":51.5073219,
        "lon":-0.1276474,
        "country":"GB",
        "state":"England"
    }
    """
    
    city_info = (
        data['name'],
        data['local_names'].get('ko', 'N/A'),
        data['lat'],
        data['lon'],
        data['country'],
        data.get('state', 'N/A')
    )
    return city_info




@task
def load(city_info, schema, table):
    cur = get_snowflake_connection()
    
    # 원본 테이블이 없다면 생성
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id INT,
        name_eng VARCHAR NOT NULL,
        name_ko VARCHAR,
        lat DOUBLE,
        lon DOUBLE,
        country VARCHAR,
        state VARCHAR,
        get_date TIMESTAMP default GETDATE()
    );"""
    
    # 임시 테이블 생성
    create_temp_table_sql = f"""
    CREATE TEMPORARY TABLE t AS
    SELECT 
        name_eng, name_ko, lat, lon, 
        country, state, get_date 
    FROM {schema}.{table};
    """

    logging.info(create_table_sql)
    logging.info(create_temp_table_sql)

    try:
        cur.execute(create_table_sql)
        cur.execute(create_temp_table_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    

    # 중복 데이터 삭제(기존 데이터를를)
    delete_sql = f"""
    DELETE FROM t
    WHERE name_eng ='{city_info[0]}' AND country = '{city_info[4]}';
    """
    logging.info(delete_sql)

    try:
        cur.execute(delete_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 새로운 데이터 삽입
    insert_sql = f"""
    INSERT INTO t 
    VALUES (%s, %s, %s, %s, %s, %s, GETDATE());
    """
    logging.info(insert_sql)

    try:
        cur.execute(insert_sql,city_info)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # ROW_NUMBER를 이용해서 컬럼 추가하고 CTAS로 테이블 생성 
    replace_sql = f"""
    CREATE OR REPLACE TABLE {schema}.{table} AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY get_date ASC) AS row_num,
        name_eng,
        name_ko,
        lat,
        lon,
        country,
        state,
        get_date
    FROM t;
    """

    logging.info(replace_sql)

    try:
        cur.execute(replace_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    

with DAG(
    dag_id = 'get_city_lat_lon',
    start_date = datetime(2025,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule = None,  # 적당히 조절
    is_paused_upon_creation=True,
    max_active_runs = 1,
    catchup = False,
) as dag:

    city_info = et()
    load(city_info, 'yeojun', 'get_city_info')
    
    
