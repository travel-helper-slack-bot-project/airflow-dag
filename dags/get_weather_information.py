from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from datetime import datetime, timedelta
from plugins.s3_to_snowflake import * 
from plugins.parquet_to_s3 import * 

import pandas as pd

import requests
import logging
import time


def get_snowflake_connection():
    # autocommit is False by default
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()


@task
def get_lat_lon():
    cur = get_snowflake_connection()
    select_sql = "SELECT row_num, lat, lon FROM raw_data.get_city_info;"

    try: 
        cur.execute(select_sql)
        result = cur.fetchall()
        if not result:
            raise ValueError("No city data found in 'yeojun.get_city_info' table.")
        return result
    except Exception as e:
        logging.error(f"Error fetching data from Snowflake: {e}")
        raise
    finally:
        cur.close()


@task
def et(lat_lon_list):

    """
    {"cod":"200","message":0,"cnt":40,"list":[
    
    {"dt":1661871600,"main":{"temp":296.76,"feels_like":296.98,"temp_min":296.76,"temp_max":297.87,"pressure":1015,"sea_level":1015,"grnd_level":933,"humidity":69,"temp_kf":-1.11},"weather":[{"id":500,"main":"Rain","description":"light rain","icon":"10d"}],"clouds":{"all":100},"wind":{"speed":0.62,"deg":349,"gust":1.18},"visibility":10000,"pop":0.32,"rain":{"3h":0.26},"sys":{"pod":"d"},"dt_txt":"2022-08-30 15:00:00"},
    
    {"dt":1662292800,"main":{"temp":294.93,"feels_like":294.83,"temp_min":294.93,"temp_max":294.93,"pressure":1018,"sea_level":1018,"grnd_level":935,"humidity":64,"temp_kf":0},"weather":[{"id":804,"main":"Clouds","description":"overcast clouds","icon":"04d"}],"clouds":{"all":88},"wind":{"speed":1.14,"deg":17,"gust":1.57},"visibility":10000,"pop":0,"sys":{"pod":"d"},"dt_txt":"2022-09-04 12:00:00"}

    ],"city":{"id":3163858,"name":"Zocca","coord":{"lat":44.34,"lon":10.99},"country":"IT","population":4593,"timezone":7200,"sunrise":1661834187,"sunset":1661882248}}
    """

    api_key = Variable.get("open_weather_api_key")

    # https://openweathermap.org/forecast5
    city_info = []
    for row_num, lat, lon in lat_lon_list:
        url = f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}"

        try:
            response = requests.get(url, timeout=10)  # 10초 타임아웃 설정
            response.raise_for_status()  # HTTP 오류 발생 시 예외 발생
            data = response.json()
            
            if "city" not in data:
                raise ValueError(f"Invalid API response for lat: {lat}, lon: {lon}")

            for d in data.get("list", []):  # list 키가 없을 수도 있으므로 안전하게 가져옴
                temperature = d['main'].get('temp', 'NULL')  # 기본값 설정
                weather_condition = d['weather'][0].get('main', 'Unknown')
                cloudiness = d['clouds'].get('all', 0)  # 기본값 0
                precipitation = d.get('pop', 0)  # 기본값 0
                timestamp = d.get('dt_txt', 'Unknown')

                city_info.append({
                    "row_num":row_num,
                    "temp": temperature,
                    "weather": weather_condition,
                    "clouds": cloudiness,
                    "pop": precipitation,
                    "datetime": timestamp
                })


            # API Rate Limit을 고려한 딜레이 추가 (1초 대기)
            time.sleep(1)

        except requests.exceptions.RequestException as e:
            logging.error(f"API Request failed: {e}")
            raise
        except KeyError as e:
            logging.error(f"Missing expected key in API response: {e}")
            raise
    
    df = pd.DataFrame(city_info)
    return df

@task
def parquet_to_s3(topic, s3_conn_id, replace, df):
    upload_parquet_to_s3(topic, s3_conn_id, replace, df)

@task
def s3_to_snowflake(topic,schema, table):
    load_s3_to_snowflake(topic, schema, table)


with DAG(
    dag_id = 'get_weather_information',
    start_date = datetime(2025,1,1), # 날짜가 미래인 경우 실행이 안됨
    schedule = '30 15 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    tags=['API'],
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    
    topic = "weather"
    table = "slackbot_"+ topic +"_info"
    schema = "raw_data"

    lat_lon_list = get_lat_lon()
    city_df = et(lat_lon_list)
    parquet_to_s3(topic, "aws_conn_id", True, city_df) >>  s3_to_snowflake(topic, schema, table)
    

