from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import logging
from datetime import datetime

def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_dev_db')
    return hook.get_conn().cursor()

@task
def elt(extract_schema, load_schema, table):
    cur= get_snowflake_connection()

    ctas_sql = f"""
    CREATE OR REPLACE TABLE dev.{load_schema}.{table} AS(
    WITH
    airline_summary AS (
        SELECT cityname, CEIL(AVG(price)/100) * 100 AS avg_price
        FROM dev.{extract_schema}.slackbot_airline_info
        WHERE datetime = (SELECT MIN(datetime) FROM dev.raw_data.slackbot_airline_info)
        GROUP BY cityname)
    ,
    weather_summary AS (
        SELECT row_num, ROUND(MIN(temp)-273.15) min_temp, ROUND(MAX(temp)-273.15) max_temp , MAX(weather) weather, MAX(pop) pop  
        FROM dev.{extract_schema}.slackbot_weather_info
        WHERE LEFT(datetime,10) = TO_CHAR(CONVERT_TIMEZONE('UTC', 'Asia/Seoul', CURRENT_TIMESTAMP), 'YYYY-MM-DD')
        GROUP BY row_num)

    SELECT 
        country, name_ko, name_eng, min_temp, max_temp, 
        weather, pop, kftc_deal_bas_r, avg_price
    FROM dev.{extract_schema}.country_currency_info A
    INNER JOIN dev.{extract_schema}.get_city_info B ON A.iso3166 = B.country
    INNER JOIN dev.{extract_schema}.slackbot_currency_info C ON A.iso4217 = LEFT(C.cur_unit,3)
    INNER JOIN weather_summary D ON B.row_num = D.row_num
    INNER JOIN airline_summary E ON B.name_ko = E.cityname
    );"""

    logging.info(ctas_sql)
    try:
        cur.execute(ctas_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

with DAG(
    dag_id = 'get_slackbot_backend',
    start_date = datetime(2025,1,1),
    catchup=False,
    schedule_interval='0 16 * * *'
) as dag:
    
    extract_schema ='raw_data'
    load_schema ="analytics"
    table ='slackbot_backend'

    elt(extract_schema, load_schema, table)
