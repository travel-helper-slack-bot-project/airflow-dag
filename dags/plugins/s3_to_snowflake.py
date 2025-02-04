from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import json
from datetime import datetime
import logging

def load_s3_to_snowflake(topic, schema, table):

    today_date = datetime.now().strftime("%Y%m%d")

    s3_key = f"slackbot/{topic}/{today_date}/{topic}_info.parquet"
    s3_stage = "my_s3_stage"

    hook = SnowflakeHook(snowflake_conn_id="snowflake_dev_db")
    cur = hook.get_conn().cursor()

    create_sql_dict = {
        'weather': f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table}(
            row_num INT,
            temp NUMBER(10,2),
            weather VARCHAR,
            clouds NUMBER,
            pop NUMBER(10,2),
            datetime TIMESTAMP
        );""",
        'currency': f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table}(
            cur_unit varchar(255),
            cur_nm varchar(255),
            kftc_deal_bas_r double
        );""",
        'airline': f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table}(
            dateTime VARCHAR,
            destination VARCHAR,
            cityName VARCHAR,
            airline VARCHAR,
            price INT
        );"""
    }
    
    delete_sql = f"DELETE FROM {schema}.{table};"
    copy_sql =  f"""
    COPY INTO {schema}.{table}
    FROM @{schema}.{s3_stage}/{s3_key}
    FILE_FORMAT = (TYPE ="PARQUET")
    MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
    Force=True;
    """

    print(copy_sql)

    try:
        cur.execute(create_sql_dict[topic])
        cur.execute(delete_sql)
        cur.execute(copy_sql)
        cur.execute("COMMIT;")

        logging.info(f"S3 Parquet 데이터가 Snowflake {schema}.{table}에 성공적으로 적재되었습니다.")

    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    finally:
        cur.close()

