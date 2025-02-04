import requests
import os
from dotenv import load_dotenv
import time
import random
import pandas as pd
from tqdm import tqdm

def get_exchange_loacation_information():
    # 환경 변수 로드
    load_dotenv()

    # API 키 가져오기
    KAKAO_API_KEY = os.environ.get("KAKAO_API_KEY")

    # URL 설정
    url = 'https://dapi.kakao.com/v2/local/search/keyword.json'

    locations = [
    {"x": "126.9784", "y": "37.5665"},  # 서울
    {"x": "129.0756", "y": "35.1796"},  # 부산
    {"x": "128.6014", "y": "35.8714"},  # 대구
    {"x": "126.7052", "y": "37.4563"},  # 인천
    {"x": "126.8530", "y": "35.1595"},  # 광주
    {"x": "127.3845", "y": "36.3504"},  # 대전
    {"x": "129.3114", "y": "35.5384"},  # 울산
    {"x": "127.2891", "y": "36.4800"},  # 세종
    {"x": "127.0286", "y": "37.2636"},  # 수원
    {"x": "127.7341", "y": "37.8854"},  # 춘천
    {"x": "128.8965", "y": "37.7519"},  # 강릉
    {"x": "127.4897", "y": "36.6424"},  # 청주
    {"x": "127.1529", "y": "36.8151"},  # 천안
    {"x": "127.1480", "y": "35.8242"},  # 전주
    {"x": "126.7365", "y": "35.9676"},  # 군산
    {"x": "126.3922", "y": "34.8118"},  # 목포
    {"x": "127.6622", "y": "34.7604"},  # 여수
    {"x": "127.4872", "y": "34.9507"},  # 순천
    {"x": "129.3650", "y": "36.0190"},  # 포항
    {"x": "128.6811", "y": "35.2270"},  # 창원
    {"x": "128.1136", "y": "35.1798"},  # 진주
    {"x": "126.5312", "y": "33.4996"},  # 제주
    {"x": "126.5600", "y": "33.2529"}   # 서귀포
    ]

    keywords = ["환전소", "외환", "환전", "외화 환전", "환전 가능한 은행"]

    params = {
        'radius': 20000
    }

    exchange_location_info = []
    for l in tqdm(locations):
        params['x'] = l['x']
        params['y'] = l['y']
        for k in keywords:
            params['query'] = k
            for i in range(1,4):

                params['page'] = i
                headers = {"Authorization": f"KakaoAK {KAKAO_API_KEY}"}
                response = requests.get(url, params=params, headers=headers)
                data = response.json()
                
                for document in data['documents']:
                    address_name = document.get("address_name", 'N/A')
                    phone = document.get("phone", 'N/A')
                    place_name = document.get("place_name", 'N/A')
                    place_url = document.get("place_url", 'N/A')
                    road_address_name = document.get("road_address_name", 'N/A')

                    exchange_location_info.append({
                        "address_name":address_name,
                        "phone":phone,
                        "place_name":place_name,
                        "place_url":place_url,
                        "road_address_name":road_address_name
                    })

                time.sleep(random.uniform(0.5, 2))

    df = pd.DataFrame(exchange_location_info)
    df = df.drop_duplicates(ignore_index=True)
    return df

df = get_exchange_loacation_information()
df.to_csv("C:/Users/Yeojun/airflow/airflow-dag/data/exchange_location.csv",index=False, encoding='utf-8-sig')


    