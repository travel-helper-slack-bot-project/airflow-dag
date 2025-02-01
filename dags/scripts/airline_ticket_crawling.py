from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By

import pandas as pd
from datetime import datetime, timedelta
from airflow.models import Variable
import time
import logging
import os


#데이터 프레임 생성 + 반환 
def make_dataframe():
    
    columns = ['dateTime', 'destination', 'cityName','airline', 'price']
    
    return pd.DataFrame(columns=columns)


#크롤링 데이터를 csv로 저장 
def save_as_csv_file(df):
    today = datetime.now().strftime("%Y-%m-%d")
    
    #data 폴더에 크롤링한 파일(csv) 저장
    file_path = f"data/airline_crawling_data_{today}.csv"
    if os.path.exists(file_path):
        df.to_csv(file_path, encoding='utf-8', mode='a', header=False, index=False)
    else:
        df.to_csv(file_path, encoding='utf-8', mode='w', header=True, index=False)
        


def naver_airline_ticke_info_crawling():
    
    foreign_country = {'TYO': '도쿄', 'OKA': '오키나와', 'SPK':'삿포로',
                        'NYC':'뉴욕', 'LAX':'로스앤젤레스', 'SEA':'시애틀', 
                        'PAR':'파리', 'BOD':'보르도', 'NCE':'니스',
                        'BER': '베를린', 'MUC':'뮌헨', 'CGN':'쾰른',
                        'LON': '런던', 'MAN': '맨체스터', 'EDI':'에딘버러'
                        }
    
    #domestic = ['CJU']
    
    #국외
    for destination in foreign_country.keys():
        
        date = datetime.now()
        
        for i in range(3):
            date = date + timedelta(days=i)
            today = date.strftime("%Y%m%d")
            foreign_country_url = f"https://flight.naver.com/flights/international/ICN-{destination}-{today}?adult=1&fareType=Y"
            data_crawling(foreign_country_url, destination, date ,foreign_country )
            time.sleep(5)
    
    #국내
    #for destination in domestic:
    #    domestic_url = f"https://flight.naver.com/flights/domestic/ICN-{destination}-{today}?adult=1&fareType=YC"
    #    data_crawling(domestic_url, destination, today)
    #    time.sleep(5)


def data_crawling(url, destination, date ,foreign_country):
    
    df = make_dataframe()
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Headless 모드 활성화
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    service = Service("/opt/airflow/chromedriver-linux64/chromedriver")

    
    with webdriver.Chrome(service=service, options=chrome_options) as driver:

        logging.info("항공권을 크롤링하는 중입니다.")
            
        driver.get(url)
            
        driver.implicitly_wait(20)
            
            
        try:
            #항공사 티켓 리스트 가져오기 
            ticket_list =driver.find_element(By.XPATH, '//*[@id="container"]/div[6]/div/div[3]/div')
                
            #리스트 개수 확인하기 
            #div 태그로 이루어진 항공권 리스트 중에서 항공사 정보를 갖고 있는 큰 div 덩어리만 가지고 올 수 있도록 함 
            airplane_list = ticket_list.find_elements(By.XPATH, "//div[contains(@class, 'indivisual_IndivisualItem__CVm69')]")
            print(len(airplane_list))
            
            for i in range(1, len(airplane_list)+1):
                    
                #티켓 정보 div 가져오기 
                airplane_info =  ticket_list.find_element(By.XPATH, f'//*[@id="container"]/div[6]/div/div[3]/div/div[{i}]')
                
                try:
                    #광고인 경우에는 해당 부분이 없으므로 pass하고 바로 다음 정보 확인할 수 있도록 함 
                    airline_name = airplane_info.find_element(By.XPATH, f'//*[@id="container"]/div[6]/div/div[3]/div/div[{i}]/div/div[1]/div/div[1]/div[2]/div/span[2]/b').text
                except:
                    continue
                
                #특가인 경우에는 가격이 2개로 나누어짐 -> 특가 가격으로 일단 가져올 수 있도록 except에서 처리 
                try:
                    airplane_price = airplane_info.find_element(By.XPATH, f'//*[@id="container"]/div[6]/div/div[3]/div/div[{i}]/div/div[2]/div/div/div/b/i').text
                except:
                    airplane_price = airplane_info.find_element(By.XPATH, f'//*[@id="container"]/div[6]/div/div[3]/div/div[{i}]/div/div[2]/div/div/div[1]/b/i').text
                    
                print(airplane_price, airline_name)
                print(type(airplane_price))
                
                df.loc[i, 'dateTime'] =  date
                df.loc[i, 'destination'] = destination
                df.loc[i, 'cityName'] = foreign_country[destination]
                df.loc[i, 'airline'] = airline_name
                df.loc[i, 'price'] = int(airplane_price.replace(",", "").strip())
                
        except Exception as e:
            logging.info(f"error: {e}")
            
    save_as_csv_file(df)
    logging.info("csv 저장을 완료했습니다.")
            

if __name__ == "__main__":
    naver_airline_ticke_info_crawling()