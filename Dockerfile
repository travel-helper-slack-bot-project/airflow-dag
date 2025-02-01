FROM apache/airflow:2.9.1

USER root

RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    wget \
    libnss3 \
    libgconf-2-4 \
    libx11-xcb1 \
    libappindicator3-1 \
    fonts-liberation \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Google Chrome 설치
RUN apt-get update && apt-get install -y wget && \
    wget -q -O google-chrome.deb http://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_132.0.6834.159-1_amd64.deb \
    && apt install -y ./google-chrome.deb \
    && rm google-chrome.deb

# ChromeDriver 버전 확인 후 설치
RUN apt-get update && \
    apt-get install -y wget unzip && \
    wget -q -O chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/132.0.6834.159/linux64/chromedriver-linux64.zip && \
    unzip chromedriver.zip 

#RUN  mv chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
#RUN rm chromedriver.zip 
#RUN chmod +x /usr/local/bin/chromedriver

# 기본 유저로 복귀
USER airflow

