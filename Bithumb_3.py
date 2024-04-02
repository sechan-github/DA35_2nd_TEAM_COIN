import time
import requests
import pandas as pd
import asyncio
import aiohttp
import os
import pymysql
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine
from sqlalchemy.sql import text



# SQLAlchemy 엔진 생성
# 사용자 이름, 비밀번호, 호스트, 포트, 데이터베이스 이름 환경에 맞게 수정
database_name = "bithumb_data"    
engine = create_engine(f"mysql+pymysql://playdata:0000@localhost:3306/{database_name}")

# 상위 20 종목 추출
df = pd.read_sql(text(f"select * from bithumb_table where Ticker != 'USDC' and Ticker != 'USDT' and (`빗썸유통비율(%)` between 50 and 100) and `시가총액(억)` != 0 and `시가총액(억)` < 1000 and `보유비중(%)` > 15 order by `보유자수(명)` asc, `거래비중(%)` desc, `거래금액/내부보유금액(%)` desc, `3개월 상승률(%)` asc, `1개월 상승률(%)` asc, `1주일 상승률(%)` asc, `순입금/내부보유금액(%)` desc, `체결강도(%)` desc limit 20;"), con=engine)
ticker_list = list(df["Ticker"])

print(df)