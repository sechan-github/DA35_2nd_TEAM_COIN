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


# if-error, return 0
def division_if_error(dividend, divisor):
    try:
        return dividend / divisor
    except ZeroDivisionError:
        return 0

# df 내 문자열을 숫자로 변환하는 함수
def convert_to_number(value):
    # 값이 없다면 0을 반환
    if value == '':
        return 0

    # '≈', '원', '명', ',' 등을 제거
    value = value.replace('≈', '').replace('원', '').replace('명', '').replace(',', '').replace('+', '')
    if '조' in value or '억' in value or '만' in value:
        parts = value.split()
        total = 0
        for part in parts:
            if '조' in part:
                # "조"가 포함된 경우, 숫자만 추출하여 "조" 단위로 더함
                total += float(part.replace('조', '')) * 10000  # '억' 단위로 변환하여 계산
            elif '억' in part:
                # "억"이 포함된 경우, 숫자만 추출하여 바로 더함
                total += float(part.replace('억', '')) * 1  # '억' 단위를 유지
            elif '만' in part:
                # "만"이 포함된 경우, 10000으로 나누어 "억" 단위로 변환 후 더함
                total += float(part.replace('만', '')) / 10000
        return round(total, 2)
    elif '%' in value:
        # 백분율 처리
        return round(float(value.replace('%', '')), 2)
    else:
        # 그 외 숫자만 있는 경우 숫자 변환
        try:
            return int(value)
        except ValueError:
            return 0



# bithumb에 상장된 전체 코인 리스트 호출
def get_bithumb_listed_coins():
    url = "https://api.bithumb.com/public/ticker/all"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        coins = [coin for coin in data.get("data").keys() if coin != "date"]
        return coins



# selenium 크롤링 실행
def selenium_crawling(coin_name):
    service = Service(executable_path=ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    browser = webdriver.Chrome(service=service, options=options)

    url = f"https://www.bithumb.com/react/trade/order/{coin_name}-KRW"
    browser.get(url)

    wait = WebDriverWait(browser, 5)

    # 거래소 정보 모달 열기
    coin_info_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#root > main > div > div > div > div.Trade_trade__main__xec99 > div.Trade_trade__section__kkBQ7 > div.Info_info__lcEsd > div.Info_info-content__ctp7S > div.Info_info-content-top__Elyn5 > div > button:nth-child(2)")))
    exchange_info_button = wait.until(EC.element_to_be_clickable((By.CLASS_NAME, "Info_info-content-top__button-premium__pW4nU")))
    coin_info_button.click()
    exchange_info_button.click()

    # css selector 정의
    ## 최상위 회원 영향도 -> 보유비중&거래비중은 selector가 숫자 자리별로 달라서 1, 2로 구분해서 가져오는 방식을 채택
    # 최상위 회원 보유비중
    s_top_holding1 = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(3) > div > div > div.PremiumCoinInfo_tops-impact__graph-item__t6RY5.PremiumCoinInfo_tops-impact__graph-item--color-red__ntJdK > div > strong > span:nth-child(1) > span:nth-child(2) > span.PremiumCoinInfo_tops-impact__graph-item-text-value-num__JzXZf.currentTicker"
    s_top_holding2 = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(3) > div > div > div.PremiumCoinInfo_tops-impact__graph-item__t6RY5.PremiumCoinInfo_tops-impact__graph-item--color-red__ntJdK > div > strong > span:nth-child(2) > span:nth-child(2) > span.PremiumCoinInfo_tops-impact__graph-item-text-value-num__JzXZf.currentTicker"
    # 최상위 회원 거래비중
    s_top_volume1 = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(3) > div > div > div.PremiumCoinInfo_tops-impact__graph-item__t6RY5.PremiumCoinInfo_tops-impact__graph-item--color-gray__JAW-C > div > strong > span:nth-child(1) > span:nth-child(2) > span.PremiumCoinInfo_tops-impact__graph-item-text-value-num__JzXZf.currentTicker"
    s_top_volume2 = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(3) > div > div > div.PremiumCoinInfo_tops-impact__graph-item__t6RY5.PremiumCoinInfo_tops-impact__graph-item--color-gray__JAW-C > div > strong > span:nth-child(2) > span:nth-child(2) > span.PremiumCoinInfo_tops-impact__graph-item-text-value-num__JzXZf.currentTicker"
    # 전체 보유자수
    s_num_total_holding = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(2) > div > div:nth-child(1) > span.PremiumCoinInfo_related-info__item-value__f1FGz"
    # 빗썸 내부 유통량(금액)
    s_bithumb_supply = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(1) > div > div.PremiumCoinInfo_distribution__top-area__y32iv > span"
    # 전일대비 유통량
    s_bithumb_supply_change = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(1) > div > div.PremiumCoinInfo_distribution__bottom-area__DR3rz > dl > div:nth-child(1) > dd"
    # 24시간 거래금액
    s_volume_amount = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(2) > div > div:nth-child(2) > span.PremiumCoinInfo_related-info__item-value__f1FGz"
    # 24시간 순입금
    s_net_deposit = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(1) > div > div.PremiumCoinInfo_distribution__bottom-area__DR3rz > dl > div:nth-child(2) > dd"
    # 현재가
    s_price = "#root > main > div > div > div > div.Trade_trade__main__xec99 > div.Trade_trade__section__kkBQ7 > div.Info_info__lcEsd > div.info-head > div.InfoHead_info-head-price__S3s\+a > h3"
    # 체결강도
    s_transaction = "#root > main > div > div > div > div.Trade_trade__main__xec99 > div.Trade_trade__section__kkBQ7 > div.Info_info__lcEsd > div.info-head > div.InfoHead_info-head-list__rjoQY > dl > div:nth-child(5) > dd"
    # 시가총액
    s_marketcap = "#infoTabPanel02 > div.CoinInfoTab_info-tab-sub__-V7bq > table > tbody > tr:nth-child(5) > td:nth-child(4)"
    # 1주일 상승률
    s_one_week = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(4) > div > ul > li:nth-child(1) > span.PremiumCoinInfo_rising-rate__list-item-rise-rate__qF5pZ"
    # 1개월 상승률
    s_one_month = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(4) > div > ul > li:nth-child(2) > span.PremiumCoinInfo_rising-rate__list-item-rise-rate__qF5pZ"
    # 3개월 상승률
    s_three_month = "#root > main > div > div > div > div.PremiumCoinInfo_coin-info-premium__wFGjP > div > div.PremiumCoinInfo_coin-info-premium__content__xcZmA.cm-gray-scroll > div:nth-child(4) > div > ul > li:nth-child(3) > span.PremiumCoinInfo_rising-rate__list-item-rise-rate__qF5pZ"

    # 오류로 인해 동작이 멈추는 것을 방지하기 위해, 찾는 css selector의 값이 없는 경우 ""를 반환.
    ## 최상위 회원 영향도 - 보유비중/거래비중은 selector가 숫자 자리별로 달라서 1, 2로 구분해서 가져오는 방식을 채택하였으나, 경우에 따라 1만 있어서 이를 처리하기 위한 함수.
    def get_text_from_selector(css_selector):
        try:
            element_text = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, css_selector))).text
            return element_text
            
        except TimeoutException:
            return ""
    
    # 정보 크롤링
    top_holding = convert_to_number(get_text_from_selector(s_top_holding1) + get_text_from_selector(s_top_holding2)) 
    top_volume = convert_to_number(get_text_from_selector(s_top_volume1) + get_text_from_selector(s_top_volume2))
    num_total_holding = convert_to_number(get_text_from_selector(s_num_total_holding))
    bithumb_supply = convert_to_number(get_text_from_selector(s_bithumb_supply))
    net_deposit = convert_to_number(get_text_from_selector(s_net_deposit))
    bithumb_supply_chagne = convert_to_number(get_text_from_selector(s_bithumb_supply_change))
    volume_amount = convert_to_number(get_text_from_selector(s_volume_amount))
    price = convert_to_number(get_text_from_selector(s_price))
    marketcap = convert_to_number(get_text_from_selector(s_marketcap))
    transaction = convert_to_number(get_text_from_selector(s_transaction))
    one_week = convert_to_number(get_text_from_selector(s_one_week))
    one_month = convert_to_number(get_text_from_selector(s_one_month))
    three_month = convert_to_number(get_text_from_selector(s_three_month))

    browser.quit()

    return {
        "Ticker": coin_name,
        "Date": datetime.today().strftime("%Y/%m/%d"),
        "시가총액(억)": marketcap,
        "보유비중(%)": top_holding,
        "거래비중(%)": top_volume,
        "보유자수(명)": num_total_holding,
        "내부보유금액(억)": bithumb_supply,
        "24시간 순입금(억)": net_deposit,
        "전일대비유통량증감(%)": bithumb_supply_chagne,
        "24시간 거래금액(억)": volume_amount,
        "체결강도(%)": transaction,
        "가격": price,
        "빗썸유통비율(%)": round(division_if_error(bithumb_supply, marketcap)*100 ,2),
        "거래금액/내부보유금액(%)": round(division_if_error(volume_amount, bithumb_supply)*100, 2),
        "순입금/내부보유금액(%)": round(division_if_error(net_deposit, bithumb_supply)*100, 2),
        "1주일 상승률(%)": one_week,
        "1개월 상승률(%)": one_month,
        "3개월 상승률(%)": three_month
    }



# 비동기적으로 selenium 크롤링 실행
async def async_selenium_crawling(sem, coin_name):
    async with sem:
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor() as pool:
            return await loop.run_in_executor(pool, selenium_crawling, coin_name)



# main 함수 정의
async def main():
    sem = asyncio.Semaphore(10) # 동시에 최대 N개의 작업만 실행
    tasks = [async_selenium_crawling(sem, coin_name) for coin_name in coin_list] # 테스트로 5개 코인만 
    all_data = await asyncio.gather(*tasks)

    # 데이터 프레임 생성
    df = pd.DataFrame(all_data)

    print(df)

    # df.to_csv("bithumb.csv")

    # 데이터베이스 이름
    database_name = "bithumb_data"

    # pymysql을 사용하여 데이터베이스 생성 (데이터베이스가 없을 경우)
    conn = pymysql.connect(host='localhost', user='playdata', password='0000', charset='utf8mb4')
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        conn.commit()
    finally:
        conn.close()
    # SQLAlchemy 엔진 생성 (pymysql을 사용)
    # 사용자 이름, 비밀번호, 호스트, 포트, 데이터베이스 이름을 자신의 환경에 맞게 수정
    engine = create_engine(f"mysql+pymysql://playdata:0000@localhost:3306/{database_name}")

    # DataFrame을 SQL 테이블로 저장
    # if_exists 옵션을 'replace'로 설정하면 테이블이 이미 존재할 경우 대체. 'append'를 선택하면 기존 테이블에 데이터가 추가.
    df.to_sql(name='bithumb_table', con=engine, if_exists='replace', index=False)



if __name__ == "__main__":
    start=time.time()
    
    coin_list = get_bithumb_listed_coins() # Bithumb 상장 코인 리스트 받아오기
    asyncio.run(main())

    end=time.time()
    print(f"{end-start}")