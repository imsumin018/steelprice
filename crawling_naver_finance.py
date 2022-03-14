#!/usr/bin/env python
# coding: utf-8
# Crawling USD-kRW, JPY-KRW, THB-KRW data and Store to MS-SQL DB at every 8:01 AM

import schedule
import time
from datetime import date
import logging
import logging.config
import json
import pymssql

from selenium import webdriver
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(module)s:%(levelname)s:%(message)s', '%Y-%m-%d %H:%M:%S')
file_debug_handler = logging.FileHandler('debug.log')
file_debug_handler.setLevel(logging.DEBUG)
file_debug_handler.setFormatter(formatter)
logger.addHandler(file_debug_handler)

urls = ["https://finance.naver.com/marketindex/exchangeDetail.nhn?marketindexCd=FX_USDKRW",
"https://finance.naver.com/marketindex/exchangeDetail.nhn?marketindexCd=FX_JPYKRW",
'https://finance.naver.com/marketindex/exchangeDetail.nhn?marketindexCd=FX_THBKRW']

def get_exchange_info(url, driver):
    driver.get(url)
    values=[]
    
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table0 = soup.find("table", {"class": "tbl_calculator"})
    tbody = table0.select_one('tbody')
    trs = tbody.select('tr')
    values.append(trs[0].find('td').text)
  
    table = soup.find("table", {"class": "tbl_exchange"})
    a = table.findAll('tr')
    '''
    [0]매매기준율
    [1]<th class="th_ex2"><span>현찰 사실때</span></th>
    [2]<th class="th_ex3"><span>현찰 파실때</span></th>
    [3]<th class="th_ex4"><span>송금 보내실때</span></th>
    [4]<th class="th_ex5"><span>송금 받으실때</span></th>
    X<th class="th_ex6"><span>T/C 사실때</span></th>
    X<th class="th_ex7"><span>외화수표 파실때</span></th>
    '''
    for tr in a :
        try:
            values.append(tr.find('td').text) 
        except:
            print("")
    return values
   
def insert_into_db(cursor, market_index, arr):
    today = str(date.today())
    index1 = market_index[:3]
    index2 = market_index[-3:]
    tb_name = "dbo."+index2+"_"+index1
    q_format = "\'{0}\', {1}, {2}, {3}, {4}, {5}, '{6}', '{7}', '{8}'".format(today,arr[3],arr[4],arr[1],arr[2], arr[0], index2, index1, index2+'-'+index1)
    #query_string = "INSERT INTO "+tb_name+" VALUES ("+ q_format  +");"
    merge_string = "MERGE INTO " + tb_name + " AS a USING (SELECT 1 AS dual) AS b ON (a.date = \'"+ today + "\') WHEN MATCHED THEN \
        UPDATE SET a.[송금보낼때] ="+ str(arr[3]) +", a.[송금받을때] =" + str(arr[4]) + ", a.[현찰살때]=" + str(arr[1]) + ", a.[현찰팔때]=" + str(arr[2])+ ", a.[매매기준율]="+str(arr[0])+"\
        WHEN NOT MATCHED THEN INSERT ([Date],[송금보낼때],[송금받을때],[현찰살때],[현찰팔때],[매매기준율],[index1],[index2],[market_index]) VALUES ("+ q_format  +");"
    print(merge_string)
    logger.debug(merge_string)
    res = cursor.execute(merge_string)
    logger.debug(res)

def every():
    conn = pymssql.connect(host=r"203.245.157.76", user='posco2', password='posco!@34', database='workdb', charset='utf8')
    cursor = conn.cursor()
    driver = webdriver.Chrome(executable_path="D:\chromedriver_win32\chrome98\chromedriver")

    for url in urls:
        print(url)
        logger.debug(url)
        values = get_exchange_info(url, driver)
        to_store = []
        to_store.append(float(values[0].replace(",","")))
        to_store.append(float(values[3].replace(",","")))
        to_store.append(float(values[4].replace(",","")))
        to_store.append(float(values[1].replace(",","")))
        to_store.append(float(values[2].replace(",","")))
        #print(to_store)
        insert_into_db(cursor, url.split("_")[1], to_store)
    conn.commit()
    conn.close()
    driver.quit()

#every()
#rates are changed between AM 9:00 ~ PM 15:30
schedule.every().day.at("08:01").do(every)
schedule.every().day.at("12:30").do(every)
schedule.every().day.at("23:30").do(every)
while True:
    schedule.run_pending()
