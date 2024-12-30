from multiprocessing import Process
import os
from concurrent.futures import ThreadPoolExecutor
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque
import time 
import csv

BTC_final_date = ''

# 計算交易總筆數、查閱最終日期
def get_final_date(btc_driver_last):
    try:
        global BTC_final_date

        try:
            page_8 = WebDriverWait(btc_driver_last, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[8]'))
            )
            page_8_text = page_8.text
        except TimeoutException:
            page_8_text = 'null'
        try:
            page_9 = WebDriverWait(btc_driver_last, 2).until( # 等2秒，沒有找到就pass
                EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[9]'))
            )
            page_9_text = page_9.text
        except TimeoutException:
            page_9_text = 'null'


        # 判斷最後頁數的按鈕，並點擊以及紀錄頁數
        if page_9_text == '>':
            if page_8_text != '>':
                btc_driver_last.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                last_page_button = WebDriverWait(btc_driver_last, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[8]'))
                )
                last_page_button.click()
            else:
                print('can not find last button for web page')
        else:
            if page_8_text == '>':
                btc_driver_last.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                last_page_button = WebDriverWait(btc_driver_last, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[1]/div[2]/div[5]/nav/li[7]'))
                )
                last_page_button.click()
            else:
                print('can not find last button for web page')

        time.sleep(0.5)
        BTC_page_3 = BeautifulSoup(btc_driver_last.page_source, 'html.parser')
        BTC_pending_date = BTC_page_3.select('tr')[-1].select_one('td:nth-of-type(2) div')
        BTC_final_date = BTC_pending_date.text if BTC_pending_date else None # 總交易最後一筆的日期紀錄起來
    except Exception as ex:
        print(f'get_final_date fail, reason:{ex}')
        return 0,0
    



    
def btc_crawler(btc_driver_first, btc_driver_second, btc_driver_third, btc_driver_last, hashes_seen, header_written):
    try:
        BTC_soup_1 = BeautifulSoup(btc_driver_first.page_source, 'html.parser')
        BTC_soup_2 = BeautifulSoup(btc_driver_second.page_source, 'html.parser')
        BTC_soup_3 = BeautifulSoup(btc_driver_third.page_source, 'html.parser')

        BTC_rows_1 = BTC_soup_1.select('tr')
        BTC_data_to_write_1 = []
        BTC_rows_2 = BTC_soup_2.select('tr')
        BTC_data_to_write_2 = []
        BTC_rows_3 = BTC_soup_3.select('tr')
        BTC_data_to_write_3 = []

        btc_pending_txn_count = int(BTC_soup_1.find(class_='font-size-sm responsive-label nowrap secondary-text').text.replace("The total Number of ","").replace(" Txns","").replace(",",""))

        global BTC_final_date
        get_final_date(btc_driver_last)
        system_time = time.strftime('%Y/%m/%d %I:%M:%S %p', time.localtime(time.time()))

        # 本次迴圈計算的交易量
        this_time_rows = 0

        # driver_1迭代處理網頁每一行
        for row in BTC_rows_1:
            txn_link_1 = row.select_one('td:nth-of-type(1) a') # 從表格行中選擇特定的資料欄位
            if txn_link_1:
                txn_hash_1 = txn_link_1.get('href').split('/')[-1]
                if txn_hash_1 not in hashes_seen: # 利用deque排除重複的交易
                    hashes_seen.append(txn_hash_1)
                    txn_time_1 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_1 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_1 = row.select_one('td:nth-of-type(4)')
                    fees_1 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_1 = 0
                    inputsCount_amt_1 = 0
                    if inputsVolume_1:
                        inputsVolume_amt_1 = float(inputsVolume_1.text.split()[0])
                        inputsCount_amt_1 = int(inputsVolume_1.text.split()[1].replace('(',''))
                    outputsVolume_amt_1 = 0
                    outputsCount_amt_1 = 0
                    if outputsVolume_1:
                        outputsVolume_amt_1 = float(outputsVolume_1.text.split()[0])
                        outputsCount_amt_1 = int(outputsVolume_1.text.split()[1].replace('(',''))
                    fees_amt_1 = 0
                    if fees_1:
                        fees_amt_1 = float(fees_1.text.split()[0])
                    BTC_data_to_write_1.append([system_time, txn_hash_1, txn_time_1, inputsVolume_amt_1, outputsVolume_amt_1, inputsCount_amt_1, outputsCount_amt_1, fees_amt_1, btc_pending_txn_count, BTC_final_date])

        # driver_2迭代處理網頁每一行
        for row in BTC_rows_2:
            txn_link_2 = row.select_one('td:nth-of-type(1) a') 
            if txn_link_2:
                txn_hash_2 = txn_link_2.get('href').split('/')[-1]
                if txn_hash_2 not in hashes_seen: 
                    hashes_seen.append(txn_hash_2)
                    txn_time_2 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_2 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_2 = row.select_one('td:nth-of-type(4)')
                    fees_2 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_2 = 0
                    inputsCount_amt_2 = 0
                    if inputsVolume_2:
                        inputsVolume_amt_2 = float(inputsVolume_2.text.split()[0])
                        inputsCount_amt_2 = int(inputsVolume_2.text.split()[1].replace('(',''))
                    outputsVolume_amt_2 = 0
                    outputsCount_amt_2 = 0
                    if outputsVolume_2:
                        outputsVolume_amt_2 = float(outputsVolume_2.text.split()[0])
                        outputsCount_amt_2 = int(outputsVolume_2.text.split()[1].replace('(',''))
                    fees_amt_2 = 0
                    if fees_2:
                        fees_amt_2 = float(fees_2.text.split()[0])
                    BTC_data_to_write_2.append([system_time, txn_hash_2, txn_time_2, inputsVolume_amt_2, outputsVolume_amt_2, inputsCount_amt_2, outputsCount_amt_2, fees_amt_2, btc_pending_txn_count, BTC_final_date])


        # driver_3迭代處理網頁每一行
        for row in BTC_rows_3:
            txn_link_3 = row.select_one('td:nth-of-type(1) a') # 從表格行中選擇特定的資料欄位
            if txn_link_3:
                txn_hash_3 = txn_link_3.get('href').split('/')[-1]
                if txn_hash_3 not in hashes_seen: # 利用deque排除重複的交易
                    hashes_seen.append(txn_hash_3)
                    txn_time_3 = row.select_one('td:nth-of-type(2) div').text
                    inputsVolume_3 = row.select_one('td:nth-of-type(3)')
                    outputsVolume_3 = row.select_one('td:nth-of-type(4)')
                    fees_3 = row.select_one('td:nth-of-type(6)')
                    inputsVolume_amt_3 = 0
                    inputsCount_amt_3 = 0
                    if inputsVolume_3:
                        inputsVolume_amt_3 = float(inputsVolume_3.text.split()[0])
                        inputsCount_amt_3 = int(inputsVolume_3.text.split()[1].replace('(',''))
                    outputsVolume_amt_3 = 0
                    outputsCount_amt_3 = 0
                    if outputsVolume_3:
                        outputsVolume_amt_3 = float(outputsVolume_3.text.split()[0])
                        outputsCount_amt_3 = int(outputsVolume_3.text.split()[1].replace('(',''))
                    fees_amt_3 = 0
                    if fees_3:
                        fees_amt_3 = float(fees_3.text.split()[0])

                    BTC_data_to_write_3.append([system_time, txn_hash_3, txn_time_3, inputsVolume_amt_3, outputsVolume_amt_3, inputsCount_amt_3, outputsCount_amt_3, fees_amt_3, btc_pending_txn_count, BTC_final_date])

        if BTC_data_to_write_1:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_1)
            print(f"- 本次driver_1抓取了 {len(BTC_data_to_write_1)} 條交易")

        if BTC_data_to_write_2:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_2)
            print(f"- 本次driver_2抓取了 {len(BTC_data_to_write_2)} 條交易")

        if BTC_data_to_write_3:
            with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile:
                csv_writer = csv.writer(csvfile)
                if not header_written:
                    csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                    header_written = True
                csv_writer.writerows(BTC_data_to_write_3)
            print(f"- 本次driver_3抓取了 {len(BTC_data_to_write_3)} 條交易")

        this_time_rows = len(BTC_data_to_write_1) + len(BTC_data_to_write_2) + len(BTC_data_to_write_3) # 追蹤總行數

        if this_time_rows != 0:
            output = f"""
            BTC三個視窗抓取了 {this_time_rows} 條交易
            BTC總交易數量為 {btc_pending_txn_count} 條交易
            BTC目前時間為: {system_time}
            ----------------------------------------"""
            print(output)
            with open("btc_transactions.txt", "w", encoding="utf-8") as f:
                f.write(output)
    
    except Exception as ex:
        print(f"Failed reason: {ex}")






if __name__ == '__main__':
    # chrome_service = Service('chromedriver.exe')

    chrome_options = Options()
    # chrome_options.add_argument("--headless")

    # btc_driver_first = webdriver.Chrome(service=chrome_service) # btc_driver_first 抓取 btc.com第一頁交易資訊
    btc_driver_first = webdriver.Chrome() # btc_driver_first 抓取 btc.com第一頁交易資訊
    btc_driver_first.get('https://explorer.cloverpool.com/btc/unconfirmed-txs')
    btc_driver_first.maximize_window()
    # alert = WebDriverWait(btc_driver_first, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-modal"]/div/div[2]/div/div[4]/a'))
    # )
    # alert.click()
    # time.sleep(2)
    # btc_driver_first.execute_script("window.scrollTo(0, document.body.scrollHeight);") # 滑到最底下
    # page_opthon = WebDriverWait(btc_driver_first, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[5]/nav/div/span/div'))
    # )
    # page_opthon.click()
    # show_100 = WebDriverWait(btc_driver_first, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-dropdown"]/div/div/div[4]'))
    # )
    # show_100.click() 
    time.sleep(10)

    # btc_driver_second = webdriver.Chrome(service=chrome_service) # btc_driver_second 抓取 btc.com第二頁交易資訊
    btc_driver_second = webdriver.Chrome() # btc_driver_second 抓取 btc.com第二頁交易資訊
    btc_driver_second.get('https://explorer.cloverpool.com/btc/unconfirmed-txs')
    btc_driver_second.maximize_window()
    # alert = WebDriverWait(btc_driver_second, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-modal"]/div/div[2]/div/div[4]/a'))
    # )
    # alert.click()
    # time.sleep(2)
    # btc_driver_second.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    # # 點擊一次顯示100頁
    # page_opthon = WebDriverWait(btc_driver_second, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[5]/nav/div/span/div'))
    # )
    # page_opthon.click()
    # show_100 = WebDriverWait(btc_driver_second, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-dropdown"]/div/div/div[4]'))
    # )
    # show_100.click() 
    # btc_driver_second.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    # page_2 = WebDriverWait(btc_driver_second, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[5]/nav/li[3]/button'))
    # )
    # page_2.click() 
    time.sleep(10)


    # btc_driver_third = webdriver.Chrome(service=chrome_service) # btc_driver_second 抓取 btc.com第三頁交易資訊
    btc_driver_third = webdriver.Chrome() # btc_driver_second 抓取 btc.com第三頁交易資訊
    btc_driver_third.get('https://explorer.cloverpool.com/btc/unconfirmed-txs')
    btc_driver_third.maximize_window()
    # alert = WebDriverWait(btc_driver_third, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-modal"]/div/div[2]/div/div[4]/a'))
    # )
    # alert.click()
    # time.sleep(2)
    # btc_driver_third.execute_script("window.scrollTo(0, document.body.scrollHeight);") 
    # page_opthon = WebDriverWait(btc_driver_third, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[5]/nav/div/span/div'))
    # )
    # page_opthon.click()
    # show_100 = WebDriverWait(btc_driver_third, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-dropdown"]/div/div/div[4]'))
    # )
    # show_100.click() 
    # page_3 = WebDriverWait(btc_driver_third, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[5]/nav/li[4]/button'))
    # )
    # page_3.click() 
    time.sleep(10)

    # btc_driver_last = webdriver.Chrome(service=chrome_service)
    btc_driver_last = webdriver.Chrome()
    btc_driver_last.get('https://explorer.cloverpool.com/btc/unconfirmed-txs')
    btc_driver_last.maximize_window()
    # alert = WebDriverWait(btc_driver_last, 20).until(
    #     EC.presence_of_element_located((By.XPATH, '//*[@id="btccom-ui-modal"]/div/div[2]/div/div[4]/a'))
    # )
    # alert.click()
    time.sleep(10)
    btc_driver_last.execute_script("window.scrollTo(0, document.body.scrollHeight);")


    # ------------開始爬蟲程序------------

    split_count = 1000000000000000  # 輸入檔案切割數量
    interval = 60 # 單位為分鐘(多久儲存一次檔案)
    count = 1

    hashes_seen = deque(maxlen=1000000) # 初始化一個 deque 用來模擬 hashes_seen，儲存看過的交易hashe(設定 deque 的最大長度為 100000)
    header_written = False # 標題寫入的布林值

    while True:
        today_date = datetime.today().strftime('%Y_%m_%d')
        csv_file_name = f"test/BTX_Transaction_data_{today_date}_{count + 1}.csv"
        
        with open(csv_file_name, 'a', newline='', encoding='utf-8') as csvfile: # 'a' 代表 "append" 模式
            csv_writer = csv.writer(csvfile)
            if not header_written:
                csv_writer.writerow(["System Time", "Txn Hash","Txn Date", "Input Volume", "Output Volume", "Input Count", "Output Count", "Fees", "Total Txn Amount", "Final Txn Date"])
                header_written = True  # 設定為 True，表示標題已經被寫入

        timer = time.time() + interval * 60  # 設置計時器，以間隔時間為單位

        with ThreadPoolExecutor(max_workers=15) as executor:
            while time.time() < timer:
                try:
                    # 使用ThreadPoolExecutor來執行爬蟲函式，一次用五個pipeline去抓資料
                    executor.submit(btc_crawler, btc_driver_first, btc_driver_second, btc_driver_third, btc_driver_last, hashes_seen, header_written).result()
                except Exception as ex:
                    print(f"Error in threading: {ex}")

                if len(hashes_seen) > 1000: # 清系統記憶體deque快取
                    hashes_seen = deque(list(hashes_seen)[len(hashes_seen) // 2:], maxlen=1000000)
                    print('Cleaned hashes_seen')

        print(f'已經過 {count+1} 小時，第 {count+1} 次儲存')
        count += 1

print("Done BTX Web Data Scraping")
