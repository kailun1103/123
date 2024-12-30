import os
import time
import json
import requests
from selenium import webdriver
from datetime import datetime
from dateutil.parser import parse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException



def load_url(driver, hashes):
    for i, url in enumerate(hashes):
        driver.switch_to.window(driver.window_handles[i])
        driver.execute_script(f"window.location.href = '{url}';")


def get_null_state(driver):
    try:
        null_text = WebDriverWait(driver, 0.2).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/div[1]/h1'))
        ).text
        return null_text
    except TimeoutException:
        return None

def get_txn_state(txn_hash, driver):
    try:
        null = get_null_state(driver)
        if null == 'Invalid BTC Transaction':
            print('null')
            return 'Null'
        else:
            while True: # while True comfirmed webpage has been loaded
                format = WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/section/div[2]/div[3]/div[1]/div/div'))
                ).text
                if format == 'Position' or format == 'Age':
                    break
                else:
                    time.sleep(1.5)
                    print('wait')
                    null = get_null_state(driver)
                    if null == 'Invalid BTC Transaction':
                        print('null2')
                        return 'Null'
                    else:
                        format = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/section/div[2]/div[3]/div[1]/div/div'))
                        ).text
                        if format == 'Position' or format == 'Age':
                            break
                        print('connect fail, wai for reconnect')
                        driver.get(f'https://www.blockchain.com/explorer/transactions/btc/{txn_hash}')
                        time.sleep(2)
            state = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/div/section/div[1]/div[8]/section'))
            ).text
            return state
    except:
        print('impossible')
        return 'Null'
    
def switch_to_correct_page(txn_hash, driver):
    txn_hash = 'https://www.blockchain.com/explorer/transactions/btc/' + txn_hash
    for handle in driver.window_handles:
        driver.switch_to.window(handle)
        if driver.current_url == txn_hash:
            return 0
        
def get_txn_information(json_txn, driver, txn_state):       
    driver.execute_script("window.scrollBy(0, 300);")
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[1]/button[2]'))
    ).click()
    text_json = driver.execute_script("""
        return document.evaluate('//*[@id="__next"]/div[2]/div[2]/main/div/div/section/section/div[3]/div[2]/div/div/div/pre', 
        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent;
    """)
    text_json = json.loads(text_json)

    input_details = json.dumps([{
        "inputHash": item.get("address") or "Unknown_" + text_json['txid'][::3][:5],
        "amount": f"{item['value'] / 10**8:.8f}" if item['value'] != 0 else "0"
    } for item in text_json['inputs']])
    output_details = json.dumps([{
        "outputHash": item.get("address") or "Unknown_" + text_json['txid'][::3][:5],
        "amount": f"{item['value'] / 10**8:.8f}" if item['value'] != 0 else "0"
    } for item in text_json['outputs']])

    virtual_size = int(text_json['weight'] / 4) if text_json['weight'] / 4 % 1 == 0 else int(text_json['weight'] / 4) + 1

    if txn_state == 'Confirmed':
        headers = {'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'}
        base_url = f"https://www.oklink.com/api/v5/explorer/block/block-fills?chainShortName=btc&height={text_json['block']['height']}"
        response = requests.get(base_url, headers = headers)
        response_text = response.json()

    global Mempool_Final_Txn_Date
    block_txn = {}
    block_txn['Txn Initiation Date'] = json_txn['Txn Initiation Date'].replace('-','/')
    block_txn['Txn Input Amount'] = f"{float(json_txn['Txn Input Amount']):.8f}"
    block_txn['Txn Output Amount'] = f"{float(json_txn['Txn Output Amount']):.8f}"
    block_txn['Txn Fees'] = f"{float(json_txn['Txn Fees']):.8f}"
    block_txn['Mempool Final Txn Date'] = json_txn['Mempool Final Txn Date'].replace('-','/') if json_txn['Mempool Final Txn Date'] != '' else Mempool_Final_Txn_Date
    Mempool_Final_Txn_Date = json_txn['Mempool Final Txn Date']
    block_txn['Memory Depth'] = str(int( (parse(json_txn['Txn Initiation Date']) - parse(block_txn['Mempool Final Txn Date'] )).total_seconds() ))
    block_txn['Txn Weight'] = str(text_json['weight'])
    block_txn['Txn State'] = 'Pending' if txn_state == 'Pending' else 'Confirmed'
    block_txn['Txn Verification Date'] = 'null' if txn_state == 'Pending' else datetime.fromtimestamp((int(response_text['data'][0]['blockTime'])) / 1000).strftime('%Y/%m/%d %H:%M:%S')
    block_txn['Txn Input Details'] = input_details
    block_txn['Txn Output Details'] = output_details
    block_txn['Txn Fee Rate'] = f"{(float(json_txn['Txn Fees']) / float(virtual_size) * 100000000):.8f}"
    block_txn['Txn Fee Ratio'] = f"{((float(json_txn['Txn Fees'])/float(json_txn['Txn Output Amount']))*100):.8f}" if float(json_txn['Txn Output Amount']) != 0 else "0"
    block_txn['Miner Verification Time'] = 'null' if txn_state == 'Pending' else str(int( (parse(block_txn['Txn Verification Date']) - parse(json_txn['Txn Initiation Date'] )).total_seconds() ))
    block_txn['Dust Bool'] = "1" if float(json_txn['Txn Output Amount']) <= 0.00001 or (float(block_txn['Txn Fee Ratio']) >= 33 ) else "0"
    block_txn['Total Txn Size'] = str(text_json['size'])
    block_txn['Virtual Txn Size'] = str(virtual_size)
    block_txn['Block Height'] = "null" if txn_state == 'Pending' else str(text_json['block']['height'])

    return block_txn


# 初始化WebDriver
options = webdriver.ChromeOptions()
options.add_argument('--disable-background-networking')
options.add_argument('--disable-background-timer-throttling')
options.add_argument('--disable-client-side-phishing-detection')
options.add_argument('--disable-default-apps')
options.add_argument('--disable-hang-monitor')
options.add_argument('--disable-popup-blocking')
options.add_argument('--disable-prompt-on-repost')
options.add_argument('--disable-sync')
options.add_argument('--no-first-run')
options.add_argument('--safebrowsing-disable-auto-update')
options.add_argument('--start-maximized')
options.page_load_strategy = 'none'
driver = webdriver.Chrome(options=options)

for i in range(15):
    if i == 0:
        driver.get("about:blank")
    else:
        driver.execute_script("window.open('about:blank');")

json_file_path = 'new dataset(sort)'

start_time = time.time()
file_total = sum(len(files) for _, _, files in os.walk(json_file_path) if any(file.endswith('.json') for file in files))
file_counter = 1
Mempool_Final_Txn_Date = ''

for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            json_data = json.load(infile)
            batch_size = 15
            txn_count = 1
            txn_hashes = [txn["Txn Hash"] for txn in json_data if 'Txn Weight' not in txn]
            for i in range(0, len(txn_hashes), batch_size):
                batch_txn_hashes = txn_hashes[i:i+batch_size]
                
                # 修改 batch_txn_hashes 中的每個元素
                batch_txn_hashes_url = [f'https://www.blockchain.com/explorer/transactions/btc/{hash}' for hash in batch_txn_hashes]
                load_url(driver, batch_txn_hashes_url)
                for txn in batch_txn_hashes:
                    print('----------------------------------------------------')
                    try:
                        switch_to_correct_page(txn, driver)
                        txn_state = get_txn_state(txn, driver)
                        for json_txn in json_data:
                            if json_txn['Txn Hash'] == txn:
                                if txn_state == 'Confirmed' or txn_state == 'Pending':
                                    block_txn = get_txn_information(json_txn, driver, txn_state)
                                    json_txn['Txn Initiation Date'] = block_txn['Txn Initiation Date']
                                    json_txn['Txn Input Amount'] = block_txn['Txn Input Amount']
                                    json_txn['Txn Output Amount'] = block_txn['Txn Output Amount']
                                    json_txn['Txn Fees'] = block_txn['Txn Fees']
                                    json_txn['Mempool Final Txn Date'] = block_txn['Mempool Final Txn Date']
                                    json_txn['Memory Depth'] = block_txn['Memory Depth']
                                    json_txn['Txn Weight'] = block_txn['Txn Weight']
                                    json_txn['Txn State'] = block_txn['Txn State']
                                    json_txn['Txn Verification Date'] = block_txn['Txn Verification Date']
                                    json_txn['Txn Input Details'] = block_txn['Txn Input Details']
                                    json_txn['Txn Output Details'] = block_txn['Txn Output Details']
                                    json_txn['Txn Fee Rate'] = block_txn['Txn Fee Rate']
                                    json_txn['Txn Fee Ratio'] = block_txn['Txn Fee Ratio']
                                    json_txn['Miner Verification Time'] = block_txn['Miner Verification Time']
                                    json_txn['Dust Bool'] = block_txn['Dust Bool']
                                    json_txn['Total Txn Size'] = block_txn['Total Txn Size']
                                    json_txn['Virtual Txn Size'] = block_txn['Virtual Txn Size']
                                    json_txn['Block Height'] = block_txn['Block Height']
                                    print(f'{txn_count}/{len(txn_hashes)} / file: {file_counter}/{file_total}')

                                elif txn_state == 'Invalid' or txn_state == 'Null':
                                    json_txn['Txn State'] = 'error'
                                    print(f'{txn_count}/{len(txn_hashes)} / file: {file_counter}/{file_total}')
                                else:
                                    print('null')
                        txn_count += 1
                            
                    except Exception as ex:
                        print('continue')
                        print(ex)
                        continue
        
            with open(json_path, 'w', encoding='utf-8') as outfile:
                json.dump(json_data, outfile, ensure_ascii=False, indent=4)
            print(f'{file_counter}/{file_total}')  


            
        file_counter += 1

end_time = time.time()
total_time = end_time - start_time
print(f"Total time taken: {total_time:.2f} seconds")