import requests
import itertools
import json
import time
import random
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from oklink_api_key import oklink_keys

key_cycle = itertools.cycle(oklink_keys)

def create_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=0.1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods={"GET"}
    )
    adapter = HTTPAdapter(
        pool_connections=100,
        pool_maxsize=100,
        max_retries=retries
    )
    session.mount('https://', adapter)
    return session

def fetch_with_retry(session, url, headers, params, max_retries=5):
    for attempt in range(max_retries):
        try:
            # 添加隨機延遲
            time.sleep(random.uniform(0.1, 0.5))
            response = session.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            time.sleep(random.uniform(1, 2))  # 失敗後等待更長時間

def format_address_details(details, txn_hash):
    formatted = []
    for detail in details:
        hash_key = "inputHash" if "inputHash" in detail else "outputHash"
        formatted_detail = {
            hash_key: detail.get(hash_key) if detail.get(hash_key) else "OP_RETURN_" + txn_hash,
            "amount": f"{float(detail.get('amount')):.8f}" if detail.get('amount') != '0' else "0"
        }
        formatted.append(formatted_detail)
    return json.dumps(formatted, ensure_ascii=False)

def get_address_page(address):
    current_key = next(key_cycle)
    session = create_session()
    headers = {
        'Ok-Access-Key': current_key
    }
    payload = {
        "chainShortName": "btc",
        "address": address,
        "limit": 1,
        "page": 1
    }
    
    response_json = fetch_with_retry(
        session,
        "https://www.oklink.com/api/v5/explorer/address/transaction-list",
        headers,
        payload
    )
    
    return response_json['data'][0]['totalPage']

def fetch_hash_page(address, page):
    current_key = next(key_cycle)
    session = create_session()
    headers = {
        'Ok-Access-Key': current_key
    }
    payload = {
        "chainShortName": "btc",
        "address": address,
        "limit": 100,
        "page": page
    }
    
    response_json = fetch_with_retry(
        session,
        "https://www.oklink.com/api/v5/explorer/address/transaction-list",
        headers,
        payload
    )
    
    # print(f"Fetched page {page}")
    return [transaction['txId'] for transaction in response_json['data'][0]['transactionLists']]

def fetch_transaction_batch(txids_batch, current_key):
    session = create_session()
    headers = {
        'Ok-Access-Key': current_key
    }
    payload = {
        "chainShortName": "btc",
        "txid": txids_batch
    }
    
    response_json = fetch_with_retry(
        session,
        "https://www.oklink.com/api/v5/explorer/transaction/transaction-fills",
        headers,
        payload
    )
    
    txn_json = response_json['data']
    
    transaction_details = []
    for transaction in txn_json:
        txn_input_amount = float(transaction['amount'])
        txn_fee = float(transaction['txfee'])
        virtual_size = float(transaction['virtualSize'])
        fee_ratio = (txn_fee / (txn_input_amount - txn_fee)) * 100 if txn_input_amount != 0 else 0

        timestamp_ms = int(transaction['transactionTime'])
        timestamp_s = timestamp_ms // 1000  # 轉換為秒級時間戳
        date_str = datetime.fromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')   
        
        oklink_txn_item = {
            "Txn Hash": transaction["txid"],
            "Txn Input Address": str(len(transaction["inputDetails"])),
            "Txn Output Address": str(len(transaction["outputDetails"])),
            "Txn Input Amount": f"{txn_input_amount:.8f}",
            "Txn Output Amount": f"{(txn_input_amount - txn_fee):.8f}",
            "Txn Fee": f"{txn_fee:.8f}",
            "Txn Weight": transaction["weight"],
            "Txn Verification Timestamp": transaction["transactionTime"],
            "Txn Verification Date": date_str,
            "Txn Input Details": format_address_details(transaction['inputDetails'], transaction['txid']),
            "Txn Output Details": format_address_details(transaction['outputDetails'], transaction['txid']),
            "Txn Fee Rate": f"{(txn_fee / virtual_size * 100000000):.8f}",
            "Txn Fee Ratio": f"{fee_ratio:.8f}",
            "Dust Bool": str(int(fee_ratio >= 20)),
            "Total Txn Size": transaction['totalTransactionSize'],
            "Virtual Txn Size": transaction['virtualSize'],
            "Block Height": transaction['height']
        }
        transaction_details.append(oklink_txn_item)
    
    return transaction_details

def get_oklink_txn(address):
    # 首先檢查是否已有對應的 JSON 文件
    file_path = f'utxo_address/{address}_txn.json'
    if os.path.exists(file_path):
        print(f"Found existing file for {address}, loading from file...")
        with open(file_path, 'r') as infile:
            sorted_hashs_detail = json.load(infile)
            return sorted_hashs_detail
    
    # 如果沒有現有文件，則執行原本的邏輯
    print(f"No existing file found for {address}, fetching from API...")
    
    # step1. 獲取address_list的total_page數
    total_page = get_address_page(address)
    print(total_page)
    
    # step2. 平行化獲得地址的所有hash
    all_hashs_lists = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_page = {
            executor.submit(fetch_hash_page, address, page): page 
            for page in range(1, int(total_page) + 1)
        }
        
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                hash_list = future.result()
                all_hashs_lists.extend(hash_list)
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
    
    # step3. 平行化獲得交易詳細資訊
    all_hashs_detail = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_batch = {}
        for i in range(0, len(all_hashs_lists), 20):
            txids_batch = ','.join(all_hashs_lists[i:i+20])
            current_key = next(key_cycle)
            future = executor.submit(fetch_transaction_batch, txids_batch, current_key)
            future_to_batch[future] = i
        
        for future in as_completed(future_to_batch):
            batch_index = future_to_batch[future]
            try:
                transaction_details = future.result()
                all_hashs_detail.extend(transaction_details)
            except Exception as e:
                print(f"Error fetching batch starting at index {batch_index}: {e}")
    
    # 在寫入檔案前進行排序
    sorted_hashs_detail = sorted(all_hashs_detail, 
                                key=lambda x: int(x['Txn Verification Timestamp']), 
                                reverse=False)

    # 將排序後的結果寫入檔案
    os.makedirs('utxo_address', exist_ok=True)  # 確保目錄存在
    with open(file_path, 'w') as outfile:
        json.dump(sorted_hashs_detail, outfile, indent=2)

    return sorted_hashs_detail

def find_matching_transaction(sorted_hashs_detail, target_timestamp, amount):
    # 讀取已找到的交易
    with open('oklink_block_txn/found_transactions.json', 'r') as f:
        found_transactions = json.load(f)
    
    # 取出已找到的交易 hash 列表
    found_txn_hashes = [tx["txn"] for tx in found_transactions]
    
    target_timestamp = int(target_timestamp)
    
    # 找出符合條件且未被找到過的交易，同時檢查金額
    valid_transactions = []
    for txn in sorted_hashs_detail:
        if (int(txn['Txn Verification Timestamp']) >= target_timestamp
            and txn['Dust Bool'] == "0" 
            and txn['Txn Hash'] not in found_txn_hashes):
            
            # 檢查交易金額
            input_details = json.loads(txn['Txn Input Details'])
            if any(float(detail['amount']) == amount for detail in input_details):
                valid_transactions.append(txn)
    
    # print('符合交易筆數:', len(valid_transactions))
    
    if valid_transactions:
        # 一定會取最接近的那一筆
        closest_transaction = min(valid_transactions, 
                                key=lambda x: abs(int(x['Txn Verification Timestamp']) - target_timestamp))
        
        # 只把交易哈希添加到 found_transactions
        found_transactions.append({"txn": closest_transaction['Txn Hash']})
        with open('oklink_block_txn/found_transactions.json', 'w') as f:
            json.dump(found_transactions, f, indent=4)
            
        return closest_transaction  # 返回完整的交易信息
    return None

if __name__ == "__main__":
    target_address = "1LnoZawVFFQihU8d8ntxLMpYheZUfyeVAK"
    
    start_time = time.time()
    sorted_hashs_detail = get_oklink_txn(target_address)
    
    target_timestamp = "1727067016000"
    amount = 0.00000546
    
    result = find_matching_transaction(sorted_hashs_detail, target_timestamp, amount)
    
    if result:
        print(f"Found matching transaction: {result}")
    else:
        print("No matching transaction found")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    # print(f"運行時間: {elapsed_time:.2f} 秒")