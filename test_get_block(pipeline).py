import requests
import sys
import json
import time
from collections import defaultdict
import itertools
from concurrent.futures import ThreadPoolExecutor, as_completed
from oklink_api_key import oklink_keys

key_cycle = itertools.cycle(oklink_keys)

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

def get_block_page(block):
    current_key = next(key_cycle)
    headers = {
        'Ok-Access-Key': current_key
    }
    payload = {
        "chainShortName": "btc",
        "height": block,
        "protocolType": "transaction",
        "limit": 100,
        "page": 1
    }
    response = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers, params=payload)
    print(response.status_code)
    response_json = response.json()
    print(response_json)
    total_page = response_json['data'][0]['totalPage']
    return total_page

def fetch_hash_page(block, page):
    current_key = next(key_cycle)
    headers = {
        'Ok-Access-Key': current_key
    }
    payload = {
        "chainShortName": "btc",
        "height": block,
        "protocolType": "transaction",
        "limit": 100,
        "page": page
    }
    
    while True:
        response = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers, params=payload)
        if response.status_code == 200:
            break
        current_key = next(key_cycle)
        headers['Ok-Access-Key'] = current_key
    
    print(f"Fetched page {page}, status: {response.status_code}")
    response_json = response.json()
    return response_json['data'][0]['blockList']

def fetch_transaction_batch(txids_batch, current_key):
    headers = {
        'Ok-Access-Key': current_key
    }
    payload = {
        "chainShortName": "btc",
        "txid": txids_batch
    }
    
    while True:
        response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", 
                              headers=headers, params=payload)
        if response.status_code == 200:
            break
        current_key = next(key_cycle)
        headers['Ok-Access-Key'] = current_key
    
    response_json = response.json()
    txn_json = response_json['data']
    
    transaction_details = []
    for transaction in txn_json:
        txn_input_amount = float(transaction['amount'])
        if txn_input_amount == 0:
            continue
        txn_fee = float(transaction['txfee'])
        virtual_size = float(transaction['virtualSize'])
        fee_ratio = (txn_fee / (txn_input_amount - txn_fee)) * 100 if txn_input_amount != 0 else 0
        
        oklink_txn_item = {
            "Txn Hash": transaction["txid"],
            "Txn Input Address": str(len(transaction["inputDetails"])),
            "Txn Output Address": str(len(transaction["outputDetails"])),
            "Txn Input Amount": f"{txn_input_amount:.8f}",
            "Txn Output Amount": f"{(txn_input_amount - txn_fee):.8f}",
            "Txn Fee": f"{txn_fee:.8f}",
            "Txn Weight": transaction["weight"],
            "Txn Verification Date": transaction["transactionTime"],
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

def get_oklink_txn(block):
    # step1. 獲取block_list的total_page數
    total_page = get_block_page(block)
    
    # step2. 平行化獲得區塊的所有hash
    all_hashs_lists = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_page = {
            executor.submit(fetch_hash_page, block, page): page 
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
    with ThreadPoolExecutor(max_workers=20) as executor:
        # 將交易分批，每批20個
        future_to_batch = {}
        for i in range(0, len(all_hashs_lists), 20):
            txids = [entry['txid'] for entry in all_hashs_lists[i:i+20]]
            txids_batch = ','.join(txids)
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
    
    print(len(all_hashs_detail))
    
    with open(f'oklink_block_txn/{block}_txn.json', 'w') as outfile:
        json.dump(all_hashs_detail, outfile, indent=2)

if __name__ == '__main__':
    start_block = int(sys.argv[1])
    end_block = int(sys.argv[2])
    
    start_time = time.time()
    for block in range(start_block, end_block):
        get_oklink_txn(block)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"運行時間: {elapsed_time:.2f} 秒")