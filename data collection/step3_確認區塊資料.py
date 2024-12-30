import requests
import time
import os
import json
import statistics
import itertools
from oklink_api_key import oklink_keys
import concurrent.futures
import requests
import itertools
from collections import deque

key_cycle = itertools.cycle(oklink_keys)

def process_batch(batch, key_cycle):
    txids = [entry['txid'] for entry in batch if isinstance(entry, dict) and 'txid' in entry]
    txids_batch = ','.join(txids)

    current_key = next(key_cycle)
    payload = {
        "chainShortName": "btc",
        "txid": txids_batch
    }
    headers_fast = {'Ok-Access-Key': current_key}
    
    max_retries = 15
    for _ in range(max_retries):
        try:
            response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", 
                                    headers=headers_fast, params=payload, timeout=10)
            response.raise_for_status()
            break
        except requests.RequestException as e:
            time.sleep(1)
            print(f"Request failed, retrying... Error: {e}")
            current_key = next(key_cycle)
            headers_fast = {'Ok-Access-Key': current_key}
    else:
        print("errorrrrrrr1")
        return []

    response_json = response.json()
    if 'data' not in response_json or not response_json['data']:
        print("errorrrrrrr2")
        return []

    return [
        {
            "TotalSize": transaction["totalTransactionSize"],
            "VirtualSize": transaction["virtualSize"],
            'Weight': transaction['weight'],
            "FeeRate": str((float(transaction['txfee'])/float(transaction['virtualSize']))*100000000)
        }
        for transaction in response_json['data']
    ]

def fetch_block_hashes(page, block, key_cycle):
    current_key = next(key_cycle)
    headers_fast = {'Ok-Access-Key': current_key}
    payload = {
        "chainShortName": "btc",
        "height": block,
        "protocolType": "transaction",
        "limit": 100,
        "page": page
    }
    
    max_retries = 15
    for _ in range(max_retries):
        try:
            response = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", 
                                    headers=headers_fast, params=payload, timeout=10)
            response.raise_for_status()
            response_json = response.json()
            return response_json['data'][0]['blockList']
        except requests.RequestException as e:
            time.sleep(1)
            print(f"Request failed, retrying... Error: {e}")
            current_key = next(key_cycle)
            headers_fast = {'Ok-Access-Key': current_key}
    
    print("errorrrrrrr3")
    return []

def calculate_percentiles(data):
    percentiles = []
    
    percentile_value = round(statistics.quantiles(data, n=100)[0], 2)
    percentiles.append({"1%": str(percentile_value)})
    
    # Calculate 5th to 95th percentiles
    for p in range(5, 100, 5):
        percentile_value = round(statistics.quantiles(data, n=100)[p-1], 2)
        percentiles.append({f"{p}%": str(percentile_value)})
    
    # Calculate 99th percentile
    percentile_value = round(statistics.quantiles(data, n=100)[98], 2)
    percentiles.append({"99%": str(percentile_value)})
    
    # Convert the result to a JSON formatted string
    return json.dumps(percentiles)


def get_block_inf(block, file_counter, file_total):
    with open('temp2.json', 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        for json_txn in json_data:
            if int(json_txn['Block Height']) == block:
                return json_txn
            
        print(f'not exist block: {block}, file: {file_counter}/{file_total}')
        global key_cycle

        current_key = next(key_cycle)
        headers_fast = {'Ok-Access-Key': current_key}
        payload = {
            "chainShortName": "btc",
            "height": block,
            "protocolType": "transaction",
            "limit": 100,
            "page": 1
        }
        
        # step0. 獲得區塊列表的page
        response_block_list = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers_fast, params=payload)
        while True:
            if response_block_list.status_code == 200:
                response_block_list = response_block_list.json()
                break
            elif response_block_list.status_code == 429:
                print(response_block_list.status_code)
                current_key = next(key_cycle)
                headers_fast = {'Ok-Access-Key': current_key}
                response_block_list = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers_fast, params=payload)
        total_page = response_block_list['data'][0]['totalPage']

        # step1. 獲得區塊的資訊
        response_block_inf = requests.get(f"https://www.oklink.com/api/v5/explorer/block/block-fills?chainShortName=btc&height={block}", headers = headers_fast)
        while True:
            if response_block_inf.status_code == 200:
                response_block_inf = response_block_inf.json()
                break
            elif response_block_inf.status_code == 429:
                print(response_block_inf.status_code)
                current_key = next(key_cycle)
                headers_fast = {'Ok-Access-Key': current_key}
                response_block_inf = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers_fast, params=payload)
        
        # step2. 獲得區塊的所有hash
        all_hashs_lists = []
        key_cycle = itertools.cycle(oklink_keys)

        with concurrent.futures.ThreadPoolExecutor(max_workers=14) as executor:
            future_to_page = {executor.submit(fetch_block_hashes, i, block, key_cycle): i 
                            for i in range(1, int(total_page) + 1)}
            for future in concurrent.futures.as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    hashes = future.result()
                    all_hashs_lists.extend(hashes)
                except Exception as exc:
                    print(f"Page {page} generated an exception: {exc}")

        # print(f"Total hashes fetched: {len(all_hashs_lists)}")

        batch_size = 20
        batches = [all_hashs_lists[i:i+batch_size] for i in range(0, len(all_hashs_lists), batch_size)]

        all_hashs_detail = []
        key_cycle = itertools.cycle(oklink_keys)
        
        # 使用線程池處理批次
        with concurrent.futures.ThreadPoolExecutor(max_workers=22) as executor:
            future_to_batch = {executor.submit(process_batch, batch, key_cycle): batch for batch in batches}
            for future in concurrent.futures.as_completed(future_to_batch):
                all_hashs_detail.extend(future.result())

        # print(f"Total processed transactions: {len(all_hashs_detail)}")

        total_sizes = []
        virtual_sizes = []
        weights = []
        fee_rates = []

        for transaction in all_hashs_detail:
            total_sizes.append(float(transaction['TotalSize']))
            virtual_sizes.append(float(transaction['VirtualSize']))
            weights.append(float(transaction['Weight']))
            fee_rates.append(float(transaction['FeeRate']))

        total_size_percentiles = calculate_percentiles(total_sizes)
        virtual_size_percentiles = calculate_percentiles(virtual_sizes)
        weight_percentiles = calculate_percentiles(weights)
        fee_rate_percentiles = calculate_percentiles(fee_rates)

        block_inf = {}
        block_inf = {
            'Block Weight' : weight_percentiles,
            'Block Total Txn Size' : total_size_percentiles,
            'Block Virtual Txn Size' : virtual_size_percentiles,
            'Block Fee Rate' : fee_rate_percentiles,
            'Block Height': response_block_inf['data'][0]['height'],
            'Block Hash': response_block_inf['data'][0]['hash'],
            'Block Validator': response_block_inf['data'][0]['validator'],
            'Block Date': response_block_inf['data'][0]['blockTime'],
            'Block Txn Count': response_block_inf['data'][0]['txnCount'],
            'Block Txn Amount': response_block_inf['data'][0]['amount'],
            'Block Size': response_block_inf['data'][0]['blockSize'],
            'Miner Reward': response_block_inf['data'][0]['mineReward'],
            'Block Txn Fees': response_block_inf['data'][0]['totalFee'],
            'Block Merkle Root Hash': response_block_inf['data'][0]['merkleRootHash'],
            'Block Miner Hash': response_block_inf['data'][0]['miner'],
            'Block Difficulty': response_block_inf['data'][0]['difficuity'],
            'Block Nonce': response_block_inf['data'][0]['nonce'],
            'Block Confirm': response_block_inf['data'][0]['confirm']
        }
        with open('temp2.json', 'r', encoding='utf-8') as infile:
            data = json.load(infile)
        data.append(block_inf)
        with open('temp2.json', 'w', encoding='utf-8') as outfile:
            json.dump(data, outfile, ensure_ascii=False, indent=4)
        
        return block_inf
    

if __name__ == '__main__':
    start_time = time.time()
    # json_file_path = "0720-0811"
    json_file_path = "0619-0811"
    file_total = sum(len(files) for _, _, files in os.walk(json_file_path) if any(file.endswith('.json') for file in files))
    file_counter = 1
    
    # 初始化一個最大長度為5的雙端隊列
    temp_block_inf = deque(maxlen=15)

    for root, dirs, files in os.walk(json_file_path):
        json_files = [file for file in files if file.endswith('.json')]
        for json_file in json_files:
            print(json_file)
            txn_counter = 1
            json_path = os.path.join(root, json_file)
            with open(json_path, 'r', encoding='utf-8') as infile:
                json_data = json.load(infile)
                for json_txn in json_data:
                    if "Block Height" in json_txn and json_txn['Txn State'] == 'Confirmed':
                        # if 'Block Weight' in json_txn:
                        #     if len(json_txn["Block Weight"]) > 20:
                        #         continue
                        
                        # 檢查是否有匹配的 block_inf 在 temp_block_inf 中
                        matching_block_inf = next((block for block in temp_block_inf if int(block['Block Height']) == int(json_txn['Block Height'])), None)
                        
                        if matching_block_inf:
                            json_txn['Block Hash'] = matching_block_inf['Block Hash']
                            json_txn['Block Weight'] = matching_block_inf['Block Weight']
                            json_txn['Block Fee Rate'] = matching_block_inf['Block Fee Rate']
                            json_txn['Block Total Txn Size'] = matching_block_inf['Block Total Txn Size']
                            json_txn['Block Virtual Txn Size'] = matching_block_inf['Block Virtual Txn Size']
                            json_txn['Block Validator'] = matching_block_inf['Block Validator']
                            json_txn['Block Date'] = matching_block_inf['Block Date']
                            json_txn['Block Txn Count'] = matching_block_inf['Block Txn Count']
                            json_txn['Block Txn Amount'] = matching_block_inf['Block Txn Amount']
                            json_txn['Block Size'] = matching_block_inf['Block Size']
                            json_txn['Miner Reward'] = matching_block_inf['Miner Reward']
                            json_txn['Block Txn Fees'] = matching_block_inf['Block Txn Fees']
                            json_txn['Block Merkle Root Hash'] = matching_block_inf['Block Merkle Root Hash']
                            json_txn['Block Miner Hash'] = matching_block_inf['Block Miner Hash']
                            json_txn['Block Difficulty'] = matching_block_inf['Block Difficulty']
                            json_txn['Block Nonce'] = matching_block_inf['Block Nonce']
                            json_txn['Block Confirm'] = matching_block_inf['Block Confirm']

                        else:
                            # 如果沒有找到匹配的，獲取新的 block_inf 並添加到 deque 中
                            block_inf = get_block_inf(int(json_txn['Block Height']), file_counter, file_total)
                            temp_block_inf.append(block_inf)
                            
                            json_txn['Block Hash'] = block_inf['Block Hash']
                            json_txn['Block Weight'] = block_inf['Block Weight']
                            json_txn['Block Fee Rate'] = block_inf['Block Fee Rate']
                            json_txn['Block Total Txn Size'] = block_inf['Block Total Txn Size']
                            json_txn['Block Virtual Txn Size'] = block_inf['Block Virtual Txn Size']
                            json_txn['Block Validator'] = block_inf['Block Validator']
                            json_txn['Block Date'] = block_inf['Block Date']
                            json_txn['Block Txn Count'] = block_inf['Block Txn Count']
                            json_txn['Block Txn Amount'] = block_inf['Block Txn Amount']
                            json_txn['Block Size'] = block_inf['Block Size']
                            json_txn['Miner Reward'] = block_inf['Miner Reward']
                            json_txn['Block Txn Fees'] = block_inf['Block Txn Fees']
                            json_txn['Block Merkle Root Hash'] = block_inf['Block Merkle Root Hash']
                            json_txn['Block Miner Hash'] = block_inf['Block Miner Hash']
                            json_txn['Block Difficulty'] = block_inf['Block Difficulty']
                            json_txn['Block Nonce'] = block_inf['Block Nonce']
                            json_txn['Block Confirm'] = block_inf['Block Confirm']
            
            print(f"file: {file_counter}/{file_total}")
            with open(json_path, 'w', encoding='utf-8') as outfile:
                json.dump(json_data, outfile, ensure_ascii=False, indent=4)

            file_counter += 1

    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total time taken: {total_time:.2f} seconds")