import requests
import csv
import json
import time
import itertools
from datetime import datetime
from oklink_api_key import oklink_keys
from multiprocessing import Pool, Manager, cpu_count
from functools import partial

key_cycle = itertools.cycle(oklink_keys)

def format_address_details(details, txn_hash):
    return json.dumps([{
        "inputHash" if "inputHash" in detail else "outputHash": detail.get("inputHash" if "inputHash" in detail else "outputHash") or f"Unknown_{txn_hash}",
        "amount": f"{float(detail.get('amount', 0)):.8f}"
    } for detail in details], ensure_ascii=False)

def count_lines(file_path):
    with open(file_path, 'r', newline='', encoding='cp1252') as f:
        return sum(1 for _ in f) - 1

def get_api_response(url, headers, params, max_retries=5):
    for _ in range(max_retries):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            headers['Ok-Access-Key'] = next(key_cycle)
            time.sleep(2)
            print('response error')
    raise Exception(f"Failed to get response after {max_retries} attempts")

def process_address(address, shared_counters):
    total_dust_count = total_normal_count = 0
    address_txn = []
    processed_txns = []
    page = 1

    while True:
        address_payload = {
            "chainShortName": "btc",
            "address": address,
            "limit": 100,
            "page": page
        }
        address_headers = {'Ok-Access-Key': next(key_cycle)}
        address_response = get_api_response("https://www.oklink.com/api/v5/explorer/address/transaction-list", address_headers, address_payload)
        
        new_txns = [{"Txn Hash": txn['txId']} for txn in address_response['data'][0]['transactionLists']]
        address_txn.extend(new_txns)

        if len(new_txns) < 100 or page == int(address_response['data'][0]['totalPage']):
            break
        page += 1

    for i in range(0, len(address_txn), 20):
        txids_batch = ','.join(txn['Txn Hash'] for txn in address_txn[i:i+20])
        payload = {
            "chainShortName": "btc",
            "txid": txids_batch
        }
        txn_headers = {'Ok-Access-Key': next(key_cycle)}
        response_data = get_api_response("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", txn_headers, payload)
        
        for oklink_txn in response_data['data']:
            for json_txn in address_txn:
                if json_txn['Txn Hash'] == oklink_txn['txid']:
                    txn_output_amount = float(oklink_txn['amount']) - float(oklink_txn['txfee'])
                    processed_txn = {
                        'Txn Hash' : oklink_txn['txid'],
                        'Txn Input Amount': f"{float(oklink_txn['amount']):.8f}",
                        'Txn Output Amount': f"{txn_output_amount:.8f}",
                        'Txn Input Address': str(len(oklink_txn['inputDetails'])),
                        'Txn Output Address': str(len(oklink_txn['outputDetails'])),
                        'Txn Fee': f"{float(oklink_txn['txfee']):.8f}",
                        'Txn Weight': oklink_txn['weight'],
                        'Txn State': "Confirmed" if oklink_txn['state'] == 'success' else ('Pending' if oklink_txn['state'] == 'pending' else 'null'),
                        'Txn Verification Date': datetime.fromtimestamp(int(oklink_txn['transactionTime']) / 1000).strftime('%Y/%m/%d %H:%M:%S') if oklink_txn['state'] == 'success' else 'null',
                        'Txn Input Details': format_address_details(oklink_txn['inputDetails'], json_txn['Txn Hash']),
                        'Txn Output Details': format_address_details(oklink_txn['outputDetails'], json_txn['Txn Hash']),
                        'Txn Fee Rate': f"{(float(oklink_txn['txfee']) / float(oklink_txn['virtualSize']) * 100000000):.8f}",
                        'Total Txn Size': oklink_txn['totalTransactionSize'],
                        'Virtual Txn Size': oklink_txn['virtualSize'],
                        'Block Height': oklink_txn['height'] if oklink_txn['state'] == 'success' else 'null'
                    }

                    fee_ratio = ((float(oklink_txn['txfee']) / txn_output_amount) * 100) if txn_output_amount != 0 else 0
                    processed_txn['Txn Fee Ratio'] = f"{fee_ratio:.8f}"
                    processed_txn['Dust Bool'] = "1" if fee_ratio >= 20 else "0"

                    processed_txns.append(processed_txn)
                    
                    if processed_txn['Dust Bool'] == "1":
                        total_dust_count += 1
                    else:
                        total_normal_count += 1


    total_txn_count = len(processed_txns)

    with shared_counters['lock']:
        shared_counters['total_dust_count'] += total_dust_count
        shared_counters['total_normal_count'] += total_normal_count
        shared_counters['total_txn_count'] += total_txn_count
        shared_counters['processed_count'] += 1
        current_count = shared_counters['processed_count']
        total_lines = shared_counters['total_lines']
        print(f"Processed {current_count}/{total_lines} .{address}-->{total_txn_count}(D:{total_dust_count}/N: {total_normal_count})")

    # 文件路径逻辑保持不变
    if total_normal_count == 0:
        if total_dust_count == 1:
            file_path = 'gcn_dataset for address/all dust/1'
        elif 1 < total_dust_count <= 10:
            file_path = 'gcn_dataset for address/all dust/1 - 10'
        elif 10 < total_dust_count <= 50:
            file_path = 'gcn_dataset for address/all dust/10 - 50'
        elif 50 < total_dust_count <= 100:
            file_path = 'gcn_dataset for address/all dust/50 - 100'
        else:
            file_path = 'gcn_dataset for address/all dust/100 up'
    elif total_dust_count == 0:
        if total_normal_count == 1:
            file_path = 'gcn_dataset for address/all normal/1'
        elif 1 < total_normal_count <= 10:
            file_path = 'gcn_dataset for address/all normal/1 - 10'
        elif 10 < total_normal_count <= 50:
            file_path = 'gcn_dataset for address/all normal/10 - 50'
        elif 50 < total_normal_count <= 100:
            file_path = 'gcn_dataset for address/all normal/50 - 100'
        else:
            file_path = 'gcn_dataset for address/all normal/100 up'
    else:
        if total_txn_count == 1:
            file_path = 'gcn_dataset for address/all txn/1'
        elif 1 < total_txn_count <= 10:
            file_path = 'gcn_dataset for address/all txn/1 - 10'
        elif 10 < total_txn_count <= 50:
            file_path = 'gcn_dataset for address/all txn/10 - 50'
        elif 50 < total_txn_count <= 100:
            file_path = 'gcn_dataset for address/all txn/50 - 100'
        else:
            file_path = 'gcn_dataset for address/all txn/100 up'

    file_name = f'{file_path}/{total_txn_count}(D-{total_dust_count} N-{total_normal_count})_{address}.json'

    with open(file_name, 'w', encoding='utf-8') as f:
        json.dump(processed_txns, f, ensure_ascii=False, indent=2)

    return f"Processed {address}: {total_txn_count} transactions (Dust: {total_dust_count}, Normal: {total_normal_count})"

def main():
    file_path = 'output_unique_hashes_filtered.json'

    manager = Manager()
    shared_counters = manager.dict({
        'total_dust_count': 0,
        'total_normal_count': 0,
        'total_txn_count': 0,
        'processed_count': 0,
        'total_lines': 39131,
        'lock': manager.Lock()
    })

    with open(file_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        addresses = [item['address'] for item in json_data]

    # 使用系统的CPU核心数来设置进程池大小
    with Pool(processes=cpu_count()) as pool:
        process_func = partial(process_address, shared_counters=shared_counters)
        results = pool.map(process_func, addresses)

    print(f"Total transactions: {shared_counters['total_txn_count']} (Dust: {shared_counters['total_dust_count']}, Normal: {shared_counters['total_normal_count']})")
    print(f"Processed {shared_counters['processed_count']}/{39131} addresses")

if __name__ == "__main__":
    main()