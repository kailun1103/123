import requests
import csv
import json
import time
import itertools
from datetime import datetime
from oklink_api_key import oklink_keys

key_cycle = itertools.cycle(oklink_keys)
total_dust_count = 0
total_normal_count = 0
total_txn_count = 0

loop_count = 1
address_txn = []

def format_address_details(details, txn_hash):
    formatted = []
    for detail in details:
        hash_key = "inputHash" if "inputHash" in detail else "outputHash"
        formatted_detail = {
            hash_key: detail.get(hash_key) if detail.get(hash_key) else "Unknown_" + txn_hash,  # 稍微改一下邏輯
            "amount": f"{float(detail.get('amount')):.8f}" if detail.get('amount') != '0' else "0"
        }
        formatted.append(formatted_detail)
    return json.dumps(formatted, ensure_ascii=False)

def count_lines(file_path):
    with open(file_path, 'r', newline='', encoding='cp1252') as f:
        return sum(1 for line in f) - 1  # 減去標題行


# 開始處理address以及分類
file_path = '20241007_address_txn_statistics.csv'
total_lines = count_lines(file_path)
with open(file_path, 'r', newline='', encoding='cp1252') as file:
    csv_reader = csv.reader(file)
    next(csv_reader)
    for index, row in enumerate(csv_reader, start=1):
        print(f"{index}/{total_lines - index}")
        address = row[0]
        while True:
            address_payload = {
                "chainShortName": "btc",
                "address": address,  # 假設 address 變量已經定義
                "limit": 100,
                "page": loop_count
            }

            current_key = next(key_cycle)
            address_headers = {'Ok-Access-Key': current_key}
            address_response = requests.get("https://www.oklink.com//api/v5/explorer/address/transaction-list", headers=address_headers, params=address_payload).json()
            
            # 遍歷交易列表並提取每個 txId
            for i in range(len(address_response['data'][0]['transactionLists'])):
                txId = address_response['data'][0]['transactionLists'][i]['txId']
                address_txn.append({"Txn Hash": txId})

            # 把詳細資料填補到address_txn裡面
            for i in range(0, len(address_txn), 20):
                dust_count = 0
                normal_count = 0
                txids = [entry['Txn Hash'] for entry in address_txn[i:i+20]]
                txids_batch = ','.join(txids)

                payload = {
                    "chainShortName": "btc",
                    "txid": txids_batch
                }
                current_key = next(key_cycle)
                txn_headers = {'Ok-Access-Key': current_key}
                txn_response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=txn_headers, params=payload)

                while True:
                    if txn_response.status_code == 200:
                        break
                    elif txn_response.status_code == 429:
                        current_key = next(key_cycle)
                        txn_headers = {'Ok-Access-Key': current_key}
                        time.sleep(2)
                        txn_response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=txn_headers, params=payload)


                response_data = txn_response.json()
                for oklink_txn in response_data['data']:
                    for json_txn in address_txn:
                        if json_txn['Txn Hash'] == oklink_txn['txid']:
                            json_txn['Txn Input Amount'] = f"{float(oklink_txn['amount']):.8f}"
                            json_txn['Txn Output Amount'] = f"{(float(oklink_txn['amount']) - float(oklink_txn['txfee'])):.8f}"
                            json_txn['Txn Input Address'] = str(len((oklink_txn['inputDetails'])))
                            json_txn['Txn Output Address'] = str(len((oklink_txn['outputDetails'])))
                            json_txn['Txn Fee'] = f"{float(oklink_txn['txfee']):.8f}"
                            json_txn['Txn Weight'] = oklink_txn['weight']
                            json_txn['Txn State'] = "Confirmed" if oklink_txn['state'] == 'success' else  ('Pending' if oklink_txn['state'] == 'pending' else 'null')
                            json_txn['Txn Verification Date'] = datetime.fromtimestamp((int(oklink_txn['transactionTime'])) / 1000).strftime('%Y/%m/%d %H:%M:%S')  if oklink_txn['state'] == 'success' else 'null'
                            json_txn['Txn Input Details'] = format_address_details(oklink_txn['inputDetails'], json_txn['Txn Hash'])
                            json_txn['Txn Output Details'] = format_address_details(oklink_txn['outputDetails'], json_txn['Txn Hash'])
                            json_txn['Txn Fee Rate'] = f"{(float(oklink_txn['txfee']) / float(oklink_txn['virtualSize']) * 100000000):.8f}"
                            json_txn['Txn Fee Ratio'] = f"{((float(json_txn['Txn Fee'])/float(json_txn['Txn Output Amount']))*100):.8f}" if float(json_txn['Txn Output Amount']) != 0 else "0"
                            json_txn['Dust Bool'] = "1" if float(json_txn['Txn Fee Ratio']) >= 20 else "0"
                            json_txn['Total Txn Size'] = oklink_txn['totalTransactionSize']
                            json_txn['Virtual Txn Size'] = oklink_txn['virtualSize']
                            json_txn['Block Height'] = oklink_txn['height'] if oklink_txn['state'] == 'success' else 'null'

            total_txn_count += len(address_response['data'][0]['transactionLists'])

            if int(address_response['data'][0]['totalPage']) == 1 or loop_count == int(address_response['data'][0]['totalPage']):
                for json_txn in address_txn:
                    if json_txn['Dust Bool'] == "1":
                        total_dust_count += 1
                    else:
                        total_normal_count += 1
                break
            else:
                loop_count += 1
                print(f'還有{int(address_response['data'][0]['totalPage'])-loop_count}頁')

        print(f"總交易數: {total_txn_count}(D:{total_dust_count}/N: {total_normal_count})")

        if total_normal_count == 0:
            if total_dust_count == 1:
                file_path = 'gcn_dataset for address/all dust/1'
            elif total_dust_count > 1 and total_dust_count <= 10:
                file_path = 'gcn_dataset for address/all dust/1 - 10'
            elif total_dust_count > 10 and total_dust_count <= 50:
                file_path = 'gcn_dataset for address/all dust/10 - 50'
            elif total_dust_count > 50 and total_dust_count <= 100:
                file_path = 'gcn_dataset for address/all dust/50 - 100'
            else:
                file_path = 'gcn_dataset for address/all dust/100 up'

        if total_dust_count == 0:
            if total_normal_count == 1:
                file_path = 'gcn_dataset for address/all normal/1'
            elif total_normal_count > 1 and total_normal_count <= 10:
                file_path = 'gcn_dataset for address/all normal/1 - 10'
            elif total_normal_count > 10 and total_normal_count <= 50:
                file_path = 'gcn_dataset for address/all normal/10 - 50'
            elif total_normal_count > 50 and total_normal_count <= 100:
                file_path = 'gcn_dataset for address/all normal/50 - 100'
            else:
                file_path = 'gcn_dataset for address/all normal/100 up'

        if total_dust_count >= 1 and total_normal_count >= 1:
            if total_txn_count == 1:
                file_path = 'gcn_dataset for address/all txn/1'
            elif total_txn_count > 1 and total_txn_count <= 10:
                file_path = 'gcn_dataset for address/all txn/1 - 10'
            elif total_txn_count > 10 and total_txn_count <= 50:
                file_path = 'gcn_dataset for address/all txn/10 - 50'
            elif total_txn_count > 50 and total_txn_count <= 100:
                file_path = 'gcn_dataset for address/all txn/50 - 100'
            else:
                file_path = 'gcn_dataset for address/all txn/100 up'

        file_name = f'{file_path}/{total_txn_count}(D-{total_dust_count} N-{total_normal_count})_{address}.json'

        with open(file_name, 'w', encoding='utf-8') as f:
            json.dump(address_txn, f, ensure_ascii=False, indent=2)