import os
import json
import time
import requests
from datetime import datetime
import itertools
from dateutil.parser import parse
from oklink_api_key import oklink_keys

key_cycle = itertools.cycle(oklink_keys)

json_file_path = 'test'

def format_address_details(details, txn_hash):
    formatted = []
    for detail in details:
        hash_key = "inputHash" if "inputHash" in detail else "outputHash"
        formatted_detail = {
            hash_key: detail.get(hash_key) if detail.get(hash_key) else "Unknown_" + txn_hash[::3][:5],
            "amount": f"{float(detail.get('amount')):.8f}" if detail.get('amount') != '0' else "0"
        }
        formatted.append(formatted_detail)
    return json.dumps(formatted, ensure_ascii=False)

start_time = time.time()
file_total = sum(len(files) for _, _, files in os.walk(json_file_path) if any(file.endswith('.json') for file in files))
file_counter = 1
Mempool_Final_Txn_Date = ''

for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            txn_counter = 1
            json_data = json.load(infile)
            batch_size = 20
            txn_hashes = [txn["Txn Hash"] for txn in json_data]
            for i in range(0, len(txn_hashes), batch_size):
                current_key = next(key_cycle)
                headers = {'Ok-Access-Key': current_key}
                batch_txn_hashes = txn_hashes[i:i+batch_size]
                payload = {
                    "chainShortName": "btc",
                    "txid": ",".join(batch_txn_hashes)
                }
                response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
                while True:
                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        print(response.status_code)
                        print('fail!!!!!!!!!!!!!!!!')
                        time.sleep(0.5)
                        response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
                        
                response_data = response.json()
                
                for oklink_txn in response_data['data']:
                    for json_txn in json_data:
                        if json_txn['Txn Hash'] == oklink_txn['txid']:
                            txn_counter += 1
                            # print(json_txn['Txn Hash'])
                            json_txn['Txn Initiation Date'] = json_txn['Txn Initiation Date'].replace('-','/')
                            json_txn['Txn Input Amount'] = f"{float(json_txn['Txn Input Amount']):.8f}"
                            json_txn['Txn Output Amount'] = f"{float(json_txn['Txn Output Amount']):.8f}"
                            json_txn['Txn Fee'] = f"{float(oklink_txn['txfee']):.8f}"
                            json_txn['Mempool Final Txn Date'] = json_txn['Mempool Final Txn Date'].replace('-','/') if json_txn['Mempool Final Txn Date'] != '' else Mempool_Final_Txn_Date
                            Mempool_Final_Txn_Date = json_txn['Mempool Final Txn Date']
                            json_txn['Memory Depth'] = str(int( (parse(json_txn['Txn Initiation Date']) - parse(json_txn['Mempool Final Txn Date'] )).total_seconds() ))
                            json_txn['Txn Weight'] = oklink_txn['weight']
                            json_txn['Txn State'] = "Confirmed" if oklink_txn['state'] == 'success' else  ('Pending' if oklink_txn['state'] == 'pending' else 'null')
                            json_txn['Txn Verification Date'] = datetime.fromtimestamp((int(oklink_txn['transactionTime'])) / 1000).strftime('%Y/%m/%d %H:%M:%S')  if oklink_txn['state'] == 'success' else 'null'
                            json_txn['Txn Input Details'] = format_address_details(oklink_txn['inputDetails'], json_txn['Txn Hash'])
                            json_txn['Txn Output Details'] = format_address_details(oklink_txn['outputDetails'], json_txn['Txn Hash'])
                            json_txn['Txn Fee Rate'] = f"{(float(oklink_txn['txfee']) / float(oklink_txn['virtualSize']) * 100000000):.8f}"
                            json_txn['Txn Fee Ratio'] = f"{((float(json_txn['Txn Fee'])/float(json_txn['Txn Output Amount']))*100):.8f}" if float(json_txn['Txn Output Amount']) != 0 else "0"
                            json_txn['Miner Verification Time'] = str(int( (parse(json_txn['Txn Verification Date']) - parse(json_txn['Txn Initiation Date'] )).total_seconds() ))  if oklink_txn['state'] == 'success' else 'null'
                            json_txn['Dust Bool'] = "1" if float(json_txn['Txn Output Amount']) <= 0.00001 or (float(json_txn['Txn Fee Ratio']) >= 33 ) else "0"
                            json_txn['Total Txn Size'] = oklink_txn['totalTransactionSize']
                            json_txn['Virtual Txn Size'] = oklink_txn['virtualSize']
                            json_txn['Block Height'] = oklink_txn['height'] if oklink_txn['state'] == 'success' else 'null'

                print(f"file: {file_counter}/{file_total}, txn: {txn_counter}/{len(json_data)}(status:{response.status_code})")

            with open(json_path, 'w', encoding='utf-8') as outfile:
                json.dump(json_data, outfile, ensure_ascii=False, indent=4)

            file_counter += 1

end_time = time.time()
total_time = end_time - start_time
print(f"Total time taken: {total_time:.2f} seconds")