import requests
import itertools
import json
import time
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

def get_address_page(address):
    current_key = next(key_cycle)
    headers = {
        'Ok-Access-Key': current_key
    }
    payload = {
        "chainShortName": "btc",
        "address": address,
        "limit": 1,
        "page": 1
    }
    response = requests.get("https://www.oklink.com/api/v5/explorer/address/transaction-list", headers=headers, params=payload)
    print(response.status_code)
    response_json = response.json()
    print(response_json)
    total_page = response_json['data'][0]['totalPage']
    return total_page


def get_oklink_txn(address):
    # step1. 獲取address_list的total_page數
    total_page = get_address_page(address)
    print(total_page)
    current_key = next(key_cycle)

    # step2. 獲得區塊的所有hash
    all_hashs_lists = []
    for i in range(1,int(total_page)+1):
        headers = {
            'Ok-Access-Key': current_key
        }
        payload = {
            "chainShortName": "btc",
            "address": address,
            "limit": 100,
            "page": i
        }
        response = requests.get("https://www.oklink.com/api/v5/explorer/address/transaction-list", headers=headers, params=payload)
        while True:
            current_key = next(key_cycle)
            response = requests.get("https://www.oklink.com/api/v5/explorer/address/transaction-list", headers=headers, params=payload)
            if response.status_code == 200:
                break
        print(response.status_code)
        response_json = response.json()  # Parse the response text into a JSON object
        hashs_lists = [transaction['txId'] for transaction in response_json['data'][0]['transactionLists']]
        all_hashs_lists.extend(hashs_lists)

    # step3. 根據區塊裡的每一個hash獲得交易詳細資訊
    all_hashs_detail = []
    for i in range(0, len(all_hashs_lists), 20):
        # 直接使用切片取得的 txId，不需要再次取值
        txids_batch = ','.join(all_hashs_lists[i:i+20])
        headers = {
            'Ok-Access-Key': current_key
        }
        payload = {
            "chainShortName": "btc",
            "txid": txids_batch
        }
        response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
        while True:
            current_key = next(key_cycle)
            response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
            if response.status_code == 200:
                break
        response_json = response.json()
        txn_json = response_json['data']
        # 在處理交易詳細信息時，我們需要修改 input 和 output 的格式 1730185834000
        for transaction in txn_json:
            # 預先計算常用的 float 值
            txn_input_amount = float(transaction['amount'])
            txn_fee = float(transaction['txfee'])
            virtual_size = float(transaction['virtualSize'])
            
            # 計算 fee ratio
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
            all_hashs_detail.append(oklink_txn_item)
    print(len(all_hashs_detail))

    # 在寫入檔案前進行排序
    sorted_hashs_detail = sorted(all_hashs_detail, 
                            key=lambda x: int(x['Txn Verification Date']), 
                            reverse=False)  # reverse=True 表示從新到舊排序

    # 將排序後的結果寫入檔案
    with open(f'oklink_block_txn/{address}_txn.json', 'w') as outfile:
        json.dump(sorted_hashs_detail, outfile, indent=2)

    return sorted_hashs_detail

def find_matching_transaction(sorted_hashs_detail, target_timestamp):
    # 讀取已找到的交易
    with open('oklink_block_txn/found_transactions.json', 'r') as f:
        found_transactions = json.load(f)
    
    # 取出已找到的交易 hash 列表
    found_txn_hashes = [tx["txn"] for tx in found_transactions]
    
    target_timestamp = int(target_timestamp)
    
    # 找出符合條件且未被找到過的交易，同時檢查金額
    valid_transactions = []
    for txn in sorted_hashs_detail:
        if (int(txn['Txn Verification Date']) >= target_timestamp
            and txn['Dust Bool'] == "0" 
            and txn['Txn Hash'] not in found_txn_hashes):
            
            # 檢查交易金額
            input_details = json.loads(txn['Txn Input Details'])
            if any(float(detail['amount']) <= 0.00000546 for detail in input_details):
                valid_transactions.append(txn)
    
    print('符合交易筆數:', len(valid_transactions))
    
    if valid_transactions:
        # 一定會取最接近的那一筆
        closest_transaction = max(valid_transactions, 
                                key=lambda x: int(x['Txn Verification Date']))
        
        # 不管輸入地址是否匹配，都加入到 found_transactions
        found_transactions.append({"txn": closest_transaction['Txn Hash']})
        with open('oklink_block_txn/found_transactions.json', 'w') as f:
            json.dump(found_transactions, f, indent=4)
            
        return closest_transaction['Txn Hash']
    
    return None



if __name__ == "__main__":
    # 使用函數

    target_address = "1LnoZawVFFQihU8d8ntxLMpYheZUfyeVAK"


    start_time = time.time()
    sorted_hashs_detail = get_oklink_txn(target_address)



    target_timestamp = "1727067016000"
    
    result = find_matching_transaction(sorted_hashs_detail, target_timestamp, target_address)

    if result:
        print(f"Found matching transaction: {result}")
    else:
        print("No matching transaction found")

    end_time = time.time()
    
    elapsed_time = end_time - start_time
    print(f"運行時間: {elapsed_time:.2f} 秒")