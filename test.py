import os
import json
import time
from test_get_address_pipeline import get_oklink_txn, find_matching_transaction

total = 0
start_time = time.time()

with open('utxo_address_823741.json', 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)
    count = 0
    for item in json_data:
        if item.get('recieve_utxo_bool') is None:
            item['recieve_utxo_bool'] = False
            item['recieve_utxo_txn'] = []


            
            # 讀取和處理輸出詳情
            output_details = json.loads(item['Txn Output Details'])
            for detail in output_details:
                print(f"\nProcessing output: {detail['outputHash']}")
                print(f"Amount: {detail['amount']}")
                

                sorted_hashs_detail = get_oklink_txn(detail['outputHash'])
                result = find_matching_transaction(
                    sorted_hashs_detail, 
                    int(item['Txn Verification Timestamp']), 
                    float(detail['amount'])
                )
                
                if result:
                    total += 1
                    print(f"Found matching transaction: {result['Txn Hash']}")
                    item['recieve_utxo_bool'] = True
                    item['recieve_utxo_txn'].append(result)
                else:
                    print("No matching transaction found")
                    

    # 將更新後的數據寫回文件
    with open('utxo_address_823740.json', 'w', encoding='utf-8') as outfile:
        json.dump(json_data, outfile, indent=2)

print(total)
end_time = time.time()
elapsed_time = end_time - start_time
print(f"運行時間: {elapsed_time:.2f} 秒")