import os
import json
from datetime import datetime


def process_transactions(json_path):
    total = 0
    transactions = []  # 存儲所有交易
    seen_hashes = set()  # 用於追蹤已見過的交易哈希
    threshold = 0.00000546  # 設定閾值


    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        
        for item in json_data:
            # 解析輸出詳情
            output_details = json.loads(item['Txn Output Details'])
            # 過濾小於或等於閾值的輸出
            filtered_outputs = [output for output in output_details 
                                if float(output['amount']) <= threshold and float(output['amount']) != 0]
            
            # 如果有符合條件的輸出且是唯一交易
            if filtered_outputs and item['Txn Hash'] not in seen_hashes:
                total += 1
                timestamp_ms = int(item['Txn Verification Date'])
                timestamp_s = timestamp_ms // 1000  # 轉換為秒級時間戳
                date_str = datetime.fromtimestamp(timestamp_s).strftime('%Y-%m-%d %H:%M:%S')          
                transaction_info = {
                    'Txn Hash': item['Txn Hash'],
                    'Txn Verification Timestamp': item['Txn Verification Date'],  # 保留原始時間戳
                    'Txn Verification Date': date_str,  # 格式化的日期字串
                    'Txn Output Details': json.dumps(filtered_outputs),
                    'Txn UTXO count': str(len(filtered_outputs))
                }
                transactions.append(transaction_info)
                seen_hashes.add(item['Txn Hash'])

    # 依照 Txn Verification Date 排序
    sorted_transactions = sorted(transactions, 
                               key=lambda x: int(x['Txn Verification Timestamp']), 
                               reverse=False)

    # 寫入結果
    with open('utxo_address.json', 'w', encoding='utf-8') as outfile:
        json.dump(sorted_transactions, outfile, indent=2)

    return total, len(sorted_transactions)

if __name__ == "__main__":
    json_file_path = 'oklink_block_txn/823741_txn.json'
    total, unique_count = process_transactions(json_file_path)
    
    print(f"Total count (before filtering): {total}")
    print(f"Number of unique transactions: {unique_count}")