import os
import json
from decimal import Decimal

def decimal_to_str(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def process_details(details):
    merged = {}
    for item in details:
        hash_key = item['inputHash'] if 'inputHash' in item else item['outputHash']
        amount = Decimal(item['amount'])
        
        if hash_key in merged:
            merged[hash_key]['amount'] += amount
        else:
            merged[hash_key] = item.copy()
            merged[hash_key]['amount'] = amount
    
    return list(merged.values())

def process_transaction(data):
    # 處理 Txn Input Details
    input_details = json.loads(data['Txn Input Details'])
    merged_input_list = process_details(input_details)
    data['Txn Input Details'] = json.dumps(merged_input_list, default=decimal_to_str)
    
    # 處理 Txn Output Details
    output_details = json.loads(data['Txn Output Details'])
    merged_output_list = process_details(output_details)
    data['Txn Output Details'] = json.dumps(merged_output_list, default=decimal_to_str)
    
    return data

json_file_path = 'matching_dust_transactions'
total = 0

for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_path)
            json_data = json.load(infile)
            processed_data = []
            for item in json_data:
                processed_item = process_transaction(item)
                processed_data.append(processed_item)
                total += 1
        
        # 將處理後的數據寫回原文件
        with open(json_path, 'w', encoding='utf-8') as outfile:
            json.dump(processed_data, outfile, ensure_ascii=False, indent=2, default=decimal_to_str)

print(f"Total processed transactions: {total}")
print("All files have been processed and updated.")