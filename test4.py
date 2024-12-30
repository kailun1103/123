import json
import sys
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import time

datetime_formats = [
    '%Y/%m/%d %H:%M',
    '%Y/%m/%d %I:%M:%S %p',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S',
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")

def check_self_transaction(input_details, output_details):
    try:
        input_addresses = set()
        output_addresses = set()
        
        # Parse input addresses
        input_data = json.loads(input_details)
        for input_tx in input_data:
            input_addresses.add(input_tx['inputHash'])
            
        # Parse output addresses
        output_data = json.loads(output_details)
        for output_tx in output_data:
            output_addresses.add(output_tx['outputHash'])
            
        # Check for intersection between input and output addresses
        return len(input_addresses.intersection(output_addresses)) > 0
    except:
        return False

def process_file(json_path):
    hourly_stats = {hour: {'total': 0, 'self_txns': 0} for hour in range(24)}
    
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        print(infile)
        for item in json_data:
            try:
                # 首先獲取 hour
                date = parse_datetime(item["Txn Initiation Date"])
                hour = date.hour
                
                # 檢查是否為 dust transaction
                if item['Dust Bool'] == '1':
                    hourly_stats[hour]['total'] += 1
                    
                    if check_self_transaction(item["Txn Input Details"], item["Txn Output Details"]):
                        hourly_stats[hour]['self_txns'] += 1
            except Exception as e:
                continue
                
    return hourly_stats

def main():
    json_file_path = '0619-0811/0619-0723'
    start_time = time.time()

    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    total_stats = {hour: {'total': 0, 'self_txns': 0} for hour in range(24)}
    
    with ProcessPoolExecutor(max_workers=10) as executor:  # 使用4個處理程序
        results = executor.map(process_file, json_files)
        for file_stats in results:
            for hour in range(24):
                total_stats[hour]['total'] += file_stats[hour]['total']
                total_stats[hour]['self_txns'] += file_stats[hour]['self_txns']

    # Print results
    print("Hour | Total Transactions | Self Transactions | Percentage")
    print("-" * 60)
    for hour in range(24):
        total = total_stats[hour]['total']
        self_txns = total_stats[hour]['self_txns']
        percentage = (self_txns / total * 100) if total > 0 else 0
        print(f"{hour:02d}:00 | {total:>16} | {self_txns:>16} | {percentage:>8.2f}%")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nExecution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()