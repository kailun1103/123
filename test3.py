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
    noon_self_txns = []
    noon_other_txns = []
    
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        print(infile)
        for item in json_data:
            try:
                # Get hour
                date = parse_datetime(item["Txn Initiation Date"])
                hour = date.hour
                
                # Check if it's a dust transaction
                if item['Dust Bool'] == '1':
                    hourly_stats[hour]['total'] += 1
                    is_self_txn = check_self_transaction(item["Txn Input Details"], item["Txn Output Details"])
                    
                    if is_self_txn:
                        hourly_stats[hour]['self_txns'] += 1
                    
                    # If it's 12:00, save to appropriate list
                    if hour == 12:
                        if is_self_txn:
                            noon_self_txns.append(item)
                        else:
                            noon_other_txns.append(item)
                            
            except Exception as e:
                continue
                
    return hourly_stats, noon_self_txns, noon_other_txns

def main():
    json_file_path = '0619-0811/0619-0723'
    start_time = time.time()
    
    # Get all JSON files
    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))
    
    total_stats = {hour: {'total': 0, 'self_txns': 0} for hour in range(24)}
    all_noon_self_txns = []
    all_noon_other_txns = []
    
    with ProcessPoolExecutor(max_workers=10) as executor:
        results = executor.map(process_file, json_files)
        for file_stats, noon_self, noon_other in results:
            # Aggregate statistics
            for hour in range(24):
                total_stats[hour]['total'] += file_stats[hour]['total']
                total_stats[hour]['self_txns'] += file_stats[hour]['self_txns']
            
            # Aggregate noon transactions
            all_noon_self_txns.extend(noon_self)
            all_noon_other_txns.extend(noon_other)
    
    # Save noon transactions to separate files
    with open('noon_self_transactions.json', 'w', encoding='utf-8') as outfile:
        json.dump(all_noon_self_txns, outfile, ensure_ascii=False, indent=2)
    
    with open('noon_other_transactions.json', 'w', encoding='utf-8') as outfile:
        json.dump(all_noon_other_txns, outfile, ensure_ascii=False, indent=2)
    
    # Print statistics
    print("Hour | Total Transactions | Self Transactions | Percentage")
    print("-" * 60)
    for hour in range(24):
        total = total_stats[hour]['total']
        self_txns = total_stats[hour]['self_txns']
        percentage = (self_txns / total * 100) if total > 0 else 0
        print(f"{hour:02d}:00 | {total:>16} | {self_txns:>16} | {percentage:>8.2f}%")
    
    print(f"\nNoon (12:00) transactions summary:")
    print(f"Self transactions: {len(all_noon_self_txns)}")
    print(f"Other transactions: {len(all_noon_other_txns)}")
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nExecution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()