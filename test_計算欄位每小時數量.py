import json
import sys
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import time
import numpy as np  # 用於計算四分位數

# Define multiple date-time formats
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

def process_file(args):
    json_path, txn_fields = args
    hourly_values = {field: {hour: [] for hour in range(24)} for field in txn_fields}
    hourly_counts = {hour: 0 for hour in range(24)}
    
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        print(infile)
        for item in json_data:
            date = parse_datetime(item["Txn Initiation Date"])
            hour = date.hour
            hourly_counts[hour] += 1
            for field in txn_fields:
                value = float(item[field])
                hourly_values[field][hour].append(value)
    return hourly_values, hourly_counts

def calculate_averages_and_quartiles(hourly_values):
    stats = {}
    for field, hourly_value in hourly_values.items():
        stats[field] = {}
        for hour, values in hourly_value.items():
            if values:
                values_array = np.array(values)
                stats[field][hour] = {
                    'mean': np.mean(values_array)
                    # 'q1': np.percentile(values_array, 25),
                    # 'q2': np.percentile(values_array, 50),  # 中位數
                    # 'q3': np.percentile(values_array, 75),
                    # 'q4': np.max(values_array)  # Q4 通常被視為最大值
                }
    return stats

def main():
    if len(sys.argv) < 1:
        print("Usage: python script.py <txn_field1> <txn_field2>")
        sys.exit(1)

    txn_fields = sys.argv[1:]
    json_file_path = 'test'
    start_time = time.time()

    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    hourly_values = {field: {hour: [] for hour in range(24)} for field in txn_fields}
    total_hourly_counts = {hour: 0 for hour in range(24)}
    
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_file, [(file, txn_fields) for file in json_files])
        for file_values, file_counts in results:
            for hour, count in file_counts.items():
                total_hourly_counts[hour] += count
            for field in txn_fields:
                for hour, value in file_values[field].items():
                    hourly_values[field][hour].extend(value)

    # Calculate statistics
    hourly_stats = calculate_averages_and_quartiles(hourly_values)

    # Print results for each field
    for field in txn_fields:
        print(f'=== {field} Statistics ===')
        for hour in range(24):
            # print(f'Hour {hour}:')
            if hour in hourly_stats[field]:
                stats = hourly_stats[field][hour]
                print(f"{stats['mean']:.8f}")

                # print(f"  Avg: {stats['mean']:.8f}")
                # print(f"  Q1 (25%): {stats['q1']:.8f}")
                # print(f"  Q2 (50%): {stats['q2']:.8f}")
                # print(f"  Q3 (75%): {stats['q3']:.8f}")
                # print(f"  Q4 (100%): {stats['q4']:.8f}")
    
    # Print transaction counts
    print('')
    print('=== Hourly Transaction Counts ===')
    print('')
    for hour in range(24):
        print(f"{total_hourly_counts[hour]}")

    end_time = time.time()
    execution_time = end_time - start_time
    print('')
    print(f"\nExecution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()

# python test_計算欄位每小時數量.py 'Txn Input Amount' 'Txn Output Amount' 'Txn Input Address' 'Txn Output Address' 'Txn Fee' 'Txn Weight' 'Txn Fee Rate' 'Txn Fee Ratio' 'Mempool Txn Count' 'Memory Depth' 'Miner Verification Time' 'Total Txn Size' 'Virtual Txn Size' 'Block Txn Count' 'Block Txn Amount' 'Block Size' 'Block Miner Reward' 'Block Txn Fees' 'Block Difficulty' 'Block Confirm'