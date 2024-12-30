import os
import json
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
import statistics

def process_file(json_path):
    try:
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(infile)
            json_data = json.load(infile)
            return [float(txn["Total Txn Size"]) for txn in json_data if txn["Dust Bool"] == "0"]
    except Exception as e:
        print(f"Error processing file {json_path}: {str(e)}")
        return []

def percentile(data, percentile):
    size = len(data)
    if size == 0:
        return None
    index = int(size * percentile)
    if index == size:  # 處理 100% 的情況
        index = size - 1
    return sorted(data)[index]

def count_in_range(data, start, end):
    return sum(start < x <= end for x in data)

def analyze_transactions(json_file_path):
    output_amounts = []
    
    # 使用ProcessPoolExecutor來並行處理文件
    with ProcessPoolExecutor() as executor:
        json_files = [os.path.join(root, file) 
                      for root, _, files in os.walk(json_file_path) 
                      for file in files if file.endswith('.json')]
        
        results = executor.map(process_file, json_files)
        for result in results:
            output_amounts.extend(result)

    if not output_amounts:
        print("No valid transaction output amounts found.")
        return

    # 計算四分位數
    output_amounts.sort()
    q1 = percentile(output_amounts, 0.60)
    q2 = percentile(output_amounts, 0.70)
    q3 = percentile(output_amounts, 0.80)
    q4 = percentile(output_amounts, 0.95)

    if q1 is None or q2 is None or q3 is None or q4 is None:
        print("Error: Unable to calculate percentiles. Check if the data is valid.")
        return

    print(f"60%: {q1:.8f}")
    print(f"70%: {q2:.8f}")
    print(f"80%: {q3:.8f}")
    print(f"95%: {q4:.8f}")

    # 計算各區間的數量
    count_q1 = count_in_range(output_amounts, float('-inf'), q1)
    count_q2 = count_in_range(output_amounts, q1, q2)
    count_q3 = count_in_range(output_amounts, q2, q3)
    count_q4 = count_in_range(output_amounts, q3, float('inf'))

    print(f"Number of transactions in 0-25% (Q1) range: {count_q1}")
    print(f"Number of transactions in 25-50% (Q2) range: {count_q2}")
    print(f"Number of transactions in 50-75% (Q3) range: {count_q3}")
    print(f"Number of transactions in 75-100% (Q4) range: {count_q4}")

    # 額外的統計信息
    total_transactions = len(output_amounts)
    if total_transactions > 0:
        mean_fee = statistics.mean(output_amounts)
        std_dev_fee = statistics.stdev(output_amounts) if total_transactions > 1 else 0
        print(f"Total number of transactions: {total_transactions}")
        print(f"Mean transaction fee: {mean_fee:.8f}")
        print(f"Standard deviation of transaction fees: {std_dev_fee:.8f}")
    else:
        print("No transactions found to calculate statistics.")

if __name__ == "__main__":
    json_file_path = 'new dataset(sort)'
    analyze_transactions(json_file_path)