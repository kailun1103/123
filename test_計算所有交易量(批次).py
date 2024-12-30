import os
import json
import time
from concurrent.futures import ProcessPoolExecutor

def count_txn(json_path):
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(infile)
        json_data = json.load(infile)
        count = 0
        for item in json_data:
            # if item['Dust Bool'] == '0':
            #     if float(item['Miner Verification Time']) >= 10800:
            count += 1
        return count

def main():
    json_file_path = 'test'
    start_time = time.time()

    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    total = 0
    with ProcessPoolExecutor() as executor:
        results = executor.map(count_txn, json_files)
        total = sum(results)

    end_time = time.time()
    execution_time = end_time - start_time

    # print("float(item['Miner Verification Time']) > 3600 and item['Dust Bool'] == '0'")
    print(f"總計: {total}")
    print(f"執行時間: {execution_time:.2f} 秒")

if __name__ == "__main__":
    main()