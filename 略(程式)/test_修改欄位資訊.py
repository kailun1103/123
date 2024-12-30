import os
import json
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial

def process_file(json_file, root):
    json_path = os.path.join(root, json_file)
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(f"Processing {json_file}")
        json_data = json.load(infile)
        count = 0
        for item in json_data:
            item['Mempool Txn Count'] = '128169'
    
    # Write back to file
    with open(json_path, 'w', encoding='utf-8') as outfile:
        json.dump(json_data, outfile, ensure_ascii=False, indent=2)
    
    return count

def main():
    json_file_path = 'test'
    start_time = time.time()

    total = 0
    with ProcessPoolExecutor() as executor:
        for root, _, files in os.walk(json_file_path):
            json_files = [file for file in files if file.endswith('.json')]
            process_func = partial(process_file, root=root)
            results = executor.map(process_func, json_files)
            total += sum(results)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"總計: {total}")
    print(f"執行時間: {execution_time:.2f} 秒")

if __name__ == "__main__":
    main()