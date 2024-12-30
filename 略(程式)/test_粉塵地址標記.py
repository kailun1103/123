import os
import json
import multiprocessing as mp
import time

def process_single_file(args):
    json_path, unique_addresses = args
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(json_path)
        json_data = json.load(infile)
        for item in json_data:
            input_details = json.loads(item['Txn Input Details'])
            for detail in input_details:
                detail['dust_bool'] = 1 if detail.get("inputHash") in unique_addresses else 0
            item['Txn Input Details'] = json.dumps(input_details)

            output_details = json.loads(item['Txn Output Details'])
            for detail in output_details:
                detail['dust_bool'] = 1 if detail.get("outputHash") in unique_addresses else 0
            item['Txn Output Details'] = json.dumps(output_details)
    
    with open(json_path, 'w', encoding='utf-8') as outfile:
        json.dump(json_data, outfile, indent=2)
    
    return f"Processed: {os.path.basename(json_path)}"

def process_json_files(json_file_path, unique_addresses_file):
    start_time = time.time()  # 開始計時

    with open(unique_addresses_file, 'r', encoding='utf-8') as infile:
        unique_addresses_data = json.load(infile)
        unique_addresses = set(unique_addresses_data['unique_dust_addresses'])

    json_files = []
    for root, dirs, files in os.walk(json_file_path):
        json_files.extend([os.path.join(root, file) for file in files if file.endswith('.json')])

    # 創建進程池
    pool = mp.Pool(processes=mp.cpu_count())

    # 準備參數
    args = [(file, unique_addresses) for file in json_files]

    # 並行處理文件
    results = pool.map(process_single_file, args)

    # 關閉進程池
    pool.close()
    pool.join()

    # 打印結果
    for result in results:
        print(result)

    end_time = time.time()  # 結束計時
    elapsed_time = end_time - start_time
    print(f"\nTotal processing time: {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    json_file_path = 'matching_dust_transactions'
    unique_addresses_file = 'unique_dust_addresses.json'

    process_json_files(json_file_path, unique_addresses_file)