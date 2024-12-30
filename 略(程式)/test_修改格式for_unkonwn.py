import os
import json
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial

def get_millisecond_timestamp():
    # 獲取當前時間戳（秒為單位，帶小數）
    current_time = time.time()
    
    # 將秒轉換為毫秒（乘以1000並四捨五入到整數）
    milliseconds = round(current_time * 1000)
    
    return milliseconds

def process_file(json_file, root):
    json_path = os.path.join(root, json_file)
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(f"Processing {json_file}")
        json_data = json.load(infile)
        count = 0
        for item in json_data:
            reversed_string = item['Txn Hash'][::-1]
            extracted = reversed_string[::3][:5]
            # 獲取並打印毫秒級時間戳
            ms_timestamp = get_millisecond_timestamp()
            
            # 處理 Txn Input Details
            input_details = json.loads(item['Txn Input Details'])
            for detail in input_details:
                if 'Unknown' in detail['inputHash']:
                    detail['inputHash'] = 'Unknown_InputAddress_' + item['Txn Hash']
            item['Txn Input Details'] = json.dumps(input_details)
            
            # 處理 Txn Output Details
            output_details = json.loads(item['Txn Output Details'])
            for detail in output_details:
                if 'Unknown' in detail['outputHash']:
                    detail['outputHash'] = 'Unknown_OutputAddress_' + item['Txn Hash']
            item['Txn Output Details'] = json.dumps(output_details)

            count += 1
    
    # 寫回文件
    with open(json_path, 'w', encoding='utf-8') as outfile:
        json.dump(json_data, outfile, ensure_ascii=False, indent=2)
    
    return count

def main():
    json_file_path = '0619-0811'
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