import os
import json
import time
from multiprocessing import Pool, cpu_count

def process_single_file(args):
    json_path, unique_addresses = args
    matching_items = []
    count = 0
    
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        print(f"處理文件: {json_path}")
        for item in json_data:
            input_details = json.loads(item['Txn Input Details'])
            for detail in input_details:
                if detail.get("inputHash") in unique_addresses:
                    if item['Dust Bool'] == '1':
                        matching_items.append(item)
                        count += 1
    
    return matching_items, count, os.path.basename(json_path)

def save_batch(batch, batch_number, output_dir):
    output_file = os.path.join(output_dir, f'matching_dust_transactions_batch_{batch_number}.json')
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(batch, outfile, indent=2, ensure_ascii=False)
    print(f"保存批次 {batch_number} 到: {output_file}")

def process_json_files(json_file_path, unique_addresses_file, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    with open(unique_addresses_file, 'r', encoding='utf-8') as infile:
        unique_addresses = set(item['address'] for item in json.load(infile))
    
    json_files = [os.path.join(root, file) 
                  for root, _, files in os.walk(json_file_path) 
                  for file in files if file.endswith('.json')]
    
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_single_file, [(file, unique_addresses) for file in json_files])
    
    all_matching_items = []
    total = 0
    batch_size = 10000
    batch_number = 1

    for matching_items, count, filename in results:
        all_matching_items.extend(matching_items)
        total += count
        print(f"文件 {filename} 中找到的匹配項: {count}")
        
        while len(all_matching_items) >= batch_size:
            batch = all_matching_items[:batch_size]
            save_batch(batch, batch_number, output_dir)
            all_matching_items = all_matching_items[batch_size:]
            batch_number += 1
    
    if all_matching_items:
        save_batch(all_matching_items, batch_number, output_dir)
    
    print(f"總共找到的匹配項: {total}")
    print(f"匹配項已保存到目錄: {output_dir}")

if __name__ == '__main__':
    json_file_path = '0619-0811'
    unique_addresses_file = 'unique_dust_addresses.json'
    output_dir = 'matching_dust_transactions'

    start_time = time.time()
    process_json_files(json_file_path, unique_addresses_file, output_dir)
    execution_time = time.time() - start_time
    print(f"程式執行時間: {execution_time:.2f} 秒")