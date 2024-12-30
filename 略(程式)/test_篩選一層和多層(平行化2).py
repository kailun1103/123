import os
import json
from multiprocessing import Pool, cpu_count
from functools import partial
from collections import defaultdict

def load_all_data(json_path):
    """載入所有資料到記憶體中"""
    with open(json_path, 'r', encoding='utf-8') as infile:
        return json.load(infile)

def build_hash_indices(all_data):
    """建立雜湊索引以加速查詢"""
    input_index = defaultdict(list)
    output_index = defaultdict(list)
    
    for idx, item in enumerate(all_data):
        # 建立輸入地址索引
        input_addresses = json.loads(item['Txn Input Details'])
        for input_ in input_addresses:
            input_index[input_['inputHash']].append(idx)
            
        # 建立輸出地址索引
        output_addresses = json.loads(item['Txn Output Details'])
        for output_ in output_addresses:
            output_index[output_['outputHash']].append(idx)
            
    return input_index, output_index

def process_single_item(args):
    """處理單一交易項目"""
    item, input_index, output_index = args
    print(item['Txn Hash'])
    
    # 檢查輸入地址
    input_addresses = json.loads(item['Txn Input Details'])
    for input_ in input_addresses:
        # 使用索引查詢相關交易
        if input_['inputHash'] in output_index:
            return (True, item)
    
    # 檢查輸出地址
    output_addresses = json.loads(item['Txn Output Details'])
    for output_ in output_addresses:
        # 使用索引查詢相關交易
        if output_['outputHash'] in input_index:
            return (True, item)
    
    return (False, item)

def parallel_process_json(json_path, num_processes=None):
    """平行處理主函數"""
    if num_processes is None:
        num_processes = max(1, cpu_count() - 1)
    
    print(f"Using {num_processes} processes")
    
    # 載入所有資料
    all_data = load_all_data(json_path)
    total_items = len(all_data)
    
    # 建立索引
    print("Building indices...")
    input_index, output_index = build_hash_indices(all_data)
    
    # 準備處理參數
    process_args = [(item, input_index, output_index) for item in all_data]
    
    # 使用進程池進行平行處理
    print("Processing transactions...")
    with Pool(processes=num_processes) as pool:
        results = pool.map(process_single_item, process_args)
    
    # 分離結果
    multi_items = []
    one_items = []
    count = 0
    
    for is_multi, item in results:
        if is_multi:
            multi_items.append(item)
            count += 1
        else:
            one_items.append(item)
    
    # 寫入結果檔案
    with open('multi_layer(all txn).json', 'w', encoding='utf-8') as multi_file:
        json.dump(multi_items, multi_file, ensure_ascii=False, indent=2)
    
    with open('one_layer(all txn).json', 'w', encoding='utf-8') as one_file:
        json.dump(one_items, one_file, ensure_ascii=False, indent=2)
    
    return count, len(multi_items), len(one_items)

def main():
    json_path = 'txn_merged_result_sampled_sampled.json'
    count, multi_count, one_count = parallel_process_json(json_path)
    
    print(f"Total multi-layer transactions: {count}")
    print(f"Multi-layer items written: {multi_count}")
    print(f"One-layer items written: {one_count}")

if __name__ == "__main__":
    main()