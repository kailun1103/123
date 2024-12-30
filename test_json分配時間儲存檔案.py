import os
import json
from datetime import datetime
import multiprocessing as mp
from functools import partial
from tqdm import tqdm
import time

# 定義多個日期時間格式
datetime_formats = [
    '%Y/%m/%d %H:%M',
    '%Y/%m/%d %I:%M:%S %p',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S'
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"時間數據 '{datetime_str}' 不符合任何已知格式。")

def process_json_file(json_file, root):
    """處理單個 JSON 文件並返回按小時分類的交易"""
    hourly_transactions = {hour: [] for hour in range(24)}
    json_path = os.path.join(root, json_file)
    

    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        print(infile)
        
        for txn in json_data:
            date = parse_datetime(txn['Txn Initiation Date'])
            hourly_transactions[date.hour].append(txn)
 
    return hourly_transactions

def merge_hourly_transactions(results):
    """合併多個處理結果"""
    merged = {hour: [] for hour in range(24)}
    for result in results:
        for hour in range(24):
            merged[hour].extend(result[hour])
    return merged

def save_hourly_transactions(output_dir, hourly_transactions):
    """保存按小時分類的交易到文件"""
    for hour, transactions in hourly_transactions.items():
        if transactions:
            output_json_file_path = os.path.join(output_dir, f"transactions_hour_{hour:02d}.json")
            with open(output_json_file_path, 'w', encoding='utf-8') as outfile:
                json.dump(transactions, outfile, ensure_ascii=False, indent=4)
            print(f"已保存 {len(transactions)} 筆交易到文件 {output_json_file_path}")


def process_json_files_parallel():
    json_file_path = '0619-0723/2'
    output_dir = 'test/2'
    
    # 如果輸出目錄不存在，則創建它
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # 獲取所有 JSON 文件
    json_files = []
    for root, dirs, files in os.walk(json_file_path):
        json_files.extend((file, root) for file in files if file.endswith('.json'))
    
    # 設置進程數（使用 CPU 核心數量）
    num_processes = mp.cpu_count()
    
    # 建立進程池
    with mp.Pool(num_processes) as pool:
        # 使用 tqdm 顯示進度條
        results = list(tqdm(
            pool.starmap(process_json_file, json_files),
            total=len(json_files),
            desc="處理文件進度"
        ))
    
    # 合併所有處理結果
    merged_transactions = merge_hourly_transactions(results)
    
    # 保存結果
    save_hourly_transactions(output_dir, merged_transactions)
    
    print(f"處理完成。所有交易已按小時保存到 '{output_dir}' 目錄中。")

if __name__ == "__main__":
    process_json_files_parallel()