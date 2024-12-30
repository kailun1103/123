import os
import json
from multiprocessing import Pool, cpu_count
import numpy as np
from datetime import datetime
import time
from functools import lru_cache

@lru_cache(maxsize=10000)
def parse_datetime_cached(datetime_str):
    for fmt in ('%Y/%m/%d %H:%M', '%Y/%m/%d %I:%M:%S %p', 
                '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S'):
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"時間數據 '{datetime_str}' 不符合任何已知格式。")

def preprocess_dust_transactions(dust_data):
    """預處理 dust 交易數據，建立高效的查詢結構"""
    dust_utxo_map = {}
    
    for d_item in dust_data:
        d_datetime = parse_datetime_cached(d_item["Txn Initiation Date"])
        d_output_details = json.loads(d_item['Txn Output Details'])
        
        for d_output_detail in d_output_details:
            if float(d_output_detail['amount']) <= 0.00000546:
                output_hash = d_output_detail['outputHash']
                if output_hash not in dust_utxo_map:
                    dust_utxo_map[output_hash] = {
                        'datetime': d_datetime,
                        'txn_data': {
                            'Txn Hash': d_item['Txn Hash'],
                            'Txn Fee Rate': d_item['Txn Fee Rate'],
                            'Miner Verification Time': d_item['Miner Verification Time'],
                            'Virtual Txn Size': d_item['Virtual Txn Size']
                        }
                    }
    
    return dust_utxo_map

def process_chunk(chunk_data):
    """處理正常交易的分塊數據"""
    chunk, dust_utxo_map, chunk_id = chunk_data
    results = []
    no_utxo_results = []  # 新增：儲存沒有接收到 UTXO 的交易
    chunk_len = len(chunk)
    
    for i, n_item in enumerate(chunk):
        if (i + 1) % 5000 == 0:
            print(f"處理程序 {chunk_id}: 已處理 {i + 1}/{chunk_len} 筆交易")
            
        n_datetime = parse_datetime_cached(n_item["Txn Initiation Date"])
        n_input_details = json.loads(n_item['Txn Input Details'])
        
        found_utxo = False
        for n_input_detail in n_input_details:
            input_hash = n_input_detail['inputHash']
            if input_hash in dust_utxo_map:
                dust_data = dust_utxo_map[input_hash]
                if n_datetime > dust_data['datetime']:
                    time_diff = (n_datetime - dust_data['datetime']).total_seconds()
                    result_item = n_item.copy()
                    result_item['time_diff'] = time_diff
                    result_item.update({
                        f"Dust_{key}": value 
                        for key, value in dust_data['txn_data'].items()
                    })
                    results.append(result_item)
                    found_utxo = True
                    break
        
        if not found_utxo:
            no_utxo_results.append(n_item)
    
    return results, no_utxo_results

def main():
    start_time = time.time()
    
    # 檔案路徑
    file_name_dust = 'dataset/冷門時段粉塵utxo交易.json'
    file_name_nor = 'dataset/nor_txn_data.json'
    
    print("讀取檔案中...")
    with open(file_name_dust, 'r', encoding='utf-8') as infile:
        dust_data = json.load(infile)
    with open(file_name_nor, 'r', encoding='utf-8') as infile:
        nor_data = json.load(infile)
    
    print(f"總dust交易數: {len(dust_data)}")
    print(f"總正常交易數: {len(nor_data)}")
    
    print("建立dust交易索引...")
    dust_utxo_map = preprocess_dust_transactions(dust_data)
    print(f"檔案讀取和預處理完成，耗時: {time.time() - start_time:.2f} 秒")
    
    # 優化CPU使用率
    num_processes = max(1, int(cpu_count() * 0.8))
    chunks = np.array_split(nor_data, num_processes)
    
    # 優化參數傳遞
    chunk_args = [(chunk.tolist(), dust_utxo_map, i) for i, chunk in enumerate(chunks)]
    
    print(f"開始使用 {num_processes} 個處理程序進行平行處理...")
    process_start_time = time.time()
    
    with Pool(num_processes) as pool:
        all_results = pool.map(process_chunk, chunk_args)
    
    # 分別合併有 UTXO 和沒有 UTXO 的結果
    filter_utxo_txn = []
    no_utxo_txn = []
    for results, no_utxo_results in all_results:
        filter_utxo_txn.extend(results)
        no_utxo_txn.extend(no_utxo_results)
    
    print(f"找到符合條件的交易數: {len(filter_utxo_txn)}")
    print(f"找到沒有接收到 UTXO 的交易數: {len(no_utxo_txn)}")
    print(f"平行處理完成，耗時: {time.time() - process_start_time:.2f} 秒")
    
    # 寫入結果
    print("寫入結果到文件...")
    with open('test.json', 'w', encoding='utf-8') as outfile:
        json.dump(filter_utxo_txn, outfile, indent=2, ensure_ascii=False)
        
    # 寫入沒有接收到 UTXO 的交易
    with open('no_utxo_transactions.json', 'w', encoding='utf-8') as outfile:
        json.dump(no_utxo_txn, outfile, indent=2, ensure_ascii=False)
    
    print(f"\n總耗時: {time.time() - start_time:.2f} 秒")

if __name__ == '__main__':
    main()