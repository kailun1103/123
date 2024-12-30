import os
import json
from multiprocessing import Pool, cpu_count
import numpy as np
from datetime import datetime
import time
from collections import defaultdict

# 使用 LRU 緩存裝飾器來更高效地緩存日期時間解析結果
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

def preprocess_transactions(json_data):
    """預處理交易數據，提取所需信息並構建高效的查詢結構"""
    dust_transactions = {}
    
    # 預先解析所有輸出細節，避免重複解析
    for item in json_data:
        if item['Dust Bool'] == '1':
            item_time = parse_datetime_cached(item["Txn Initiation Date"])
            output_details = json.loads(item['Txn Output Details'])
            
            # 使用集合而不是列表來存儲時間，提高查找效率
            for outdetail in output_details:
                output_hash = outdetail['outputHash']
                if output_hash not in dust_transactions:
                    dust_transactions[output_hash] = {item_time}
                else:
                    dust_transactions[output_hash].add(item_time)
    
    return dust_transactions

def process_chunk(chunk_data):
    chunk, dust_transactions, chunk_id = chunk_data
    results = []
    
    # 只處理非 dust 交易
    normal_transactions = [item for item in chunk if item['Dust Bool'] == '0' and 100 >= float(item['Txn Output Address']) >= 3]
    chunk_len = len(normal_transactions)
    
    for i in range(chunk_len):
        if (i + 1) % 5000 == 0:
            print(f"處理程序 {chunk_id}: 已處理 {i + 1}/{chunk_len} 筆正常交易")
        
        item = normal_transactions[i]
        has_dust = False
        
        transaction_time = parse_datetime_cached(item["Txn Initiation Date"])
        input_details = json.loads(item['Txn Input Details'])
        
        # 檢查輸入是否使用了 dust UTXO
        for input_detail in input_details:
            input_hash = input_detail['inputHash']
            if input_hash in dust_transactions:
                dust_times = dust_transactions[input_hash]
                if any(transaction_time > dust_time for dust_time in dust_times):
                    has_dust = True
                    break
        
        results.append({
            'Txn Initiation Date': item['Txn Initiation Date'],
            'Virtual Txn Size': item['Virtual Txn Size'],
            'Txn Fee': item['Txn Fee'],
            'Txn Fee Rate': item['Txn Fee Rate'],
            'Txn Fee Ratio': item['Txn Fee Ratio'],
            'Txn Output Amount': item['Txn Output Amount'],
            'Miner Verification Time': item['Miner Verification Time'],
            'dust_utxo': has_dust
        })
    
    return results

def calculate_statistics(processed_data):
    """計算統計數據"""
    dust_transactions = []
    non_dust_transactions = []
    
    for item in processed_data:
        if item['dust_utxo']:
            dust_transactions.append(item)
        else:
            non_dust_transactions.append(item)
    
    stats = {'total': len(processed_data),
             'dust_count': len(dust_transactions),
             'non_dust_count': len(non_dust_transactions)}
    
    if stats['dust_count'] > 0:
        stats['dust'] = {
            'txn_output_amount': sum(float(item['Txn Output Amount']) for item in dust_transactions) / stats['dust_count'],
            'txn_fee': sum(float(item['Txn Fee']) for item in dust_transactions) / stats['dust_count'],
            'txn_fee_rate': sum(float(item['Txn Fee Rate']) for item in dust_transactions) / stats['dust_count'],
            'txn_fee_ratio': sum(float(item['Txn Fee Ratio']) for item in dust_transactions) / stats['dust_count'],
            'virtual_size': sum(float(item['Virtual Txn Size']) for item in dust_transactions) / stats['dust_count'],
            'miner_verification_time': sum(float(item['Miner Verification Time']) for item in dust_transactions) / stats['dust_count']

        }
    
    if stats['non_dust_count'] > 0:
        stats['non_dust'] = {
            'txn_output_amount': sum(float(item['Txn Output Amount']) for item in non_dust_transactions) / stats['non_dust_count'],
            'txn_fee': sum(float(item['Txn Fee']) for item in non_dust_transactions) / stats['non_dust_count'],
            'txn_fee_rate': sum(float(item['Txn Fee Rate']) for item in non_dust_transactions) / stats['non_dust_count'],
            'txn_fee_ratio': sum(float(item['Txn Fee Ratio']) for item in non_dust_transactions) / stats['non_dust_count'],
            'virtual_size': sum(float(item['Virtual Txn Size']) for item in non_dust_transactions) / stats['non_dust_count'],
            'miner_verification_time': sum(float(item['Miner Verification Time']) for item in non_dust_transactions) / stats['non_dust_count']
        }
    
    return stats

def main():
    file_name = '0619-0723/transactions_hour_23.json'
    start_time = time.time()
    
    print("讀取檔案中...")
    with open(file_name, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
    
    print("建立dust交易索引...")
    dust_transactions = preprocess_transactions(json_data)
    print(f"檔案讀取和預處理完成，耗時: {time.time() - start_time:.2f} 秒")
    
    # 優化CPU使用率
    num_processes = max(1, int(cpu_count() * 0.8))
    chunks = np.array_split(json_data, num_processes)
    
    # 優化參數傳遞
    chunk_args = [(chunk.tolist(), dust_transactions, i) for i, chunk in enumerate(chunks)]
    
    print(f"開始使用 {num_processes} 個處理程序進行平行處理...")
    process_start_time = time.time()
    
    with Pool(num_processes) as pool:
        results = pool.map(process_chunk, chunk_args)
    
    # 合併結果並計算統計數據
    processed_data = [item for result in results for item in result]
    stats = calculate_statistics(processed_data)
    
    print(f"{file_name}處理完成!\n平行處理完成，耗時: {time.time() - process_start_time:.2f} 秒")
    print(f"總處理交易數: {stats['total']}")
    
    if 'dust' in stats:
        print("\n有 dust UTXO 交易的平均值:")
        print(f"有 dust UTXO 的交易數: {stats['dust_count']}")
        print(f"Txn Output Amount 平均值: {stats['dust']['virtual_size']:.8f}")
        print(f"Txn Fee 平均值: {stats['dust']['txn_fee']:.8f}")
        print(f"Txn Fee Rate 平均值: {stats['dust']['txn_fee_rate']:.8f}")
        print(f"Txn Fee Ratio 平均值: {stats['dust']['txn_fee_ratio']:.8f}")
        print(f"Miner Verification Time 平均值: {stats['dust']['miner_verification_time']:.8f}")
        print(f"Virtual Txn Size 平均值: {stats['dust']['virtual_size']:.8f}")
    
    if 'non_dust' in stats:
        print("\n無 dust UTXO 交易的平均值:")
        print(f"無 dust UTXO 的交易數: {stats['non_dust_count']}")
        print(f"Txn Output Amount 平均值: {stats['non_dust']['virtual_size']:.8f}")
        print(f"Txn Fee 平均值: {stats['non_dust']['txn_fee']:.8f}")
        print(f"Txn Fee Rate 平均值: {stats['non_dust']['txn_fee_rate']:.8f}")
        print(f"Txn Fee Ratio 平均值: {stats['non_dust']['txn_fee_ratio']:.8f}")
        print(f"Miner Verification Time 平均值: {stats['non_dust']['miner_verification_time']:.8f}")
        print(f"Virtual Txn Size 平均值: {stats['non_dust']['virtual_size']:.8f}")
    
    print(f"\n總耗時: {time.time() - start_time:.2f} 秒")

if __name__ == '__main__':
    main()