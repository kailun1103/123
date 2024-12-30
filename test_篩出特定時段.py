import os
import json
import time
from datetime import datetime
from multiprocessing import Pool, Value, Lock
from functools import partial
from itertools import chain
import ijson

# 創建共享計數器
counter = None
counter_lock = None

def init_counter():
    global counter, counter_lock
    counter = Value('i', 0)
    counter_lock = Lock()

def parse_datetime_cached(datetime_str):
    for fmt in ('%Y/%m/%d %H:%M', '%Y/%m/%d %I:%M:%S %p', 
                '%Y-%m-%d %H:%M:%S', '%Y/%m/%d %H:%M:%S'):
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"時間數據 '{datetime_str}' 不符合任何已知格式。")

def process_chunk(chunk, hour_filter=12):
    global counter, counter_lock
    filtered_chunk = []
    processed_in_chunk = 0
    
    for item in chunk:
        if item['Dust Bool'] == '1':
            dust_utxo = False
            try:
                item_datetime = parse_datetime_cached(item["Txn Initiation Date"])
                if item_datetime.hour == hour_filter:
                    output_details = json.loads(item['Txn Output Details'])
                    for outdetail in output_details:
                        if float(outdetail['amount']) <= 0.00000546 and float(outdetail['amount']) != 0:
                            dust_utxo = True
                            break
                if dust_utxo: 
                    filtered_chunk.append(item)
                
                processed_in_chunk += 1
                if processed_in_chunk % 5000 == 0:
                    with counter_lock:
                        counter.value += 5000
                        print(f"已處理: {counter.value:,} 筆數據")
            except (ValueError, json.JSONDecodeError) as e:
                print(f"處理項目時出錯: {e}")
    
    remaining = processed_in_chunk % 5000
    if remaining > 0:
        with counter_lock:
            counter.value += remaining
    
    return filtered_chunk

def load_json_in_chunks(filename, chunk_size=10000):
    """使用 ijson 流式讀取 JSON 數據"""
    current_chunk = []
    
    with open(filename, 'rb') as file:
        parser = ijson.items(file, 'item')
        
        for item in parser:
            current_chunk.append(item)
            
            if len(current_chunk) >= chunk_size:
                yield current_chunk
                current_chunk = []
        
        if current_chunk:
            yield current_chunk

def parallel_process_file(file_name, num_cores=9, chunk_size=10000):
    start_time = time.time()
    print(f"開始處理文件: {file_name}")
    print(f"使用 {num_cores} 個 CPU 核心進行處理")
    
    # 確保輸入文件存在
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"輸入文件不存在: {file_name}")
    
    # 創建輸出目錄（如果需要）
    output_dir = os.path.dirname(os.path.abspath(file_name))
    base_name = os.path.basename(file_name)
    output_file = os.path.join(output_dir, f'filtered_{base_name}')
    
    # 確保輸出目錄存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 讀取並處理文件
    all_chunks = []
    chunk_count = 0
    file_load_start = time.time()
    
    try:
        for chunk in load_json_in_chunks(file_name, chunk_size):
            all_chunks.append(chunk)
            chunk_count += 1
            if chunk_count % 10 == 0:
                print(f"已讀取 {chunk_count * chunk_size:,} 筆數據")
    except Exception as e:
        print(f"讀取文件時發生錯誤: {e}")
        return
    
    file_load_time = time.time() - file_load_start
    print(f"文件讀取耗時: {file_load_time:.2f} 秒")
    
    total_items = sum(len(chunk) for chunk in all_chunks)
    print(f"總數據量: {total_items:,} 條")
    print(f"分割成 {len(all_chunks)} 個區塊進行處理")
    
    # 平行處理數據
    process_start = time.time()
    with Pool(processes=num_cores, initializer=init_counter) as pool:
        results = pool.map(partial(process_chunk, hour_filter=12), all_chunks)
    process_time = time.time() - process_start
    
    # 合併結果
    filtered_transactions = list(chain.from_iterable(results))
    
    # 保存結果
    save_start = time.time()
    try:
        with open(output_file, 'w', encoding='utf-8') as outfile:
            json.dump(filtered_transactions, outfile, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"保存結果時發生錯誤: {e}")
        return
    save_time = time.time() - save_start
    
    total_time = time.time() - start_time
    
    # 輸出統計信息
    print(f"\n處理完成!")
    print(f"符合條件的交易筆數: {len(filtered_transactions):,}")
    print(f"\n時間統計:")
    print(f"文件讀取時間: {file_load_time:.2f} 秒")
    print(f"資料處理時間: {process_time:.2f} 秒")
    print(f"結果保存時間: {save_time:.2f} 秒")
    print(f"總處理時間: {total_time:.2f} 秒")
    print(f"\n結果已保存至: {output_file}")

if __name__ == '__main__':
    file_name = 'test/all_txn_data.json'
    parallel_process_file(file_name, num_cores=9)