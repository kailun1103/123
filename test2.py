import os
import json
import time
from multiprocessing import Pool, Manager
from test_get_address_pipeline import get_oklink_txn, find_matching_transaction

def process_item(args):
    item, shared_dict = args
    
    # 更新進度
    with shared_dict['lock']:
        shared_dict['current_count'].value += 1
        current = shared_dict['current_count'].value
        total = shared_dict['total_items'].value
        print(f"\n進度: {current}/{total} ({(current/total*100):.2f}%)")
    
    try:
        # 處理當前項目
        output_details = json.loads(item['Txn Output Details'])
        item_results = {
            'recieve_utxo_bool': False,
            'recieve_utxo_txn': []
        }
        
        for detail in output_details:
            print(f"處理 output hash: {detail['outputHash']}")
            print(f"金額: {detail['amount']}")
            
            sorted_hashs_detail = get_oklink_txn(detail['outputHash'])
            result = find_matching_transaction(
                sorted_hashs_detail,
                int(item['Txn Verification Timestamp']),
                float(detail['amount'])
            )
            
            if result:
                with shared_dict['lock']:
                    shared_dict['match_count'].value += 1
                print(f"找到匹配的交易: {result['Txn Hash']}")
                item_results['recieve_utxo_bool'] = True
                item_results['recieve_utxo_txn'].append(result)
            else:
                print("未找到匹配的交易")
        
        return item, item_results
        
    except Exception as e:
        print(f"處理項目時發生錯誤: {str(e)}")
        return item, None

def main(num_cores=None):
    start_time = time.time()
    
    # 如果沒有指定核心數，使用系統可用核心數
    if num_cores is None:
        num_cores = os.cpu_count()
    
    print(f"使用 {num_cores} 個核心進行處理")
    
    # 使用 Manager 來創建共享對象
    with Manager() as manager:
        # 讀取JSON檔案
        with open('utxo_address.json', 'r', encoding='utf-8') as infile:
            json_data = json.load(infile)
            total_items = len(json_data)
        
        shared_dict = {
            'match_count': manager.Value('i', 0),      # 匹配計數器
            'current_count': manager.Value('i', 0),    # 當前處理筆數計數器
            'total_items': manager.Value('i', total_items),  # 總項目數
            'lock': manager.Lock()                     # 共享鎖
        }
        
        print(f"總共需要處理 {total_items} 筆資料")
        
        # 創建處理程序池
        pool = Pool(processes=num_cores)
        
        # 準備參數
        args = [(item, shared_dict) for item in json_data]
        
        # 使用進程池進行平行處理
        try:
            results = pool.map(process_item, args)
            
            # 更新原始數據
            for original_item, (_, result) in zip(json_data, results):
                if result is not None:
                    original_item['recieve_utxo_bool'] = result['recieve_utxo_bool']
                    original_item['recieve_utxo_txn'] = result['recieve_utxo_txn']
            
            # 將更新後的數據寫回文件
            with open('utxo_address_copy.json', 'w', encoding='utf-8') as outfile:
                json.dump(json_data, outfile, indent=2)
            
        except Exception as e:
            print(f"執行過程中發生錯誤: {str(e)}")
        finally:
            pool.close()
            pool.join()
        
        # 輸出統計資訊
        print(f"\n處理完成!")
        print(f"總共找到 {shared_dict['match_count'].value} 個匹配的交易")
        print(f"總共處理了 {shared_dict['current_count'].value} 筆資料")
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"運行時間: {elapsed_time:.2f} 秒")

if __name__ == '__main__':
    # 可以在這裡指定要使用的核心數量
    num_cores = 2  # 例如使用4個核心
    main(num_cores)