import os
import json
from multiprocessing import Pool, cpu_count
from collections import defaultdict

def process_file(json_file, root):
    json_path = os.path.join(root, json_file)
    # 初始化結果字典
    results = {
        'count': 0,
        'total_amounts': defaultdict(float)
    }
    
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(f"處理檔案: {json_file}")
        json_data = json.load(infile)
        
        for item in json_data:
            if item['Dust Bool'] == '0':

                results['count'] += 1
                
                # 計算多個欄位的總和
                columns_to_analyze = [
                    'Txn Input Amount',
                    'Txn Output Amount',
                    'Txn Input Address',
                    'Txn Output Address',
                    'Txn Fee',
                    'Txn Fee Rate',
                    'Txn Fee Ratio',
                    'Miner Verification Time',
                    'Virtual Txn Size'
                ]
                
                for column in columns_to_analyze:
                    # try:
                    value = float(item[column])
                    results['total_amounts'][column] += value
                    # except (KeyError, ValueError) as e:
                    #     print(f"警告: 無法處理欄位 {column} 在檔案 {json_file} 中的值: {e}")
                        
    return results

def main():
    json_file_path = '0619-0723'
    all_json_files = []
    
    # 收集所有JSON檔案
    for root, dirs, files in os.walk(json_file_path):
        json_files = [file for file in files if file.endswith('.json')]
        all_json_files.extend([(file, root) for file in json_files])
    
    # 使用多處理程序處理檔案
    with Pool(processes=cpu_count()) as pool:
        results = pool.starmap(process_file, all_json_files)
    
    # 合併所有結果
    total_count = 0
    total_amounts = defaultdict(float)
    
    for result in results:
        total_count += result['count']
        for column, amount in result['total_amounts'].items():
            total_amounts[column] += amount
    
    # 輸出結果
    print(f"\n總共找到 {total_count} 筆符合條件的交易")
    
    if total_count > 0:
        print("\n各欄位的平均值：")
        for column, total in total_amounts.items():
            average = total / total_count
            print(f"{column} 平均值: {average:.8f}")
    else:
        print("沒有找到符合條件的交易")

if __name__ == '__main__':
    main()