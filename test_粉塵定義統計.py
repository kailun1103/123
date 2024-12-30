import os
import json
from multiprocessing import Pool, cpu_count
from functools import partial

def process_json_file(json_path):
    file_address = []
    file_amount = []
    file_address_amount = []
    count_dust = 0
    count_546 = 0
    amount_avg = 0
    
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(os.path.basename(json_path))
        json_data = json.load(infile)
        for item in json_data:
            if float(item['Txn Fee Ratio']) >= 10:
                count_dust += 1
                file_address.append(float(item['Txn Output Address']))
                file_amount.append(float(item['Txn Output Amount']))
                output_details = json.loads(item['Txn Output Details'])
                for detail in output_details:
                    if detail['amount'] != '0':
                        file_address_amount.append(float(detail['amount']))
                        if float(detail['amount']) <= 0.00000546:
                            count_546 += 1
    
    return file_address, file_amount, file_address_amount, count_dust, count_546

def main():
    json_file_path = '0619-0811/0619-0723'
    
    # 獲取所有 JSON 文件的完整路徑
    json_files = []
    for root, dirs, files in os.walk(json_file_path):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    
    # 創建進程池
    num_processes = cpu_count()  # 使用所有可用的 CPU 核心
    with Pool(num_processes) as pool:
        # 直接使用完整路徑進行處理
        results = pool.map(process_json_file, json_files)
    
    # 合併所有結果
    address = []
    amount = []
    address_amount = []
    total_dust_count = 0  # 新增總計數變數
    total_546_count = 0
    
    for file_address, file_amount, file_address_amount, count_dust, count_546 in results:  # 修改這裡以接收 count
        address.extend(file_address)
        amount.extend(file_amount)
        address_amount.extend(file_address_amount)
        total_dust_count += count_dust  # 累加每個文件的 count
        total_546_count += count_546
    
    # 計算並打印平均值
    if address: # 平均粉塵交易地址輸出數量
        average1 = sum(address) / len(address)
        print(f"Average address: {average1:.8f}")

    if amount: # 平均粉塵交易金額
        average2 = sum(amount) / len(amount)
        print(f"Average amount: {average2:.8f}")

    if address_amount:  # 平均粉塵交易地址金額(地址總金額/地址總數)
        average3 = sum(address_amount) / len(address_amount)
        print(f"Average address_amount: {average3:.8f}")
    
    print(f"Total count of dust transactions: {total_dust_count}")  # 打印總交易數
    print(f"Total count of 546 transactions: {total_546_count}")  # 打印總546數

if __name__ == '__main__':
    main()