import csv
import os
import time
from datetime import datetime

# 設置 CSV 文件的字段大小限制
csv.field_size_limit(2147483647)

# 指定 CSV 文件所在的目錄路徑
csv_file_path = '0720-0811 csv'  # 更改為你的實際目錄路徑

# 定義需要添加的標題行
header = [
    'System Time', 'Txn Hash', 'Txn Initiation Date', 'Txn Input Amount', 
    'Txn Output Amount', 'Txn Input Address', 'Txn Output Address', 
    'Txn Fees', 'Mempool Count', 'Mempool Final Txn Date'
]

# 定義一個函數來為 CSV 文件添加標題行
def add_header_to_csv(file_path):
    # 創建一個臨時文件路徑
    temp_file_path = file_path + '.tmp'
    
    # 打開原始 CSV 文件進行讀取，並創建一個臨時文件進行寫入
    with open(file_path, mode='r', newline='', encoding='utf-8') as infile, \
         open(temp_file_path, mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        # 寫入新的標題行
        writer.writerow(header)
        
        # 寫入現有的數據行
        for row in reader:
            writer.writerow(row)
    
    # 用修改過的臨時文件替換原始文件
    os.replace(temp_file_path, file_path)

# 遍歷指定目錄中的所有 CSV 文件
total = 0
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        csv_path = os.path.join(root, csv_file)
        print(f'Processing {csv_file}')
        
        # 為每個 CSV 文件添加標題行
        add_header_to_csv(csv_path)
        print(f'Header added to {csv_file}')
