import os
import json
from datetime import datetime

# 定義多個日期時間格式
datetime_formats = [
    '%Y/%m/%d %H:%M',  # 示例: 2024/1/18 00:00
    '%Y/%m/%d %I:%M:%S %p',  # 示例: 2024/01/18 12:02:11 AM
    '%Y-%m-%d %H:%M:%S',  # 示例: 2024-01-19 01:44:01
    '%Y/%m/%d %H:%M:%S'
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"時間數據 '{datetime_str}' 不符合任何已知格式。")

for i in range(20, 32): # 處理日期的迴圈，i為幾號
    json_file_path = '0619-0811/0619-0723' 
    output_dir = '0619-0723'

    # 如果輸出目錄不存在，則創建它
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 字典來保存每小時的交易
    hourly_transactions = {hour: [] for hour in range(24)}

    # 遍歷文件和文件夾
    for root, dirs, files in os.walk(json_file_path):
        json_files = [file for file in files if file.endswith('.json')]
        for json_file in json_files:
            print(json_file)
            json_path = os.path.join(root, json_file)
            with open(json_path, 'r', encoding='utf-8') as infile:
                json_data = json.load(infile)
                for txn in json_data:
                    # print(txn)
                    date = parse_datetime(txn['Txn Initiation Date'])
                    if date.date() == datetime(2024, 7, i).date():
                        hourly_transactions[date.hour].append(txn)

    count = 0
    # 將每小時的交易寫入單獨的JSON文件
    for hour, transactions in hourly_transactions.items():
        output_json_file_path = os.path.join(output_dir, f"BTX_Transaction_data_2024_07_{i}_{str(count)}.json")
        count += 1
        with open(output_json_file_path, 'w', encoding='utf-8') as outfile:
            json.dump(transactions, outfile, ensure_ascii=False, indent=4)

    print(f"每小時的交易已保存到 '{output_dir}' 目錄中。")
