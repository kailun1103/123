import os
import json

# 指定包含JSON檔案的目錄
json_file_path = '0619-0811/0619-0723/week 1 19-25/2024_06_19'

# 要保留的欄位列表
columns_to_keep = [
    "Txn Hash", "Txn Initiation Date", "Txn Input Amount", "Txn Output Amount",
    "Txn Input Address", "Txn Output Address", "Txn Fee",
    "Txn Verification Date", "Txn Input Details", "Txn Output Details", "Txn Fee Rate",
    "Txn Fee Ratio", "Dust Bool", "Mempool Txn Count",
    "Miner Verification Time", "Virtual Txn Size"
]

# 合併後的輸出檔案
output_file = '2024_06_19.json'

# 儲存所有合併的記錄
merged_data = []

# 總記錄計數器
total_records = 0
total_files = 0

# 遍歷目錄
for root, dirs, files in os.walk(json_file_path):
    # 篩選JSON檔案
    json_files = [file for file in files if file.endswith('.json')]
    
    for json_file in json_files:
        # 建立完整的輸入路徑
        input_path = os.path.join(root, json_file)
        
        try:
            # 讀取JSON檔案
            with open(input_path, 'r', encoding='utf-8') as infile:
                print(infile)
                # 載入JSON資料
                json_data = json.load(infile)
                
                # 如果是字典，轉換為列表
                if isinstance(json_data, dict):
                    json_data = [json_data]
                
                # 遍歷每筆記錄
                for record in json_data:
                    # 建立只包含指定欄位的新字典
                    filtered_record = {col: record.get(col, None) for col in columns_to_keep}
                    merged_data.append(record)
                
                # 增加檔案計數器和記錄計數器
                total_files += 1
                total_records += len(json_data)
                print(f"已處理: {json_file} (共 {len(json_data)} 筆記錄)")
        
        except Exception as e:
            print(f"處理 {json_file} 時發生錯誤: {e}")

# 將合併的資料寫入輸出檔案
with open(output_file, 'w', encoding='utf-8') as outfile:
    json.dump(merged_data, outfile, indent=2, ensure_ascii=False)

print(f"總共處理了 {total_files} 個檔案，共 {total_records} 筆記錄")
print(f"已將資料合併至 {output_file}")