import os
import json
import concurrent.futures

def process_json_file(input_path, output_path, columns_to_keep):
    try:
        # 讀取JSON檔案
        with open(input_path, 'r', encoding='utf-8') as infile:
            print(infile)
            # 載入JSON資料
            json_data = json.load(infile)
            
            # 如果是字典，轉換為列表
            if isinstance(json_data, dict):
                json_data = [json_data]
            
            # 儲存篩選後的資料
            filtered_data = []
            
            # 遍歷每筆記錄
            for record in json_data:
                # 建立只包含指定欄位的新字典
                if record['Dust Bool'] == '0':
                    filtered_record = {col: record.get(col, None) for col in columns_to_keep}
                    filtered_data.append(filtered_record)
            
            # 將篩選後的資料寫入新的JSON檔案
            with open(output_path, 'w', encoding='utf-8') as outfile:
                json.dump(filtered_data, outfile, indent=2, ensure_ascii=False)
            
            return len(filtered_data)
    
    except Exception as e:
        print(f"處理 {os.path.basename(input_path)} 時發生錯誤: {e}")
        return 0

def main():
    # 指定包含JSON檔案的目錄
    json_file_path = 'test'

    # 要保留的欄位列表
    columns_to_keep = [
        "Txn Hash", "Txn Initiation Date", "Txn Input Amount", "Txn Output Amount",
        "Txn Input Address", "Txn Output Address", "Txn Fee",
        "Txn Verification Date", "Txn Input Details", "Txn Output Details", "Txn Fee Rate",
        "Txn Fee Ratio", "Dust Bool", "Mempool Txn Count",
        "Miner Verification Time", "Virtual Txn Size"
    ]

    # 輸出目錄
    output_dir = 'processed_data'
    os.makedirs(output_dir, exist_ok=True)

    # 找出所有JSON檔案
    json_files = []
    for root, dirs, files in os.walk(json_file_path):
        json_files.extend([os.path.join(root, file) for file in files if file.endswith('.json')])

    # 使用concurrent.futures進行平行處理
    total_records = 0
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # 準備處理參數
        futures = []
        for input_path in json_files:
            output_path = os.path.join(output_dir, os.path.basename(input_path))
            futures.append(executor.submit(process_json_file, input_path, output_path, columns_to_keep))
        
        # 等待所有任務完成並收集結果
        for future in concurrent.futures.as_completed(futures):
            records = future.result()
            total_records += records

    print(f"總共處理了 {len(json_files)} 個檔案，共 {total_records} 筆記錄")

if __name__ == '__main__':
    main()