import os
import json

# 指定 JSON 文件所在的目錄路徑
json_file_path = 'matching_dust_transactions'

# 初始化總計數
total = 0
seen_hashes = set()
# 遍歷指定目錄中的所有 JSON 文件
for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(f'Processing {json_file}')
            json_data = json.load(infile)

            # 初始化一個字典來追踪已經出現過的Txn Hash
            
            unique_transactions = []

            # 遍歷每個交易
            for txn in json_data:
                txn_hash = txn["Txn Hash"]
                # 如果這個Txn Hash沒有出現過，則添加到結果列表中
                if (txn_hash not in seen_hashes):
                    unique_transactions.append(txn)
                    seen_hashes.add(txn_hash)

        # 將去重後的交易數據寫回同一個文件
        with open(json_path, 'w', encoding='utf-8') as outfile:
            json.dump(unique_transactions, outfile, ensure_ascii=False, indent=4)

#         print(f'Finished processing {json_file}')

# print("All files have been processed.")
