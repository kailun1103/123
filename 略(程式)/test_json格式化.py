import os
import json
from datetime import datetime

# json_file_path = 'new dataset(sort) raw'
json_file_path = '0720-0811 json'
total = 0

Mempool_Txn_Count = 0

for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        custom_data = []
        
        # 讀取原始 JSON 文件
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(f"處理文件: {json_file}")
            json_data = json.load(infile)
            
            for item in json_data:
                if 'Block Height' in item:
                    if item['Txn State'] == 'Confirmed':
                        if float(item['Txn Output Amount']) != 0:
                            # if 'Block Miner Reward' in item:
                            #     if item['Block Miner Reward'] == '':
                            #         Block_Miner_Reward = item['Miner Reward']
                            #     else:
                            #         Block_Miner_Reward = item['Block Miner Reward']
                            # elif 'Miner Reward' in item:
                            #     if item['Miner Reward'] == '':
                            #         Block_Miner_Reward = item['Block Miner Reward']
                            #     else:
                            #         Block_Miner_Reward = item['Miner Reward']
                            
                            # if 'Rate gap' in item:
                            #     Rate_gap = f"{float(item['Rate gap']):.8f}"
                            # else:
                            #     Rate_gap = float(item['Txn Fee Rate']) - float(item['Block Fee Rate'])
                            #     Rate_gap = f"{Rate_gap:.8f}"


                            # 創建新的自定義項目
                            custom_item = {
                                "Txn Hash": item.get("Txn Hash", ""),
                                "Txn Initiation Date": item.get("Txn Initiation Date", ""),
                                "Txn Input Amount": item.get("Txn Input Amount", ""),
                                "Txn Output Amount": item.get("Txn Output Amount", ""),
                                "Txn Input Address": item.get("Txn Input Address", ""),
                                "Txn Output Address": item.get("Txn Output Address", ""),
                                "Txn Fee": item.get("Txn Fees", ""),
                                "Txn Weight": item.get("Txn Weight", ""),
                                "Txn State": item.get("Txn State", ""),
                                "Txn Verification Date": item.get("Txn Verification Date", ""),
                                "Txn Input Details": item.get("Txn Input Details", ""),
                                "Txn Output Details": item.get("Txn Output Details", ""),
                                "Txn Fee Rate": item.get("Txn Fee Rate", ""),
                                "Txn Fee Rate Indicator": item.get("Txn rate indicator", ""),
                                "Txn Fee Ratio": item.get("Txn Fee Ratio", ""),
                                "Dust Bool": item.get("Dust Bool", ""),
                                "Mempool Txn Count": item.get("Mempool Count", ""),
                                "Mempool Final Txn Date": item.get("Mempool Final Txn Date", ""),
                                "Memory Depth": item.get("Memory Depth", ""),
                                "Miner Verification Time": item.get("Miner Verification Time", ""),
                                "Total Txn Size": item.get("Total Txn Size", ""),
                                "Virtual Txn Size": item.get("Virtual Txn Size", ""),
                                "Block Height": item.get("Block Height", ""),
                                "Block Weight": item.get("Block Weight", ""),
                                "Block Fee Rate": item.get("Block Fee Rate", ""),
                                "Block Total Txn Size": item.get("Block Total Txn Size", ""),
                                "Block Virtual Txn Size": item.get("Block Virtual Txn Size", ""),
                                "Block Hash": item.get("Block Hash", ""),
                                "Block Validator": item.get("Block Validator", ""),
                                "Block Date": item.get("Block Date", ""),
                                "Block Txn Count": item.get("Block Txn Count", ""),
                                "Block Txn Amount": item.get("Block Txn Amount", ""),
                                "Block Size": item.get("Block Size", ""),
                                "Block Miner Reward": item.get("Miner Reward", ""),
                                "Block Txn Fees": item.get("Block Txn Fees", ""),
                                "Block Merkle Root Hash": item.get("Block Merkle Root Hash", ""),
                                "Block Miner Hash": item.get("Block Miner Hash", ""),
                                "Block Difficulty": item.get("Block Difficulty", ""),
                                "Block Nonce": item.get("Block Nonce", ""),
                                "Block Confirm": item.get("Block Confirm", "")
                            }
                            custom_data.append(custom_item)
                            total += 1
        
        # 將新的數據寫回原文件，覆蓋原內容
        with open(json_path, 'w', encoding='utf-8') as outfile:
            json.dump(custom_data, outfile, ensure_ascii=False, indent=2)
        
        print(f"已更新文件: {json_file}，處理了 {len(custom_data)} 個項目")

print(f"總共處理了 {total} 個項目")
print("所有文件已更新完成")