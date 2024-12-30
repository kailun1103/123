import os
import json

json_file_path = 'utxo_txn_process/input_utxo_txn'

total = 0
for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_file)
            json_data = json.load(infile)
            for item in json_data:
                if "recieve_utxo_txn" in item:
                    print(item['recieve_utxo_txn'][0]['Virtual Txn Size'])

print(total)
