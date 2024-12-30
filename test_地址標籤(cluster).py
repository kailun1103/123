import os
import json
import time
from concurrent.futures import ProcessPoolExecutor
from functools import partial

with open('BTX_Transaction_data_2024_01_18_12-13(address label).json', 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)

with open('gcn/gcn_dataset for one hour(已訓練)/20241007_address_txn_statistics.json', 'r', encoding='utf-8') as infile:
    addr_data = json.load(infile)

for item in json_data:
    input_details = json.loads(item['Txn Input Details'])
    for idetail in input_details:
        for add in addr_data:
            if add['address'] == idetail['inputHash']:
                idetail['cluster'] = add['cluster']

    output_details = json.loads(item['Txn Output Details'])
    for odetail in output_details:
        for add in addr_data:
            if add['address'] == odetail['outputHash']:
                odetail['cluster'] = add['cluster']

# Write back to file
with open('BTX_Transaction_data_2024_01_18_12-13(address label).json', 'w', encoding='utf-8') as outfile:
    json.dump(json_data, outfile, ensure_ascii=False, indent=2)

