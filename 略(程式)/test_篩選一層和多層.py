import os
import json

json_file_path = 'test' # folder to store json files

for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            print(json_file)
            json_data = json.load(infile)
            count = 0
            for item in json_data:
                if item['Dust Bool'] == '1':
                    multi_layer = False
                    # check txn have multi_layer?
                    input_addresses = json.loads(item['Txn Input Details'])
                    for input_ in input_addresses:
                        for item2 in json_data:
                            output_addresses = json.loads(item2['Txn Output Details'])
                            for output_ in output_addresses:
                                if input_['inputHash'] == output_['outputHash']:
                                    multi_layer = True
                                    break # If there are multiple layers, it will break

                    output_addresses = json.loads(item['Txn Output Details'])
                    for output_ in output_addresses:#我要跟邊
                        for item2 in json_data:
                            input_addresses = json.loads(item2['Txn Input Details'])
                            for input_ in input_addresses:
                                if input_['inputHash'] == output_['outputHash']:
                                    multi_layer = True
                                    break
                    
                    if  multi_layer == True:
                        count += 1

print(count)
