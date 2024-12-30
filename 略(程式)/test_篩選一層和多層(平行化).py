import os
import json
from multiprocessing import Pool, cpu_count

def process_json_file(json_path):
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(infile)
        json_data = json.load(infile)
        count = 0
        multi_items = []
        one_items = []
        
        for item in json_data:
            
            count += 1
            print(count)
            # if item['Dust Bool'] == '0':
                # print(item['Txn Hash'])
            multi_layer = False
            
            # Check input addresses
            input_addresses = json.loads(item['Txn Input Details'])
            for input_ in input_addresses:
                for item2 in json_data:
                    output_addresses = json.loads(item2['Txn Output Details'])
                    if any(input_['inputHash'] == output_['outputHash'] for output_ in output_addresses):
                        multi_layer = True
                        break
                if multi_layer:
                    break
            
            # Check output addresses if not already found
            if not multi_layer:
                output_addresses = json.loads(item['Txn Output Details'])
                for output_ in output_addresses:
                    for item2 in json_data:
                        input_addresses = json.loads(item2['Txn Input Details'])
                        if any(input_['inputHash'] == output_['outputHash'] for input_ in input_addresses):
                            multi_layer = True
                            break
                    if multi_layer:
                        break
            
            if multi_layer:
                multi_items.append(item)
            else:
                one_items.append(item)
        
        # Write multi-layer items to multi.json
    with open('multi_layer(all dust).json', 'a', encoding='utf-8') as multi_file:
        json.dump(multi_items, multi_file, ensure_ascii=False, indent=2)
    
    # Write one-layer items to one.json
    with open('one_layer(all dust).json', 'a', encoding='utf-8') as one_file:
        json.dump(one_items, one_file, ensure_ascii=False, indent=2)

    print('多層')
    print(len(multi_items))
    print('單層')
    print(len(one_items))


    return count

def main():
    json_path = 'dust_merged_result.json'

    results = process_json_file(json_path)
    print(len(results))
    
    

if __name__ == "__main__":
    main()