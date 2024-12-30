import os
import json
from multiprocessing import Pool, cpu_count

dust_ratio = 5
json_file_path = '0619-0811/0619-0723'

def process_json_file(json_path):
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(infile)
        json_data = json.load(infile)
        dust_count = sum(1 for item in json_data if float(item['Txn Fee Ratio']) >= dust_ratio)
    return dust_count

def main():
    json_files = []
    for root, dirs, files in os.walk(json_file_path):
        json_files.extend([os.path.join(root, file) for file in files if file.endswith('.json')])

    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_json_file, json_files)

    dust_total = map(sum, zip(*results))

    print(f"{dust_ratio}%, Dust Total: {dust_total}")

if __name__ == '__main__':
    main()