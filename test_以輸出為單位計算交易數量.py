import os
import json
from multiprocessing import Pool, cpu_count

def process_file(json_path):
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(os.path.basename(json_path))
        json_data = json.load(infile)
        count = 0
        for item in json_data:
            output_details = json.loads(item['Txn Output Details'])
            for detail in output_details:
                if float(detail['amount']) <= 0.00000546:
                    count += 1
                # count += 1
    return count

if __name__ == '__main__':
    json_file_path = '0619-0811/0619-0723'

    all_json_files = []
    for root, dirs, files in os.walk(json_file_path):
        json_files = [os.path.join(root, file) for file in files if file.endswith('.json')]
        all_json_files.extend(json_files)

    # 使用可用的CPU核心數量
    num_processes = cpu_count()

    # 創建進程池
    with Pool(num_processes) as pool:
        results = pool.map(process_file, all_json_files)

    total = sum(results)
    print(f"Total count: {total}")