import csv
import os
import json
import time
from datetime import datetime
csv.field_size_limit(2147483647)


csv_file_path = '0720-0811 csv'
output_dir = '0720-0811 json'

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

data = []

total = 0
for root, dirs, files in os.walk(csv_file_path):
    csv_files = [file for file in files if file.endswith('.csv')]
    for csv_file in csv_files:
        # time.sleep(0.5)
        csv_path = os.path.join(root, csv_file)
        data = []
        with open(csv_path, newline='', encoding='utf-8') as infile:
            print(csv_file)
            json_file = csv_file.replace('.csv','')
            reader = csv.DictReader(infile)
            count = 0
            for row in reader:
                data.append(row)
                count += 1

            with open(f'{output_dir}/{json_file}.json', 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
            total += count


    print(total)

