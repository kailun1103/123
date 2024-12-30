import csv
import os
import json
import time
from datetime import datetime
csv.field_size_limit(2147483647)


data = []

total = 0

with open('20241007_address_txn_statistics.csv', newline='', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    count = 0
    for row in reader:
        data.append(row)
        count += 1

    with open('20241007_address_txn_statistics.json', 'w', encoding='utf-8') as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)
    total += count


    print(total)

