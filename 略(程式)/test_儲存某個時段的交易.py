import csv
import os
import json
from datetime import datetime

# Define multiple date-time formats
datetime_formats = [
    '%Y/%m/%d %H:%M',  # Example: 2024/1/18 00:00
    '%Y/%m/%d %I:%M:%S %p',  # Example: 2024/01/18 12:02:11 AM
    '%Y-%m-%d %H:%M:%S',  # Example: 2024-01-19 01:44:01
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")





json_file_path = '2024_01_18-2024_01_24 - 資料集(json)/2024_01_18'
output_json_file_path = 'filtered_transactions_12_to_1.json'

# Dictionary to hold hourly counts
hourly_counts = {hour: 0 for hour in range(24)}
filtered_transactions = []

# Traverse through files and folders
for root, dirs, files in os.walk(json_file_path):
    json_files = [file for file in files if file.endswith('.json')]
    for json_file in json_files:
        print(json_file)
        json_path = os.path.join(root, json_file)
        with open(json_path, 'r', encoding='utf-8') as infile:
            json_data = json.load(infile)
            for txn in json_data:
                date = parse_datetime(txn['Txn Initiation Date'])
                hourly_counts[date.hour] += 1
                if date.hour == 12:
                    filtered_transactions.append(txn)

# Print out the hourly counts
for hour, count in hourly_counts.items():
    print(f"{hour}: {count}")

print('--------------------')
print('Total:', sum(hourly_counts.values()))

# Write the filtered transactions to a new JSON file
with open(output_json_file_path, 'w', encoding='utf-8') as outfile:
    json.dump(filtered_transactions, outfile, ensure_ascii=False, indent=4)

print(f"Filtered transactions have been saved to {output_json_file_path}")
