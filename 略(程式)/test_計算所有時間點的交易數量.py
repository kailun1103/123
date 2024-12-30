import json
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import time

# Define multiple date-time formats
datetime_formats = [
    '%Y/%m/%d %H:%M',  # Example: 2024/1/18 00:00
    '%Y/%m/%d %I:%M:%S %p',  # Example: 2024/01/18 12:02:11 AM
    '%Y-%m-%d %H:%M:%S',  # Example: 2024-01-19 01:44:01
    '%Y/%m/%d %H:%M:%S',
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")

def process_file(json_path):
    file_hourly_counts = {
        'dust': {hour: 0 for hour in range(24)},
        'non_dust': {hour: 0 for hour in range(24)}
    }
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(os.path.basename(json_path))
        json_data = json.load(infile)
        for item in json_data:
            date = parse_datetime(item["Txn Initiation Date"])
            if item['Dust Bool'] == '1':
                file_hourly_counts['non_dust'][date.hour] += 1
                if item['Txn Input Address'] == '1' and item['Txn Output Address'] == '1':
                    file_hourly_counts['dust'][date.hour] += 1
                # else:
                #     file_hourly_counts['non_dust'][date.hour] += 1
    return file_hourly_counts

def main():
    json_file_path = '0619-0811/0619-0723'
    start_time = time.time()

    # Get all JSON files
    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    # Process files in parallel
    hourly_counts = {
        'dust': {hour: 0 for hour in range(24)},
        'non_dust': {hour: 0 for hour in range(24)}
    }
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_file, json_files)
        for file_counts in results:
            for dust_type in ['dust', 'non_dust']:
                for hour, count in file_counts[dust_type].items():
                    hourly_counts[dust_type][hour] += count

    # Print out the hourly counts
    print("Dust Count")
    for hour in range(24):
        dust_count = hourly_counts['dust'][hour]
        print(f"{dust_count}")

    print("Normal Count")
    for hour in range(24):
        normal_count = hourly_counts['non_dust'][hour]
        print(f"{normal_count}")

    print("ratio")
    for hour in range(24):
        dust_count = hourly_counts['dust'][hour]
        normal_count = hourly_counts['non_dust'][hour]
        ratio = (dust_count/normal_count)
        print(f"{ratio}")

    print('--------------------')
    dust_total = sum(hourly_counts['dust'].values())
    non_dust_total = sum(hourly_counts['non_dust'].values())
    print(f"Dust Total: {dust_total}")
    print(f"Non-Dust Total: {non_dust_total}")
    print(f"Grand Total: {dust_total + non_dust_total}")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()