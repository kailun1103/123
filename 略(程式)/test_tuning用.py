import json
import sys
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import time

# Define multiple date-time formats
datetime_formats = [
    '%Y/%m/%d %H:%M',
    '%Y/%m/%d %I:%M:%S %p',
    '%Y-%m-%d %H:%M:%S',
    '%Y/%m/%d %H:%M:%S',
]

def parse_datetime(datetime_str):
    for fmt in datetime_formats:
        try:
            return datetime.strptime(datetime_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Time data '{datetime_str}' does not match any known formats.")

def process_file(args):
    json_path, txn_field = args
    hourly_data = {hour: [] for hour in range(24)}
    with open(json_path, 'r', encoding='utf-8') as infile:
        print(infile)
        json_data = json.load(infile)
        for item in json_data:
            if item['Dust Bool'] == '0':
                date = parse_datetime(item["Txn Initiation Date"])
                hour = date.hour
                try:
                    value = float(item[txn_field])
                    txn_hash = item['Txn Hash']
                    hourly_data[hour].append({
                        txn_field: value,
                        'Dust Bool':item['Dust Bool'],
                        'Txn Hash': txn_hash
                    })
                except (ValueError, KeyError):
                    pass
    return hourly_data

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <txn_field>")
        sys.exit(1)
    
    txn_field = sys.argv[1]
    json_file_path = '0619-0719 sort'
    start_time = time.time()

    # Get all JSON files
    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    # Process files in parallel
    hourly_data = {hour: [] for hour in range(24)}
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_file, [(file, txn_field) for file in json_files])
        for file_data in results:
            for hour, data in file_data.items():
                hourly_data[hour].extend(data)

    # Create output directory
    output_dir = f"{txn_field}_hourly_data_sorted"
    os.makedirs(output_dir, exist_ok=True)

    # Sort and save hourly data to JSON files
    for hour in range(24):
        if hourly_data[hour]:
            # Sort the data based on txn_field value
            sorted_data = sorted(hourly_data[hour], key=lambda x: x[txn_field])
            
            filename = os.path.join(output_dir, f"hour_{hour:02d}.json")
            with open(filename, 'w', encoding='utf-8') as outfile:
                json.dump(sorted_data, outfile, indent=2)
            print(f"Saved sorted data for hour {hour} to {filename}")
        else:
            print(f"No data for hour {hour}")

    # Calculate and print averages (keeping this part for reference)
    hourly_averages = {hour: sum(item[txn_field] for item in data) / len(data) if data else 0 for hour, data in hourly_data.items()}
    
    print('\nHourly Averages:')
    print(txn_field)
    print('')
    for hour in range(24):
        if hourly_averages[hour] != 0:
            print(f"{hour:02d}: {hourly_averages[hour]:.8f}")
        else:
            print(f"{hour:02d}: No data")

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nExecution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()