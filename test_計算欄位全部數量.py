import json
import sys
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import time
import numpy as np

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
    json_path, txn_fields = args
    field_values = {field: [] for field in txn_fields}
    txn_count = 0
    
    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        print(f"Processing: {json_path}")
        for item in json_data:
            if item['Dust Bool'] == '0':
                txn_count += 1
                for field in txn_fields:
                    value = float(item[field])
                    field_values[field].append(value)
    
    return field_values, txn_count

def calculate_statistics(values):
    if not values:
        return None
    
    values_array = np.array(values)
    return {
        'mean': np.mean(values_array),
        'q1': np.percentile(values_array, 25),
        'q2': np.percentile(values_array, 50),  # median
        'q3': np.percentile(values_array, 75),
        'q4': np.max(values_array),  # maximum
        'count': len(values_array)
    }

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <txn_field1> <txn_field2> ...")
        sys.exit(1)

    txn_fields = sys.argv[1:]
    json_file_path = '0619-0811/0619-0723'
    start_time = time.time()

    # Get list of JSON files
    json_files = []
    for root, _, files in os.walk(json_file_path):
        json_files.extend(os.path.join(root, file) for file in files if file.endswith('.json'))

    # Initialize containers for values
    all_values = {field: [] for field in txn_fields}
    total_txn_count = 0
    
    # Process files in parallel
    with ProcessPoolExecutor() as executor:
        results = executor.map(process_file, [(file, txn_fields) for file in json_files])
        
        # Combine results
        for file_values, file_count in results:
            total_txn_count += file_count
            for field in txn_fields:
                all_values[field].extend(file_values[field])

    # Calculate and print statistics for each field
    print("\n=== Overall Statistics ===\n")
    print(f"Total Transactions Processed: {total_txn_count}\n")
    
    for field in txn_fields:
        print(f'=== {field} ===')
        stats = calculate_statistics(all_values[field])
        if stats:
            print(f"Mean: {stats['mean']:.8f}")
            print(f"Q1 (25%): {stats['q1']:.8f}")
            print(f"Q2 (50%): {stats['q2']:.8f}")
            print(f"Q3 (75%): {stats['q3']:.8f}")
            print(f"Q4 (100%): {stats['q4']:.8f}")
            print(f"Count: {stats['count']}")
        print('')

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")

if __name__ == "__main__":
    main()