import os
import json
import math
import shutil
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
import time
from tqdm import tqdm

def process_file(json_path):
    """Process a single JSON file and return its unique hashes"""
    file_hashes = set()
    try:
        with open(json_path, 'r', encoding='utf-8') as infile:
            json_data = json.load(infile)
            for item in json_data:
                input_details = json.loads(item['Txn Input Details'])
                output_details = json.loads(item['Txn Output Details'])
                
                # Add input hashes
                file_hashes.update(detail['inputHash'] for detail in input_details)
                # Add output hashes
                file_hashes.update(detail['outputHash'] for detail in output_details)
                
    except Exception as e:
        print(f"Error processing {json_path}: {e}")
    
    return file_hashes

def save_batch_wrapper(args):
    """Wrapper function for saving batch to handle multiple arguments"""
    data, output_folder, batch_num = args
    output_file_path = os.path.join(output_folder, f'unique_hashes_part_{batch_num}.json')
    with open(output_file_path, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, ensure_ascii=False, indent=2)
    return len(data)

def main():
    start_time = time.time()
    
    # Configuration
    json_file_path = 'test'
    # json_file_path = '0619-0811/0619-0723'
    output_folder = 'output_address_hashes'
    batch_size = 100000
    
    # Clean up output folder
    if os.path.exists(output_folder):
        print(f"Cleaning up existing folder: {output_folder}")
        shutil.rmtree(output_folder)
    os.makedirs(output_folder)
    print(f"Created clean output folder: {output_folder}")
    
    # Collect all JSON files
    json_files = []
    for root, dirs, files in os.walk(json_file_path):
        json_files.extend([
            os.path.join(root, file)
            for file in files if file.endswith('.json')
        ])
    
    print(f"Found {len(json_files)} JSON files to process")
    
    # Process files in parallel
    unique_hashes = set()
    num_cores = mp.cpu_count()
    print(f"Using {num_cores} CPU cores for processing")
    
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        # Use tqdm to show progress bar
        results = list(tqdm(
            executor.map(process_file, json_files),
            total=len(json_files),
            desc="Processing files"
        ))
    
    # Combine results
    for result in results:
        unique_hashes.update(result)
    
    print(f"Total unique hashes before filtering: {len(unique_hashes)}")
    
    # Filter and prepare data
    filtered_hashes = [hash_value for hash_value in unique_hashes if 'Unknown_' not in hash_value]
    unique_hashes_list = [{"address": hash_value} for hash_value in filtered_hashes]
    print(f"Unique hashes after filtering: {len(unique_hashes_list)}")
    
    # Calculate number of files needed
    num_files = math.ceil(len(unique_hashes_list) / batch_size)
    
    # Prepare batches
    batches = []
    for i in range(num_files):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(unique_hashes_list))
        batches.append((
            unique_hashes_list[start_idx:end_idx],
            output_folder,
            i + 1
        ))
    
    # Save files in parallel using the wrapper function
    print("Saving files...")
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        save_results = list(tqdm(
            executor.map(save_batch_wrapper, batches),
            total=len(batches),
            desc="Saving files"
        ))
    
    total_records_saved = sum(save_results)
    
    end_time = time.time()
    print(f"\nProcessing completed in {end_time - start_time:.2f} seconds")
    print(f"All files have been saved in the '{output_folder}' directory")
    print(f"Total number of files created: {num_files}")
    print(f"Total records saved: {total_records_saved}")

if __name__ == '__main__':
    main()