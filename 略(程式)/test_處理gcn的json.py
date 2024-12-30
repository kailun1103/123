import json
import csv
from multiprocessing import Pool, cpu_count

def process_address(address):
    print(address)
    input_output = ''
    dust_count = 0
    nor_count = 0
    txn_input_avg = 0
    txn_output_avg = 0
    txn_count = 0
    input = False
    output = False
    txn_detail = []
    
    with open('updated_BTX_Transaction_data.json', 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
        for item in json_data:
            input_details = json.loads(item['Txn Input Details'])
            output_details = json.loads(item['Txn Output Details'])
            
            for detail in input_details:
                if detail['inputHash'] == address:
                    input = True
                    txn_detail.append(item)
            
            for detail in output_details:
                if detail['outputHash'] == address:
                    output = True
                    txn_detail.append(item)
    
    # 去除重複的交易記錄
    unique_txns = []
    seen_hashes = set()
    
    for txn in txn_detail:
        if txn['Txn Hash'] not in seen_hashes:
            seen_hashes.add(txn['Txn Hash'])
            unique_txns.append(txn)
    
    # 更新 txn_detail 為去重後的列表
    txn_detail = unique_txns

    input_count = 0
    input_amount = 0
    input_average = 0

    output_count = 0
    output_amount = 0
    output_average = 0

    for txn in txn_detail:
        txn_count += 1
        txn_input_avg += int(txn['Txn Input Address'])
        txn_output_avg += int(txn['Txn Output Address'])
        input_details = json.loads(txn['Txn Input Details'])
        output_details = json.loads(txn['Txn Output Details'])
        for addr in input_details:
            if addr['inputHash'] == address:
                input_count += 1
                input_amount += float(addr['amount'])
        for addr in output_details:
            if addr['outputHash'] == address:
                output_count += 1
                output_amount += float(addr['amount'])

    txn_input_avg = txn_input_avg / txn_count
    txn_output_avg = txn_output_avg / txn_count

    if input_count == 0:
        input_average = 0
        output_average = output_amount/output_count
        output_average = f"{format(output_average, '.8f')}"
    elif output_count == 0:
        output_average
        input_average = input_amount/input_count
        input_average = f"{format(input_average, '.8f')}"
    elif input_count == 0 and output_count == 0:
        input_average = 0
        output_average
    else:
        input_average = input_amount/input_count
        input_average = f"{format(input_average, '.8f')}"
        output_average = output_amount/output_count
        output_average = f"{format(output_average, '.8f')}"
    
    # 重新計算 dust_count 和 nor_count（因為可能有重複計算）
    dust_count = sum(1 for txn in unique_txns if txn['Dust Bool'] == '1')
    nor_count = sum(1 for txn in unique_txns if txn['Dust Bool'] == '0')
                    
    
    if input and output:
        input_output = 'input/output'
    elif input:
        input_output = 'input'
    elif output:
        input_output = 'output'
    
    return address, input_output, input_average, output_average, txn_detail, dust_count, nor_count, txn_input_avg, txn_output_avg

def process_chunk(chunk):
    results = []
    for item in chunk:
        address = item['address']
        # if 'Unknown' in address:
        #     print('continue')
        #     continue
        _, input_output, input_average, output_average, txn_detail, dust_count, nor_count, txn_input_avg, txn_output_avg = process_address(address)
        item['input_output'] = input_output
        item['total_txn_count'] = str(len(txn_detail))
        item['dust_bool_1_count'] = str(dust_count)
        item['dust_bool_0_count'] = str(nor_count)
        item['input_avg_amount'] = str(input_average)
        item['output_avg_amount'] = str(output_average)
        item['input_avg_count'] = str(txn_input_avg)
        item['output_avg_count'] = str(txn_output_avg)
        item['txn_detail'] = txn_detail

        results.append(item)
    return results

if __name__ == '__main__':
    json_path = '20241007_address_txn_statistics_cluster=-1,all-txn.json'

    with open(json_path, 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)

    # 將數據分成小塊
    chunk_size = len(json_data) // cpu_count() + 1
    chunks = [json_data[i:i+chunk_size] for i in range(0, len(json_data), chunk_size)]

    # 使用進程池並行處理
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_chunk, chunks)

    # 合併結果
    processed_data = [item for sublist in results for item in sublist]

    with open('20241007_address_txn_statistics_cluster=-1,all-txn(count input output).json', 'w', encoding='utf-8') as outfile:
        json.dump(processed_data, outfile, ensure_ascii=False, indent=2)

    print("處理完成")