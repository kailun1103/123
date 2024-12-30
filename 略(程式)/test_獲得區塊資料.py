import requests
import sys
import json
import statistics
import time
import csv
import os
from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from neo4j import GraphDatabase, Driver


oklink_key = 'b5bae772-4c29-49bd-912b-7005e275837a'
    
block_inf = {}

def get_block_inf(block):
    global oklink_key
    global block_inf

    headers = {
        'Ok-Access-Key': oklink_key
    }
    payload = {
        "chainShortName": "btc",
        "height": block,
        "protocolType": "transaction",
        "limit": 100,
        "page": 1
    }
    response_block_list = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers, params=payload).json()
    total_page = response_block_list['data'][0]['totalPage']
    response_block_inf = requests.get(f"https://www.oklink.com/api/v5/explorer/block/block-fills?chainShortName=btc&height={block}", headers = headers).json()

    # step1. 獲取block_list的total_page數
    print(response_block_inf)
    
    # # step2. 獲得區塊的所有hash
    all_hashs_lists = []
    for i in range(1,int(total_page)+1):
        print(i)
        time.sleep(0.5)
        headers = {
            'Ok-Access-Key': oklink_key
        }
        payload = {
            "chainShortName": "btc",
            "height": block,
            "protocolType": "transaction",
            "limit": 100,
            "page": i
        }
        response_block_list = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers, params=payload)
        
        while True:
            if response_block_list.status_code == 200:
                break
            elif response_block_list.status_code == 429:
                time.sleep(2)
                response_block_list = requests.get("https://www.oklink.com/api/v5/explorer/block/transaction-list", headers=headers, params=payload)

        response_json = response_block_list.json()  # Parse the response text into a JSON object
        hashs_lists = response_json['data'][0]['blockList']
        all_hashs_lists.extend(hashs_lists)


    # step3. 根據區塊裡的每一個hash獲得交易詳細資訊
    all_hashs_detail = []
    for i in range(0, len(all_hashs_lists), 20):
        time.sleep(0.5)
        txids = [entry['txid'] for entry in all_hashs_lists[i:i+20]]
        txids_batch = ','.join(txids)
        headers = {
            'Ok-Access-Key': oklink_key
        }
        payload = {
            "chainShortName": "btc",
            "txid": txids_batch
        }
        response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
        print(response.status_code)
        while True:
            if response.status_code == 200:
                break
            elif response.status_code == 429:
                time.sleep(2)
                response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)


        response_json = response.json()
        txn_json = response_json['data']
        for transaction in txn_json:
            oklink_txn_item = {
                "TotalSize": transaction["totalTransactionSize"],
                "VirtualSize": transaction["virtualSize"],
                'Weight': transaction['weight'],
                "FeeRate": str((float(transaction['txfee'])/float(transaction['virtualSize']))*100000000)
            }
            all_hashs_detail.append(oklink_txn_item)

    total_sizes = []
    virtual_sizes = []
    weights = []
    fee_rates = []

    # 收集所有值
    for transaction in all_hashs_detail:
        total_sizes.append(float(transaction['TotalSize']))
        virtual_sizes.append(float(transaction['VirtualSize']))
        weights.append(float(transaction['Weight']))
        fee_rates.append(float(transaction['FeeRate']))

    # 計算中位數
    median_total_size = statistics.median(total_sizes)
    median_virtual_size = statistics.median(virtual_sizes)
    median_weight = statistics.median(weights)
    median_fee_rate = statistics.median(fee_rates)

    block_inf[block] = {
        'Block Hash': response_block_inf['data'][0]['hash'],
        'Block Weight': str(median_weight),
        'Block Fee Rate': str(median_fee_rate),
        'Block Total Txn Size': str(median_total_size),
        'Block Virtual Txn Size': str(median_virtual_size),
        'Block Validator': response_block_inf['data'][0]['validator'],
        'Block Date': response_block_inf['data'][0]['blockTime'],
        'Block Txn Count': response_block_inf['data'][0]['txnCount'],
        'Block Txn Amount': response_block_inf['data'][0]['amount'],
        'Block Size': response_block_inf['data'][0]['blockSize'],
        'Miner Reward': response_block_inf['data'][0]['mineReward'],
        'Block Txn Fees': response_block_inf['data'][0]['totalFee'],
        'Block Merkle Root Hash': response_block_inf['data'][0]['merkleRootHash'],
        'Block Miner Hash': response_block_inf['data'][0]['miner'],
        'Block Difficulty': response_block_inf['data'][0]['difficuity'],
        'Block Nonce': response_block_inf['data'][0]['nonce'],
        'Block Confirm': response_block_inf['data'][0]['confirm']
    }

    return block_inf[block]

if __name__ == '__main__':

    start_time = time.time()
    block_inf = get_block_inf(853736)
    print(block_inf)

    elapsed_time = time.time() - start_time
    print(f"運行時間: {elapsed_time:.2f} 秒")