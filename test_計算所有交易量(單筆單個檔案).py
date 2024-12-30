import os
import json


total = 0

with open('utxo_txn_process/input_utxo_txn/input_utxo_address_833810(txn).json', 'r', encoding='utf-8') as infile:
  json_data = json.load(infile)
  count = 0
  for item in json_data:
    # Check if 'recieve_utxo_txn' key exists in the item dictionary
    if "recieve_utxo_txn" in item:
      count += len(item["recieve_utxo_txn"])
  total += count

print(total)