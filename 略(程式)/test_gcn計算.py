import json

with open('20241007_address_txn_statistics_cluster=-1,all-txn(count input output).json', 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)
    # 用字典收集每個地址的所有交易金額
    address_transactions = {}
    
    # 收集每個地址的所有交易
    for item in json_data:
        if item['input_output'] == 'output':
            for txn in item['txn_detail']:
                input_details = json.loads(txn['Txn Output Details'])
                for addr in input_details:
                    if addr['outputHash'] == item['address']:
                        if addr['outputHash'] not in address_transactions:
                            address_transactions[addr['outputHash']] = []
                        address_transactions[addr['outputHash']].append(float(addr['amount']))

    # 篩選出所有交易都 <= 0.00001 的地址
    valid_addresses = set()
    total_amount = 0
    total_count = 0

    for address, amounts in address_transactions.items():
        if all(amount <= 0.00001 for amount in amounts):
            valid_addresses.add(address)
            total_amount += sum(amounts)
            total_count += len(amounts)
            print(address)

print(f"\n符合條件的地址數量: {len(valid_addresses)}")
print(f"符合條件的交易數量: {total_count}")
if total_count > 0:
    average = total_amount/total_count
    print(f"平均金額: {format(average, '.8f')}")

# 寫入新的 JSON 檔案
output_data = [{"address": addr} for addr in valid_addresses]
with open('new.json', 'w', encoding='utf-8') as outfile:
    json.dump(output_data, outfile, indent=2)