import json


with open('20241007_address_txn_statistics_cluster=0,all-dust(count input output).json', 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)
    add = set()
    count = 0
    amount = 0
    for item in json_data:
        amount += float(item['input_avg_count'])
        count+=1
        add.add(item['address'])
        #     # print(item['address'])

        # # if item['dust_bool_1_count'] == "output":
        # #     count+=1
        # # if float(item['input_avg_amount']) != 0:
        # #     count+=1
        # #     amount+=float(item['input_avg_amount'])

        #     for txn in item['txn_detail']:
        #         input_details = json.loads(txn['Txn Output Details'])
        #         for addr in input_details:
        #             if addr['outputHash'] == item['address']:
        #                 if float(addr['amount']) > 0.00001:
        #                     print(addr['outputHash'])
        #                     add.add(addr['outputHash'])
        #                     amount += float(addr['amount'])
        #                     count += 1

print(len(add))
print(count)
average = amount/count
print(f"平均金額: {format(average, '.8f')}")

output_data = [{"address": addr} for addr in add]

# 寫入新的 JSON 檔案
with open('new.json', 'w', encoding='utf-8') as outfile:
    json.dump(output_data, outfile, indent=2)
