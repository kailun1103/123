import os
import json


with open('new.json', 'r', encoding='utf-8') as infile:
    json_data = json.load(infile)
    count = 0
    for item in json_data:
        # if item['input_output'] == 'input/output':
        count +=1

print(count)
