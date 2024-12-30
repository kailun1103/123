import requests
import json

headers = {
    'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
}
payload = {
    "chainShortName": "btc",
    "address": 'bc1qdj8dxppx6gyukpemsqx06a39877rt8kcc08uqs',
    "limit": 5,
    "page": 1
}

response = requests.get("https://www.oklink.com/api/v5/explorer/address/transaction-list", headers=headers, params=payload)
response_data = response.json()  # 将响应数据解析为 JSON 格式
print(response_data['data'][0])
# print(response_data['data'][0]['txfee'])
    
