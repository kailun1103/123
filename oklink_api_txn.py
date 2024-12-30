import requests
import json

headers = {
    'Ok-Access-Key': 'b5bae772-4c29-49bd-912b-7005e275837a'
}
payload = {
    "chainShortName": "btc",
    "txid": "fc0af25e32950cbabe86c4a233493f3becf313d48c3c09ed80fccb91d9d3005d"
}

response = requests.get("https://www.oklink.com/api/v5/explorer/transaction/transaction-fills", headers=headers, params=payload)
response_data = response.json()  # 将响应数据解析为 JSON 格式
print(response_data)
# print(response_data['data'][0]['txfee'])
    
