from neo4j import GraphDatabase
import json

URI = "bolt://localhost:7687"
AUTH = ("kailun1103", "00000000")
DATABASE = "btcforgcn"

def run_query_and_save_json():
    with GraphDatabase.driver(URI, auth=AUTH) as neo4j_driver:
        with neo4j_driver.session(database=DATABASE) as session:
            result = session.run(
                """
                MATCH (a:Address)-->(n:Transaction)
                WHERE ALL(transaction IN [(a)-->(t:Transaction) | t] WHERE transaction.DustBool = '1')
                RETURN a.address
                """
            ).data()

    # 提取地址並使用set過濾唯一值
    unique_addresses = set(record['a.address'] for record in result)
    
    # 創建所需的格式
    formatted_addresses = [{"address": address} for address in unique_addresses]
    # print("查詢到的唯一地址:", formatted_addresses)

    # 生成文件名
    filename = "unique_dust_addresses.json"

    # 將數據保存為 JSON 文件
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(formatted_addresses, f, ensure_ascii=False, indent=4)

    print(f"唯一地址已保存到文件: {filename}")

if __name__ == "__main__":
    run_query_and_save_json()