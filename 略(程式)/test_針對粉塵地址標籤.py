import json
import time
from neo4j import GraphDatabase
from multiprocessing import Pool, cpu_count

def process_addresses(addresses):
    URI = "bolt://localhost:7687"
    AUTH = ("kailun1103", "00000000")
    DATABASE = "temp"
    
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        input_results = get_input_addresses_result(addresses, DATABASE, driver)
        output_results = get_output_addresses_result(addresses, DATABASE, driver)
    
    updated_items = []
    for address in addresses:
        item = {
            'address': address,
            'input': 1 if address in input_results else 0,
            'output': 1 if address in output_results else 0
        }
        updated_items.append(item)
    
    return updated_items

def get_input_addresses_result(addresses, database, driver):
    with driver.session(database=database) as session:
        result = session.run(
            f"""
                MATCH (t:Transaction)-->(n:Address)
                WHERE n.address IN {addresses}
                RETURN n.address AS address
            """
        ).data()
    return {record['address'] for record in result}

def get_output_addresses_result(addresses, database, driver):
    with driver.session(database=database) as session:
        result = session.run(
            f"""
                MATCH (n:Address)-->(t:Transaction)
                WHERE n.address IN {addresses}
                RETURN n.address AS address
            """
        ).data()
    return {record['address'] for record in result}

if __name__ == '__main__':
    start_time = time.time()

    with open('unique_dust_addresses copy.json', 'r', encoding='utf-8') as infile:
        json_data = json.load(infile)
    
    # 获取所有地址
    all_addresses = [item['address'] for item in json_data]

    # 使用系统的CPU核心数作为进程数
    num_processes = cpu_count()
    
    # 分块处理地址
    chunk_size = 1000  # 每次处理 1000 个地址，可以根据实际情况调整
    address_chunks = [all_addresses[i:i + chunk_size] for i in range(0, len(all_addresses), chunk_size)]
    
    results = []
    # 创建进程池
    with Pool(processes=num_processes) as pool:
        for i, chunk_results in enumerate(pool.imap_unordered(process_addresses, address_chunks), 1):
            results.extend(chunk_results)
            print(f"已处理 {i * chunk_size}/{len(all_addresses)} 个地址")
    
    # 将更新后的数据写回 JSON 文件
    with open('updated_unique_dust_addresses2.json', 'w', encoding='utf-8') as outfile:
        json.dump(results, outfile, ensure_ascii=False, indent=4)

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"数据已更新并写入 unique_dust_addresses copy.json 文件")
    print(f"程序总执行时间: {execution_time:.2f} 秒")
