import json
import time
import os
from py2neo import Graph, Node, Relationship, Subgraph


def create_subgraph(txn_hash, Txn_Input_Details, Txn_Output_Details, address_cache, transaction):
    nodes = []
    relationships = []

    # Create Transaction node
    txn_node = Node("Transaction", 
        TxnHash=txn_hash,
        TxnInitiationDate=transaction['Txn Initiation Date'],
        TxnVerificationDate=transaction['Txn Verification Date'],
        TxnInputAmount=transaction['Txn Input Amount'],
        TxnOutputAmount=transaction['Txn Output Amount'],
        TxnInputAddress=transaction['Txn Input Address'],
        TxnOutputAddress=transaction['Txn Output Address'],
        TxnFee=transaction['Txn Fee'],
        TxnWeight=transaction['Txn Weight'],
        TxnState=transaction['Txn State'],
        TxnInputDetails=transaction['Txn Input Details'],
        TxnOutputDetails=transaction['Txn Output Details'],
        TxnFeeRate=transaction['Txn Fee Rate'],
        TxnFeeRatio=transaction['Txn Fee Ratio'],
        DustBool=transaction['Dust Bool'],
        MempoolTxnCount=transaction['Mempool Txn Count'],
        MempoolFinalTxnDate=transaction['Mempool Final Txn Date'],
        MemoryDepth=transaction['Memory Depth'],
        MinerVerificationTime=transaction['Miner Verification Time'],
        TotalTxnSize=transaction['Total Txn Size'],
        VirtualTxnSize=transaction['Virtual Txn Size'],
        BlockHeight=transaction['Block Height'],
        BlockHash=transaction['Block Hash'],
        BlockValidator=transaction['Block Validator'],
        BlockDate=transaction['Block Date'],
        BlockTxnCount=transaction['Block Txn Count'],
        BlockTxnAmount=transaction['Block Txn Amount'],
        BlockSize=transaction['Block Size'],
        BlockMinerReward=transaction['Block Miner Reward'],
        BlockTxnFees=transaction['Block Txn Fees'],
        BlockMerkleRootHash=transaction['Block Merkle Root Hash'],
        BlockMinerHash=transaction['Block Miner Hash'],
        BlockDifficulty=transaction['Block Difficulty'],
        BlockNonce=transaction['Block Nonce'],
        BlockConfirm=transaction['Block Confirm']
    )

    nodes.append(txn_node)

    input_count = 0
    # Create input Address nodes and SENT relationships using cache
    for inp in Txn_Input_Details:
        input_address = inp['inputHash']
        input_cluster = inp.get('cluster', 'Unknown')
        if input_address not in address_cache:
            address_node = Node("Address", address=input_address, Cluster=input_cluster)
            # address_node = Node("Address", address=input_address)
            address_cache[input_address] = address_node
        else:
            address_node = address_cache[input_address]
        sent_rel = Relationship(address_node, f"SENT_{input_count}", txn_node, amount=inp['amount'])
        relationships.append(sent_rel)
        input_count += 1

    output_count = 0
    # Create output Address nodes and RECEIVED relationships using cache
    for out in Txn_Output_Details:
        output_address = out['outputHash']
        output_cluster = out.get('cluster', 'Unknown')
        if output_address not in address_cache:
            address_node = Node("Address", address=output_address, Cluster=output_cluster)
            # address_node = Node("Address", address=output_address)
            address_cache[output_address] = address_node
        else:
            address_node = address_cache[output_address]
        received_rel = Relationship(txn_node, f"RECEIVED_{output_count}", address_node, amount=out['amount'])
        relationships.append(received_rel)
        output_count += 1

    return Subgraph(nodes, relationships)

def process_transactions(graph_uri, auth, database, json_data, batch_size, json_file, file_counter, file_total, address_cache):
    graph = Graph(graph_uri, auth=auth, name=database)
    combined_subgraph = Subgraph()
    for index, transaction in enumerate(json_data, start=1):
        txn_hash = transaction['Txn Hash']
        Txn_Input_Details = json.loads(transaction['Txn Input Details'])
        Txn_Output_Details = json.loads(transaction['Txn Output Details'])
        subgraph = create_subgraph(txn_hash, Txn_Input_Details, Txn_Output_Details, address_cache, transaction)
        combined_subgraph |= subgraph
        
        # Write to the database every batch_size transactions
        if index % batch_size == 0:
            graph.create(combined_subgraph)
            combined_subgraph = Subgraph()  # Reset subgraph
            print(f"Processing file {file_counter}/{file_total}: {json_file}, Processed batch {index//batch_size}, transaction {index}/{len(json_data)}, catch length: {len(address_cache)}")

    # Process remaining transactions
    if combined_subgraph:
        graph.create(combined_subgraph)

def save_address_cache(address_cache, output_dir, addresses_per_file=100000):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    sorted_addresses = sorted(address_cache.keys())
    file_counter = 1
    for i in range(0, len(sorted_addresses), addresses_per_file):
        chunk = sorted_addresses[i:i+addresses_per_file]
        chunk_dict = {addr: str(address_cache[addr]) for addr in chunk}
        
        output_file = os.path.join(output_dir, f'address_cache_{file_counter}.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(chunk_dict, f, ensure_ascii=False, indent=4)
        
        print(f'Saved {len(chunk)} addresses to {output_file}')
        file_counter += 1

if __name__ == '__main__':
    URI = "bolt://localhost:7687"
    AUTH = ("kailun1103", "00000000")
    DATABASE = "gcn"  # Specify your database name here
    address_cache = {}

    start_time = time.time()

    json_file_path = 'test'
    file_total = sum(len(files) for _, _, files in os.walk(json_file_path) if any(file.endswith('.json') for file in files))
    file_counter = 1
    for root, dirs, files in os.walk(json_file_path):
        json_files = [file for file in files if file.endswith('.json')]
        for json_file in json_files:
            json_path = os.path.join(root, json_file)
            with open(json_path, mode='r', newline='', encoding='utf-8') as infile:
                json_data = json.load(infile)

                batch_size = 30
                process_transactions(URI, AUTH, DATABASE, json_data, batch_size, json_file, file_counter, file_total, address_cache)
                file_counter += 1

    end_time = time.time()
    total_time = end_time - start_time  # Calculate total runtime
    print(f'DONE. Total time taken: {total_time:.2f} seconds')

    # Save address_cache to multiple JSON files
    # output_dir = 'address_cache_files'
    # addresses_per_file = 100000  # You can adjust this number
    # save_address_cache(address_cache, output_dir, addresses_per_file)
    # print(f'Address cache saved to multiple files in {output_dir}')