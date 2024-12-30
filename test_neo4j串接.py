from py2neo import Graph
import time

# 計時開始
start_time = time.time()

# 連接資料庫
graph = Graph("bolt://localhost:7687", auth=("kailun1103", "00000000"), name="highdust")

# 執行查詢
result = graph.run("""
    MATCH c = (ina:Address)-->(t:Transaction)-->(outa:Address)
    WHERE t.TxnHash = '2f6f7451bf7a28794920bd86b3b8c018e48914154b0a2615badff9d6fff5f19d'
    WITH ina
    MATCH c2 = (t2:Transaction)-->(ina)
    RETURN DISTINCT t2.TxnHash as largeAmountCount
""").data()

# 計算執行時間
execution_time = time.time() - start_time

print(f"查詢結果: {result}")
print(f"執行時間: {execution_time:.4f} 秒")