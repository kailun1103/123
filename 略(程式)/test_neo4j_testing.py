from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
AUTH = ("kailun1103", "00000000")
DATABASE = "test"

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    with driver.session(database=DATABASE) as session:
        result = session.run(
            """
                MATCH c = (a1:Address)<--(t1:Transaction)-->(a2:Address)-->(t2:Transaction)-->(a3:Address)
                WHERE a1 <> a2 <> a3 AND a1 <> a3 AND t1 <> t2
                AND NOT EXISTS {
                MATCH (t0:Transaction)-->(a1)
                WHERE t0 <> t1
                }
                AND NOT EXISTS {
                MATCH (a3)-->(t3:Transaction)
                WHERE t3 <> t2
                }
                RETURN t1.TxnHash, t2.TxnHash
                LIMIT 10
            """
        ).data()

print(result)
print(len(result))