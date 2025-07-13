from neo4j import GraphDatabase
from configs.conf import config
import time





driver = GraphDatabase.driver(config.get("memgraph", "uri"))
database_name = "neo4j"

def execute_create_statements(file_path):
    with driver.session() as session:
        session.run("match (n) detach delete n")
        with open(file_path, "r") as file:
            for line in file:
                if line.strip():  
                    try:
                        new_line = line
                        if "CREATE" in new_line or "MERGE" in new_line: 
                            session.run(new_line)  
                            print(f"Executed: {new_line}")
                    except Exception as e:
                        print(f"Failed to execute: {line.strip()}, Error: {e}")

def reproduce_test(test_file):
    with driver.session(database=database_name) as session:
        with open(file_path, "r") as file:
            for line in file:
                if line.strip():
                    line = line.strip()


def run(query:str, session):
    start = time.time()
    result = session.run(query, timeout=config.get("neo4j", "timeout"))
    di = result.data()
    end = time.time()
    elapsed = int((end - start) * 1000)
    return di, elapsed


# file path
file_path = "query_producer/logs/composite/database129-cur.log"
execute_create_statements(file_path)
query1="MATCH (n0 :L2)-[r0 :T0]->(n1 :L2)-[r1 :T0]->(n2 :L3), (n0 :L2)-[r2 :T1]->(n3 :L1)<-[r3 :T3]-(n4 :L2), (n5 :L3)<-[r4 :T1]-(n6)-[r5 :T1]->(n3 :L1), (n1)-[r6 :T2]->(n7 :L3)<-[r7 :T3]-(n8 :L2) MATCH (n9 :L4)<-[r8 :T1]-(n10 :L2)<-[r9 :T3]-(n11 :L0), (n10 :L2)<-[r10 :T2]-(n12 :L0)-[r11 :T5]->(n13 :L4), (n14)<-[r12 :T2]-(n15 :L4)-[r13 :T5]->(n13) OPTIONAL MATCH (n0)-[]->(n1 :L2)-[]->(n2) WHERE (('l' CONTAINS '9H8lM') AND ((r6.k47) <> (n15.k26))) OPTIONAL MATCH (n14)<-[]-(n15)-[]->(n13) WHERE (NOT (n0.k18)) RETURN min('3zxda6q') AS a0, (r6.k44) AS a1, (n2.k24) AS a2 ORDER BY a1 DESC;"
create="CREATE INDEX ON:T5(k77)"
delete = "DROP INDEX ON:T1(K77)"
query2="MATCH (n1 :L2)-[r1 :T0]->(n2 :L3),(n1)<-[r0 :T0]-(n0 :L2)-[r2 :T1]->(n3 :L1)<-[r3 :T3]-(n4 :L2),(n3)<-[r5 :T1]-(n6)-[r4 :T1]->(n5 :L3),(n1)-[r6 :T2]->(n7 :L3)<-[r7 :T3]-(n8 :L2) MATCH (n10 :L2)<-[r9 :T3]-(n11 :L0),(n9 :L4)<-[r8 :T1]-(n10)<-[r10 :T2]-(n12 :L0)-[r11 :T5]->(n13 :L4)<-[r13 :T5]-(n15 :L4),(n14)<-[r12 :T2]-(n15)  OPTIONAL MATCH (n0)-[]->(n1 :L2)-[]->(n2) WHERE (('l' CONTAINS '9H8lM') AND ((r6.k47) <> (n15.k26))) OPTIONAL MATCH (n14)<-[]-(n15)-[]->(n13) WHERE (NOT (n0.k18)) RETURN min('3zxda6q') AS a0, (r6.k44) AS a1, (n2.k24) AS a2 ORDER BY a1 DESC;"
result1 = run(query1, driver.session())
print(result1)

result2 = run(query2, driver.session())
print(result2)

driver.close()