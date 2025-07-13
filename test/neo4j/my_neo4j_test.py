from neo4j import GraphDatabase


uri = "bolt://localhost:7681"  # ipaddress
username = "neo4j"             # username
password = "testtest"          # password


driver = GraphDatabase.driver(uri, auth=(username, password))
database_name = "neo4j"

def execute_create_statements(file_path):
    with driver.session(database=database_name) as session:
        
        session.run("match (n) detach delete n")
        session.run("CALL db.clearQueryCaches()")
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


def run(query: str):
    with driver.session(database=database_name) as session:
        result = session.run(query)
        di = result.data()

        res = result.consume()
        t1 = res.result_available_after
        t2 = res.result_consumed_after
        return di, t1



file_path = "query_producer/logs/composite/database101-cur.log"
execute_create_statements(file_path)
query1="MATCH (n0 :L4)-[r0 :T3]->(n1)-[r1 :T5]->(n2 :L1), (n3 :L1)-[r2 :T4]->(n1), (n4 :L2)<-[r3 :T5]-(n2)-[r4 :T2]->(n5 :L2), (n6 :L2)<-[r5 :T1]-(n2)-[r6 :T5]->(n7 :L2), (n8 :L0)<-[r7 :T2]-(n1), (n9 :L0)<-[r8 :T5]-(n7 :L2)-[r9 :T1]->(n10 :L6), (n11 :L3)-[r10 :T2]->(n10), (n12 :L0)-[r11 :T5]->(n5) WHERE ((r0.id) > -1) OPTIONAL MATCH (n3 :L1)-[]->(n1)<-[]-(n0) WHERE (('s28sYb3h' < ('R'+((n3.k12)+'b'))) OR ((((r8.k77) STARTS WITH (r8.k77)) AND ((n12.k1) <= (n0.k29))) OR (('t'+(n12.k2)) <> ((r1.k77)+(r0.k64))))) RETURN (r3.k80) AS a0, (n2.k8) AS a1, (n6.k15) AS a2;"

driver.close()
