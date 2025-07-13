from falkordb import FalkorDB


database = "falkor_misc"
db = FalkorDB(host='localhost', port=6381)
graph = db.select_graph(database)

def execute_create_statements(file_path):
    try:
        graph.query("match (n) detach delete n")
        print("Cleared existing data.")

        with open(file_path, "r") as file:
            for line in file:
                if line.strip():  
                    try:
                        new_line = line.strip()
                        if "CREATE" in new_line or "MERGE" in new_line:
                            new_line = new_line.replace(';', '')
                            graph.query(new_line)
                            print(f"Executed: {new_line}")
                    except Exception as e:
                        print(f"Failed to execute: {new_line}, Error: {e}")

    except Exception as e:
        print(f"Error connecting to Redis: {e}")

file_path = "query_producer/logs/composite/database0-cur.log"
execute_create_statements(file_path)
query0 = "MATCH (n0 :L1)-[r0 :T2]->(n1 :L0)<-[r1 :T5]-(n2), (n3 :L2)-[r2 :T6]->(n1 :L0), (n4)-[r3 :T3]->(n0 :L1) WHERE (((r3.k54) OR (n0.k8)) OR (NOT (n3.k12))) WITH (n3.k13) AS a0, min('pkMNg9ATb') AS a1 WHERE (2050878361 < 2139769018) UNWIND [-751293521, 1241635349] AS a2 RETURN DISTINCT a1"
query1 = "MATCH (n0)<-[r0 :T1]-(n1 :L0), (n2)-[r1 :T1]->(n0 :L2)<-[r2 :T1]-(n3), (n0)<-[r3 :T3]-(n4 :L4)<-[r4 :T2]-(n5) WHERE (n0.k13) WITH DISTINCT min('vg') AS a0 WHERE (true AND (NOT (true AND true))) MATCH (n0)<-[]-(n1) WHERE true MATCH (n2)-[]->(n0)<-[]-(n3) WITH a0 WHERE (a0 <> (a0+a0)) RETURN a0"
query2 = "MATCH (n0 :L3)-[r0 :T0]->(n1)-[r1 :T0]->(n2 :L5), (n2)-[r2 :T1]->(n3 :L0), (n0)<-[r3 :T6]-(n4 :L1), (n5 :L3)-[r4 :T3]->(n6 :L1)<-[r5 :T0]-(n3), (n0)<-[r6 :T0]-(n7), (n5 :L3)-[r7 :T4]->(n8 :L4) WHERE (r3.k76) UNWIND [(r5.k37)] AS a0 MATCH (n1)-[]->(n2)-[]->(n3) WHERE (NOT ((n2.k33) AND (r2.k45))) OPTIONAL MATCH (n3)<-[]-(n2 :L5)<-[]-(n1) WHERE ((r4.k53) CONTAINS ((r3.k75)+((n0.k22)+((r7.k59)+(r2.k43))))) OPTIONAL MATCH (n1)<-[]-(n0)<-[]-(n7) WHERE (n5.k19) RETURN (n8.k27) AS a1, (r0.k39) AS a2"
query3 = "MATCH (n0 :L1) WHERE (((-1373946010 <= (n0.k7)) OR (((n0.k8) OR (n0.k8)) AND ((n0.k10) ENDS WITH '5a'))) OR ((n0.k10) CONTAINS (n0.k10))) MATCH (n0) WHERE (2021140772 >= (n0.k6)) WITH DISTINCT (n0.k6) AS a0, max('dPSJt') AS a1, (n0.k6) AS a2 WHERE true RETURN 2 AS a3"

result0 = graph.query(query0).result_set
result = graph.query(query1).result_set
result2 = graph.query(query2).result_set
result3 = graph.query(query3).result_set

print(result)