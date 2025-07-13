import redis
from redisgraph import Graph

redis_host = "localhost"
redis_port = 6379
redis_graph_name = "graphdb"  


r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
graph = Graph(redis_graph_name, r)
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


file_path = "query_producer/logs/composite/database103-cur.log"
execute_create_statements(file_path)

r.close()