from neo4j import GraphDatabase, basic_auth
from gdb_clients import GdbFactory
from configs.conf import config
from neo4j.exceptions import ClientError
import time
#from gqlalchemy import models
from typing import List

class MemGraph(GdbFactory):
    def __init__(self):
        #self.connection = Memgraph(host=config.get("memgraph", "uri"), port=config.getint("memgraph", "port"))
        self.driver = GraphDatabase.driver(config.get("memgraph", "uri"))
        self.session = self.driver.session()        
        #print(f"Memgraph port={self.connection.port}")
        #models.IGNORE_SUBCLASSNOTFOUNDWARNING = True
        self.clear()

    def run(self, query: str):
        try:
            start = time.time()
            result = self.session.run(query, timeout=config.get("neo4j", "timeout"))
            di = result.data()
            end = time.time()
            elapsed = int((end - start) * 1000)
            return di, elapsed
        except ClientError as e:
            print(f"query timeout: {e}")
            return None, None
        
    def get_explain_query_plan(self, query: str):
        # remove_return_query = query.split("RETURN")[0].strip()
        #results= self.session.run(f"EXPLAIN {query}")
        explain_query = "EXPLAIN " + query
        
        result = self.session.run(explain_query)
        plan_lines = [record[0] for record in result]
        # print(summary.plan['args']['string-representation'])
        return "\n".join(plan_lines)
    
    def batch_run(self, queries: List[str]):
        self.clear()
        for stmt in queries:
            try:
                self.session.run(stmt)
            except Exception as e:
                print("create session error, ", e)
    # def run(self, query):
    #     start_time = time.time()
    #     res = self.connection.execute_and_fetch(query)
    #     execution_time = time.time() - start_time
    #     return list(res), execution_time*100000

    # def batch_run(self, query):
    #     for q in query:
    #         self.connection.execute(q)
    def clear(self):
        self.run("MATCH (n) DETACH DELETE n")
    # def clear(self):
    #     self.connection.execute("MATCH (n) DETACH DELETE n")
