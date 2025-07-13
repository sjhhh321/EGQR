from gdb_clients import GdbFactory
from neo4j import GraphDatabase, basic_auth
from typing import List
from configs.conf import *
from neo4j.exceptions import ClientError
import neo4j


class Neo4j(GdbFactory):
    def __init__(self, uri, username, passwd, database="neo4j"):
        print("Neo4j driver version:", neo4j.__version__)
        self.database = database
        self.driver = GraphDatabase.driver(uri, auth=basic_auth(username, passwd))
        self.session = self.driver.session()

    def clear(self):
        self.run("MATCH (n) DETACH DELETE n")
        
    def clear_index(self):
        for i in range(2000):
            self.run(f"DROP INDEX node_index_{i} IF EXISTS")
            self.run(f"DROP INDEX edge_index_{i} IF EXISTS")
    

    def run(self, query: str):
        try:
            result = self.session.run(query, timeout=config.get("neo4j", "timeout"))
            di = result.data()

            res = result.consume()
            t1 = res.result_available_after
            t2 = res.result_consumed_after
            return di, t1
        except ClientError as e:
            print(f"query timeout: {e}")
            return None, None

    def get_execution_plan(self, query: str):
        result = self.session.run(query)
        result = result.consume()
        return result.plan
       
    def get_explain_query_plan(self, query: str):
        # remove_return_query = query.split("RETURN")[0].strip()
        #results= self.session.run(f"EXPLAIN {query}")
        explain_query = "EXPLAIN " + query
        
        summary = self.driver.execute_query(explain_query).summary
        # print(summary.plan['args']['string-representation'])
        return summary.plan['args']['string-representation']
        
    def get_profile_query_plan(self, query: str):
        
        result = self.session.run(f"PROFILE {query}")
        plan = result.consume().profile
        return plan
    
    def extract_plan_structure(self, plan):

        structure = {
            "operator_type": plan.get("operatorType"),
            "children": []
        }
        for child in plan.get("children", []):
            structure["children"].append(self.extract_plan_structure(child))

        return structure

    def compare_plan_structure(self, plan1, plan2):

        if plan1["operator_type"] != plan2["operator_type"]:
            return False  

        if len(plan1["children"]) != len(plan2["children"]):
            return False  

        for child1, child2 in zip(plan1["children"], plan2["children"]):
            if not self.compare_plan_structure(child1, child2):
                return False  

        return True


    def batch_run(self, queries: List[str]):
        self.clear()
        for stmt in queries:
            try:
                self.session.run(stmt)
            except Exception as e:
                print("create session error, ", e)


if __name__ == "__main__":
    neo4j = Neo4j(uri="bolt://localhost:10200", username=config.get('neo4j', 'username'), passwd=config.get('neo4j', 'passwd'))
    neo4j.clear()
