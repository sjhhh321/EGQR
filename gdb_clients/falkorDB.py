from falkordb import FalkorDB

from gdb_clients import GdbFactory

class Falkor(GdbFactory):
    def __init__(self, port, database):
        self.db = FalkorDB(host='localhost', port=port,socket_timeout=10)
        self.graph = self.db.select_graph(database)
    
    def clear(self):
        self.run("MATCH (n) DETACH DELETE n")

    def get_plan(self, query):
        pass
    def get_explain_query_plan(self, query):
        query = query.replace(';', '')
        plan = self.db.execute_command("GRAPH.EXPLAIN", self.graph.name, query)
        plan = [step.decode('utf-8') if isinstance(step, bytes) else step for step in plan]
        return tuple(plan)
    
    def run(self, query):
      query = query.replace(';', '')
      result = self.graph.query(query)
      return result.result_set, result.run_time_ms
    def batch_run(self, queries):
        for i in queries:
            i = i.replace(';', '')
            self.run(i)



