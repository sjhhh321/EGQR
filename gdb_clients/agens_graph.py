import psycopg2
import agensgraph
from gdb_clients import GdbFactory
from typing import List
import re
import time

class AgensGraph(GdbFactory):
    def __init__(self, uri, username, passwd, database):
        print("Connecting to AgensGraph...")
        self.conn = psycopg2.connect(
            dbname=database,
            user=username,
            password=passwd,
            host=uri.split(":")[0],
            port=uri.split(":")[1]
        )
        self.cur = self.conn.cursor()
        self.graph_path = "g"  # default graph path
        self._init_graph()
        
    def _init_graph(self):
        """
        set graph_path
        """
        self.cur.execute(f"DROP GRAPH IF EXISTS {self.graph_path} CASCADE")
        self.cur.execute(f"CREATE GRAPH {self.graph_path}")
        self.cur.execute(f"SET graph_path = {self.graph_path}")
        self.conn.commit()

    def run(self, query):

        try:
            q = self.replace_string_quotes(query)
            start = time.time()
            self.cur.execute(q)
            end = time.time()
            elapsed = int((end - start) * 1000)
            try:
                result = self.cur.fetchall()
            except psycopg2.ProgrammingError:
                result = None  
            self.conn.commit()
            return result, elapsed  
        except Exception as e:
            print(f"AgensGraph query error: {e}")
            self.conn.rollback()
            return None, None
    
    def clean_plan(self, plan: List[str]):
        cleaned = []
        for line in plan:

            line = re.sub(r'\(cost=.*?\)', '', line)
            line = re.sub(r'\(rows=.*?\)', '', line)
            line = re.sub(r'\(width=.*?\)', '', line)
 
            cleaned.append(line.strip())
        return "".join(cleaned)
    
    def get_explain_query_plan(self, query):
        try:
            q = self.replace_string_quotes(query)
            self.cur.execute(f"EXPLAIN {q}")
            plan_raw = self.cur.fetchall()
            plan = [row[0] for row in plan_raw]
            cleaned_plan = self.clean_plan(plan)
            return cleaned_plan
        except Exception as e:
            print(f"AgensGraph query error: {e}")
            self.conn.rollback()

        

    def replace_string_quotes(self, query: str) -> str:

        return re.sub(r'"\s*([^"]*?)\s*"', r"'\1'", query)
    def batch_run(self, query:List[str]):

        self.clear()
        for q in query:
            try:
                q = self.replace_string_quotes(q)
                self.cur.execute(q)
            except Exception as e:
                print(f"Batch run error: {e}")
                self.conn.rollback()
        self.conn.commit()

    def clear(self):

        self.run("MATCH (n) DETACH DELETE n")
        self.conn.commit()

    




