import csv
from cypher.query_generator import *
import threading
from copy import deepcopy
from database_tests.helper import *
from gdb_clients import *
from configs.conf import *
from hint.neo4j.HintNeo4j import HintNeo4j

import time

def compare(result1, result2):
    if len(result1) != len(result2):
        return False
    lst1 = [i.__str__() for i in result1]
    lst2 = [i.__str__() for i in result2]
    lst1.sort()
    lst2.sort()
    return lst1 == lst2

empty_cnt = 0
# result: is returned by client.run()
def oracle(conf: TestConfig, result1, result2):
    global empty_cnt
    if len(result1[0]) == 0 and len(result2[0]) == 0:
        empty_cnt += 1
    elif 'a1' in result1[0][0] and 'a1' in result2[0][0]:
        if result1[0][0]['a1'] == 0 and result2[0][0]['a1']==0:
            empty_cnt += 1
    if not compare(result1[0], result2[0]):
        if conf.mode == 'live':
            conf.report(conf.report_token,f"[{conf.database_name}][{conf.source_file}]Logic inconsistency",
                        conf.q1 + "\n" + conf.q2)
            conf.logger.warning({
                "database_name": conf.database_name,
                "source_file": conf.source_file,
                "tag": "logic_inconsistency",
                "query1": conf.q1,
                "query2": conf.q2,
                "query_res1": result1[0].__str__() if len(result1[0])<100 else "",
                "query_res2": result2[0].__str__() if len(result2[0])<100 else "",
                "query_time1": result1[1],
                "query_time2": result2[1],
            })
        with open(conf.logic_inconsistency_trace_file, mode='a', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow([conf.database_name, conf.source_file, conf.q1, conf.q2])
    big = max(result1[1], result2[1])
    small = min(result1[1], result2[1])
    heap = MaxHeap("logs/neo4j_performance.json",10)
    metric = big/(small+100)
    if metric > 1:
        heap.insert(metric)
    threshold = heap.get_heap()
    if metric in threshold and conf.q1 != conf.q2:
        if conf.mode == 'live':
            conf.report(conf.report_token,f"[{conf.database_name}][{conf.source_file}][{big}ms,{small}ms]Performance inconsistency",
                        conf.q1 + "\n" + conf.q2)
        conf.logger.warning({
                "database_name": conf.database_name,
                "source_file": conf.source_file,
                "tag": "performance_inconsistency",
                "query1": conf.q1,
                "query2": conf.q2,
                "query_res1": result1[0].__str__() if len(result1[0])<100 else "",
                "query_res2": result2[0].__str__() if len(result2[0])<100 else "",
                "query_time1": result1[1],
                "query_time2": result2[1],
            })

class Neo4jTester(TesterAbs):
    def __init__(self, database):
        temp_conn = Neo4j(config.get('neo4j', 'uri'), config.get('neo4j', 'username'), config.get('neo4j', 'passwd'),
                          '')
        print("Initializing dabtases...")
        result, _ = temp_conn.run("SHOW DATABASES")
        database_names = [record['name'] for record in result]
        if database in database_names:
            print("The database exists...")
        else:
            print("Creating database...")
            temp_conn.run(f"CREATE DATABASE {database}")
        temp_conn = None
        self.connections = {}
        self.database = database

    def get_connection(self):
        thread_id = threading.get_ident()
        if thread_id not in self.connections:
            self.connections[thread_id] = Neo4j(config.get("neo4j", 'uri'), config.get('neo4j', 'username'),
                                                config.get('neo4j', 'passwd'),
                                                self.database)
        return self.connections[thread_id]

    def single_file_testing(self, logfile):
        t = time.time()
        if config.get("neo4j", "generator") != "gdsmith":
            logfile = f"./query_producer/cypher/{t}.log"
        def query_producer():
            print(config.get("neo4j", "generator"))
            if config.get("neo4j", "generator") == "gdsmith":
                print('using gdsmith as generator...')
                with open(logfile, 'r') as f:
                    content = f.read()
                contents = content.strip().split('\n')
                query_statements = contents[-1000:]
                create_statements = contents[4:-1000]
                return create_statements, query_statements
            print('using diy-cypher as generator...')
            generator = QueryGenerator(f"./query_producer/cypher/{t}.log")
            with open(f"./query_producer/cypher/{t}.log", 'r') as f:
                content = f.read()
                f.close()
            match_statements = [generator.gen_query() for i in range(2000)]
            contents = content.strip().split('\n')
            return contents, match_statements
        
        logger = new_logger("logs/neo4j.log", True)
        conf = TestConfig(
            client=Neo4j(config.get("neo4j", 'uri'), config.get('neo4j', 'username'), config.get('neo4j', 'passwd'),
                         self.database),
            logger=logger,
            hintDB= HintNeo4j(),
            source_file=logfile,
            logic_inconsistency_trace_file='logs/neo4j_logic_error.tsv',
            database_name='neo4j',
            query_producer_func=query_producer,
            oracle_func=oracle,
            report_token=config.get('lark','neo4j')
        )
        general_testing_procedure(conf)

        global empty_cnt
        print(f"############### empty {empty_cnt}/3000 ########################")
        return True  

def schedule():
    scheduler(config.get('neo4j', 'input_path'), Neo4jTester("neo4j"), 'neo4j')


if __name__ == "__main__":
    if config.get("GLOBAL", 'env') == "debug":
        print('debug')
        Tester = Neo4jTester('neo4j')
        Tester.single_file_testing("query_producer/logs/composite/database137-cur.log")
    else:
        schedule()
