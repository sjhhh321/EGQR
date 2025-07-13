from database_tests.helper import *
from configs.conf import *
from gdb_clients.agens_graph import *
from hint.agensgraph.HintAgens import *

import csv

def compare(result1, result2):
    
    if result1 is None or result2 is None:
        return True
    if len(result1) != len(result2):
        return False
    lst1 = [str(i) for i in result1]
    lst2 = [str(i) for i in result2]
    lst1.sort()
    lst2.sort()
    return lst1 == lst2

empty_cnt = 0
def oracle(conf: TestConfig, result1, result2):
    global empty_cnt

    
    data1, time1 = result1
    data2, time2 = result2

    
    if result1 is None or result2 is None:
        return
    
    
    if not data1 and not data2:
        empty_cnt += 1
    elif isinstance(data1, list) and isinstance(data2, list):
        if data1 and data2 and isinstance(data1[0], dict) and 'a1' in data1[0] and 'a1' in data2[0]:
            if data1[0]['a1'] == 0 and data2[0]['a1'] == 0:
                empty_cnt += 1

   
    if not compare(data1, data2):
        if conf.mode == 'live':
            conf.report(conf.report_token, f"[{conf.database_name}][{conf.source_file}]Logic inconsistency",
                        conf.q1 + "\n" + conf.q2)
            conf.logger.warning({
                "database_name": conf.database_name,
                "source_file": conf.source_file,
                "tag": "logic_inconsistency",
                "query1": conf.q1,
                "query2": conf.q2,
                "query_res1": str(data1) if len(data1) < 100 else "",
                "query_res2": str(data2) if len(data2) < 100 else "",
                "query_time1": time1,
                "query_time2": time2,
            })
        with open(conf.logic_inconsistency_trace_file, mode='a', newline='') as file:
            writer = csv.writer(file, delimiter='\t')
            writer.writerow([conf.database_name, conf.source_file, conf.q1, conf.q2])

    
    big = max(time1, time2)
    small = min(time1, time2)
    heap = MaxHeap("logs/agensgraph_performance.json", 10)
    metric = big / (small + 100)  
    if metric > 1:
        heap.insert(metric)
    threshold = heap.get_heap()
    if metric in threshold:
        if conf.mode == 'live':
            conf.report(conf.report_token,
                        f"[{conf.database_name}][{conf.source_file}][{big}ms,{small}ms]Performance inconsistency",
                        conf.q1 + "\n" + conf.q2)
        conf.logger.warning({
            "database_name": conf.database_name,
            "source_file": conf.source_file,
            "tag": "performance_inconsistency",
            "query1": conf.q1,
            "query2": conf.q2,
            "query_res1": str(data1) if len(data1) < 100 else "",
            "query_res2": str(data2) if len(data2) < 100 else "",
            "query_time1": time1,
            "query_time2": time2,
        })
    
class AgensTester(TesterAbs):
    def __init__(self, database):

        self.database = database

        pass

    def get_connection(self):
        pass

    def single_file_testing(self, logfile):
        t = time.time()
        if config.get("agens", "generator") != "gdsmith":
            logfile = f"./query_producer/cypher/{t}.log"
        def query_producer():
            print(config.get("agens", "generator"))
            if config.get("agens", "generator") == "gdsmith":
                print('using gdsmith as generator...')
                with open(logfile, 'r') as f:
                    content = f.read()
                contents = content.strip().split('\n')
                query_statements = contents[-1000:]
                create_statements = contents[4:-1000]
                return create_statements, query_statements

        logger = new_logger("logs/agens.log", True)
        conf = TestConfig(
            client=AgensGraph(config.get("agens", 'uri'), config.get('agens', 'username'), config.get('agens', 'passwd'),
                        self.database),
            logger=logger,
            hintDB= HintAgens(),
            source_file=logfile,
            logic_inconsistency_trace_file='logs/agens_logic_error.tsv',
            database_name='agens',
            query_producer_func=query_producer,
            oracle_func=oracle,
            report_token=config.get('lark','agens')
        )
        general_testing_procedure(conf)

        global empty_cnt
        print(f"############### empty {empty_cnt}/3000 ########################")
        return True  

def schedule():
    scheduler(config.get("agens", 'input_path'), AgensTester("agens"), "agens")

if __name__ == "__main__":
    schedule()
