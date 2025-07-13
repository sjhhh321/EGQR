import csv
import os
from statistic.query_analyze import QueryAnalyze
from abc import ABC
from tqdm import tqdm
from tinydb import TinyDB, Query
from logging import Logger
from gdb_clients import GdbFactory
from mutator.query_transformer import QueryTransformer
from sklearn.metrics.pairwise import cosine_similarity
from sentence_transformers import SentenceTransformer, util
from sklearn.cluster import KMeans
from webhook.lark import post
from abc import ABC, abstractmethod
from typing import Callable,List
from configs import config
import redis
import traceback
import time
import json
import random


def diversify_queries_auto(queries, model):
    
    embeddings = model.encode(queries, convert_to_tensor=True)

    n = len(queries)

    
    if n <= 50:
        selected_indices = [0]
        remaining_indices = set(range(1, n))

        while remaining_indices:
            last_idx = selected_indices[-1]
            max_dist = -1
            next_idx = None

            for idx in remaining_indices:
                sim = util.pytorch_cos_sim(embeddings[last_idx], embeddings[idx]).item()
                dist = 1 - sim
                if dist > max_dist:
                    max_dist = dist
                    next_idx = idx

            selected_indices.append(next_idx)
            remaining_indices.remove(next_idx)

        return [queries[i] for i in selected_indices]

    
    else:
        embeddings_np = embeddings.cpu().numpy()
        num_clusters = max(10, int(n ** 0.5))
        kmeans = KMeans(n_clusters=num_clusters, random_state=42, n_init='auto')
        labels = kmeans.fit_predict(embeddings_np)

        
        cluster_to_queries = {i: [] for i in range(num_clusters)}
        for query, label in zip(queries, labels):
            cluster_to_queries[label].append(query)

        for qlist in cluster_to_queries.values():
            random.shuffle(qlist)

        diversified = []
        while any(cluster_to_queries.values()):
            for i in range(num_clusters):
                if cluster_to_queries[i]:
                    diversified.append(cluster_to_queries[i].pop())

        return diversified
    
class MaxHeap:
    def __init__(self, db_path, max_size):
        self.db = TinyDB(db_path)
        self.table_name = 'max_heap'
        self.table = self.db.table(self.table_name)
        self.max_size = max_size
       

    def insert(self, value):
        if len(self.table) >= self.max_size:
            min_value = min(self.table, key=lambda x: x['value'])
            if value > min_value['value']:
                self.table.remove(doc_ids=[min_value.doc_id])
                self.table.insert({'value': value})
        else:
            self.table.insert({'value': value})

    def get_heap(self):
        return [x['value'] for x in self.table.all()]


class TestConfig:
    def __init__(self, **kwargs):
        self.mode = kwargs.get('mode', 'live')
        self.report = kwargs.get('report', post)
        self.report_token = kwargs.get('report_token')
        self.transform_times = kwargs.get('transform_times', 15)
        self.hint_times = kwargs.get("hint_times", 5)

        self.client: GdbFactory = kwargs.get('client')
        self.logger: Logger = kwargs.get('logger')
        self.source_file = kwargs.get('source_file')
        self.logic_inconsistency_trace_file = kwargs.get('logic_inconsistency_trace_file')
        self.database_name = kwargs.get('database_name')
        
        self.mutator_func: Callable[[str], str] = kwargs.get('mutator_func', QueryTransformer().mutant_query_generator)
        self.mutator_func3: Callable[[str], str] = kwargs.get('mutator_func', QueryTransformer().mutant_query_generator_with_enum)
        self.EGE: Callable[[str], List[str]] = kwargs.get('mutator_func', QueryTransformer().EGE)
        self.query_producer_func = kwargs.get('query_producer_func', lambda: ([], []))
        self.oracle_func: Callable[[TestConfig, any, any], None] = kwargs.get("oracle_func")
        
        self.algorithm = "EGE"
        
        self.hintDB =  kwargs.get('hintDB')
        # temp val for consistency checker
        self.q1 = None
        self.q2 = None

        self.threshold = 9
        self.num_bug_triggering = 0
        self.num_logic = 0
        self.num_performance = 0


class TesterAbs(ABC):
    @abstractmethod
    def single_file_testing(self, path):
        pass


def batch_run_with_macro(conf: TestConfig, statements):
    pre_idx = 0
    for i, v in enumerate(statements):
        if v == 'SLEEP':
            conf.client.batch_run(statements[pre_idx:i])
            pre_idx = i + 1
            time.sleep(7)
        else:
            continue
    if pre_idx < len(statements):
        conf.client.batch_run(statements[pre_idx:len(statements)])


def general_testing_procedure(conf: TestConfig):
    create_statements, match_statements = conf.query_producer_func()
    
    conf.client.clear()
    hintDB = conf.hintDB
    dbType = conf.database_name
    # hintDB.init(create_statements)
    # if dbType == "neo4j":
    #     conf.client.clear_index()
    #     node_index_statements = hintDB.create_node_index()
    #     edge_index_statements = hintDB.create_edge_index()

    #     batch_run_with_macro(conf, node_index_statements)
    #     batch_run_with_macro(conf, edge_index_statements)
    batch_run_with_macro(conf, create_statements)
    
    
    progress_bar = tqdm(total=len(match_statements))

    ex_time = 1
    
    query_analyze = QueryAnalyze(conf.source_file, conf.algorithm,conf.database_name)
    model = SentenceTransformer('paraphrase-MiniLM-L6-v2')
    for query in match_statements:
        try:
            if isinstance(query, dict):
                result1 = conf.client.run(query['Query1'])
                result2 = conf.client.run(query['Query2'])
                conf.q1 = query['Query1']
                conf.q2 = query['Query2']
                conf.oracle_func(conf, result1, result2)
            elif isinstance(query, tuple):
                result1 = conf.client.run(query[0])
                result2 = conf.client.run(query[1])
                conf.q1 = query[0]
                conf.q2 = query[1]
                conf.oracle_func(conf, result1, result2)
            else:
                result1 = conf.client.run(query)
                query_plan1 = conf.client.get_explain_query_plan(query)
                query_analyze.add_query(query)
                query_analyze.add_query_plan(query_plan1)
                new_query_list = conf.EGE(query, conf.threshold, conf.transform_times)
                new_query_list = diversify_queries_auto(new_query_list, model)
                #random.shuffle(new_query_list)

                for i in range(min(conf.transform_times, len(new_query_list))):
                    new_query = new_query_list[i % len(new_query_list)]
                    result2 = None
                    query_plan2 = None
                    if dbType == "neo4j":
                        choose = random.random()
                        # choose = 0.5
                        if choose < 0.8:
                            result = hintDB.select_rules(new_query)
                            if result[2] == True:
                                new_query = result[1]
                                if result[3] != None:
                                    conf.client.run(result[3][0])
                                result2 = conf.client.run(new_query)
                                query_plan2 = conf.client.get_explain_query_plan(new_query)
                                if result[3] != None:
                                    conf.client.run(result[3][1])
                                if query_plan1 != query_plan2:
                                    hintDB.feedback(result[0], "active")
                                else:
                                    hintDB.feedback(result[0], "negative")
                            else:
                                result2 = conf.client.run(new_query)
                                query_plan2 = conf.client.get_explain_query_plan(new_query)
                                hintDB.feedback(result[0], "negative")
                        else:
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)
                        #print(query_plan1)
                        #print(query_plan2)
                        query_analyze.add_query(new_query)
                        query_analyze.add_query_plan(query_plan2)
                        print(query_plan1 == query_plan2)
                        conf.q1 = query
                        conf.q2 = new_query
                        conf.oracle_func(conf, result1, result2) 
                    elif dbType == "redis":
                        choose = random.random()
                        create_index = None
                        delete_index = None
                        if choose < 0.35:
                            create_delete_index = hintDB.create_and_delete_node_index(new_query)
                            create_index, delete_index = random.choice(create_delete_index)
                            conf.client.run(create_index)
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)
                            conf.client.run(delete_index)
                        else:
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)

                        query_analyze.add_query(new_query)
                        query_analyze.add_query_plan(query_plan2)
                        print(query_plan1 == query_plan2)
                        conf.q1 = query
                        conf.q2 = new_query
                        if create_index != None:
                            conf.q2 +=' (' + create_index + ')'
                        conf.oracle_func(conf, result1, result2) 
       
                    elif dbType == "memgraph":
                        choose = random.random()
                        create_index = None
                        delete_index = None
                        if choose < 0.35:
                            create_delete_index = hintDB.create_and_delete_node_index(new_query)
                            is_index = True
                            if len(create_delete_index) == 0:
                                is_index = False
                            if is_index:
                                create_index, delete_index = random.choice(create_delete_index)
                                conf.client.run(create_index)
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)
                            if is_index:
                                conf.client.run(delete_index)
                        else:
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)

                        query_analyze.add_query(new_query)
                        query_analyze.add_query_plan(query_plan2)
                        print(query_plan1 == query_plan2)
                        conf.q1 = query
                        conf.q2 = new_query
                        if create_index != None :
                            conf.q2 += ' (' + create_index + ')'
                        conf.oracle_func(conf, result1, result2)  
                    elif dbType == "agens":
                        choose = 0.5
                        create_index = None
                        delete_index = None
                        if choose < 0.35:
                            create_delete_index = hintDB.create_and_delete_node_index(new_query)
                            is_index = True
                            if len(create_delete_index) == 0:
                                is_index = False
                            if is_index:
                                create_index, delete_index = random.choice(create_delete_index)
                                conf.client.run(create_index)
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)
                            if is_index:
                                conf.client.run(delete_index)
                        else:
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)

                        query_analyze.add_query(new_query)
                        query_analyze.add_query_plan(query_plan2)
                        print(query_plan1 == query_plan2)
                        conf.q1 = query
                        conf.q2 = new_query
                        if create_index != None :
                            conf.q2 += ' (' + create_index + ')'
                        conf.oracle_func(conf, result1, result2)   
                    elif dbType == "falkordb":
                        choose = random.random()
                        create_index = None
                        delete_index = None
                        if choose < 0.35:
                            create_delete_index = hintDB.create_and_delete_node_index(new_query)
                            if len(create_delete_index) == 0:
                                continue
                            create_index, delete_index = random.choice(create_delete_index)
                            conf.client.run(create_index)
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)
                            conf.client.run(delete_index)
                        else:
                            result2 = conf.client.run(new_query)
                            query_plan2 = conf.client.get_explain_query_plan(new_query)

                        query_analyze.add_query(new_query)
                        query_analyze.add_query_plan(query_plan2)
                        print(query_plan1 == query_plan2)
                        conf.q1 = query
                        conf.q2 = new_query
                        if create_index != None:
                            conf.q2 +=' (' + create_index + ')'
                        conf.oracle_func(conf, result1, result2)  

                    #     for j in range(conf.hint_times):
                    #         hint_query = hintDB.random_select_rules(new_query)
                    #         query_analyze.add_query(hint_query)
                    #         result3 = conf.client.run(hint_query)
                    #         query_plan3 = conf.client.get_explain_query_plan(hint_query)
                    #         query_analyze.add_query_plan(query_plan3)
                    #         print(query_plan2 == query_plan3)
                    #         conf.q1 = query
                    #         conf.q2 = hint_query
                    #         conf.oracle_func(conf, result1, result3)  
                    # elif dbType == "redis":
                    #     create_delete_index = hintDB.create_and_delete_node_index(new_query)
                    #     random.shuffle(create_delete_index)
                    #     for j in range(min(conf.hint_times, len(create_delete_index))):
                    #         create_index,delete_index = create_delete_index[j]
                    #         conf.client.run(create_index)
                    #         result3 = conf.client.run(new_query)
                    #         query_plan3 = conf.client.get_explain_query_plan(new_query)
                    #         conf.client.run(delete_index)
                    #         query_analyze.add_query_plan(query_plan3)
                    #         print(query_plan2 == query_plan3)
                    #         conf.q1 = query
                    #         conf.q2 = new_query + '(' + create_index + ')'
                    #         conf.oracle_func(conf, result1, result3) 
                    # elif dbType == "memgraph":
                    #     create_delete_index = hintDB.create_and_delete_node_index(new_query)
                    #     random.shuffle(create_delete_index)
                    #     for j in range(min(conf.hint_times, len(create_delete_index))):
                    #         create_index,delete_index = create_delete_index[j]
                    #         conf.client.run(create_index)
                    #         result3 = conf.client.run(new_query)
                    #         query_plan3 = conf.client.get_explain_query_plan(new_query)
                    #         conf.client.run(delete_index)
                    #         query_analyze.add_query_plan(query_plan3)
                    #         print(query_plan2 == query_plan3)
                    #         conf.q1 = query
                    #         conf.q2 = new_query + '(' +  create_index + ')'
                    #         conf.oracle_func(conf, result1, result3) 
                    # elif dbType == "agens":
                    #     create_delete_index = hintDB.create_and_delete_node_index(new_query)
                    #     random.shuffle(create_delete_index)
                    #     for j in range(min(conf.hint_times, len(create_delete_index))):
                    #         create_index,delete_index = create_delete_index[j]
                    #         conf.client.run(create_index)
                    #         result3 = conf.client.run(new_query)
                    #         query_plan3 = conf.client.get_explain_query_plan(new_query)
                    #         conf.client.run(delete_index)
                    #         query_analyze.add_query_plan(query_plan3)
                    #         print(query_plan2 == query_plan3)
                    #         conf.q1 = query
                    #         conf.q2 = new_query + '(' +  create_index + ')'
                    #         conf.oracle_func(conf, result1, result3) 
                        
                # for _ in range(0, conf.transform_times):
                    
                #     # new_query = conf.mutator_func(query)
                #     # new_query = conf.mutator_func2(query, llm)
                #     # new_query = hintDB.rewrite_query_with_join_hint(query)
                #     # new_query = conf.mutator_func3(query)
                #     new_query_list = conf.TEGE_RWC(query, conf.threshold, conf.transform_times)
                #     new_query = random.choice(new_query_list)
                #     print(new_query)
                #     query_analyze.add_query(new_query)
                #     result2 = conf.client.run(new_query)
                #     query_plan1 = conf.client.get_explain_query_plan(query)
                #     query_plan2 = conf.client.get_explain_query_plan(new_query)
                #     #print(query_plan1)
                #     query_analyze.add_query_plan(query_plan1)
                #     query_analyze.add_query_plan(query_plan2)
                #     print(query_plan1 == query_plan2)
                #     # print(type(query_plan1))
                #     # structure1 = conf.client.extract_plan_structure(query_plan1)
                #     # structure2 = conf.client.extract_plan_structure(query_plan2)
                #     #print(query_plan1)
                #     #print(query_plan2)
                #     #print("Query 1 Plan:", json.dumps(query_plan1, indent=2))
                #     #print("Query 2 Plan:", json.dumps(query_plan2, indent=2))
                #     # max_len = min(len(query_plan1), len(query_plan2))
                #     # for i in range(max_len):
                #     #     if query_plan1[i] != query_plan2[i]:
                #     #         print(query_plan1[:i])
                #     #         print(query_plan2[:i])
                #     #         break
                #     # print(query_plan1 == query_plan2)
                #     conf.q1 = query
                #     conf.q2 = new_query
                #     conf.oracle_func(conf, result1, result2)
        # except redis.exceptions.ResponseError as e:
            # tb_str = traceback.format_tb(e.__traceback__)
            # conf.logger.info({
            #     "database_name": conf.database_name,
            #     "source_file": conf.source_file,
            #     "tag": "exception",
            #     "exception_content": e.__str__(),
            #     "query1": conf.q1,
            #     "query2": conf.q2,
            #     # "traceback": tb_str
            # })
        except ValueError as e:
            print(e.__str__())
            tb_str = traceback.format_tb(e.__traceback__)
            conf.logger.info({
                "database_name": conf.database_name,
                "source_file": conf.source_file,
                "tag": "exception",
                "exception_content": e.__str__(),
                "query1": conf.q1,
                "query2": conf.q2,
                # "traceback": tb_str
            })
        except redis.exceptions.ConnectionError as e:
            # tb_str = traceback.format_tb(e.__traceback__)
            # conf.logger.info({
            #     "database_name": conf.database_name,
            #     "source_file": conf.source_file,
            #     "tag": "exception",
            #     "exception_content": e.__str__(),
            #     "query1": conf.q1,
            #     "query2": conf.q2,
            #     # "traceback": tb_str
            # })
            # time.sleep(ex_time)
            ex_time+=1
        except Exception as e:
            print(e)
            tb_str = traceback.format_tb(e.__traceback__)
            conf.logger.info({
                "database_name": conf.database_name,
                "source_file": conf.source_file,
                "tag": "exception",
                "exception_content": e.__str__(),
                "query1": conf.q1,
                "query2": conf.q2,
                "traceback": tb_str
            })
            if conf.mode == 'live':
                conf.report(conf.report_token, f"[{conf.database_name}][{conf.source_file}]",
                            f"exception: \n{e} \nquery:\n{query}")
        progress_bar.update(1)
    query_analyze.save("query_plan_different_rate.txt", "a")

    time.sleep(1.5)


def scheduler(folder_path, tester: TesterAbs, database):
    file_paths = []
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for file in filenames:
            if 'cur.log' in file:
                file_path = os.path.join(dirpath, file)
                file_paths.append(file_path)

    sorted_file_paths = sorted(file_paths)

    for file_path in sorted_file_paths:
        db = TinyDB('db.json')
        table = db.table(database)
        session = Query()
        res = table.search(session.FileName == file_path)
        if not res:
            table.insert({'FileName': file_path, 'status': 'doing'})
            success = tester.single_file_testing(file_path)
            if success:
                table.update({'status': 'done'}, session.FileName == file_path)
            else:
                table.remove(session.FileName == file_path)


def gremlin_scheduler(folder_path, tester: TesterAbs, database):
    for i in range(100):
        file_path = os.path.join(folder_path, f'create-{i}.log')
        db = TinyDB('db.json')
        table = db.table(database)
        session = Query()
        res = table.search(session.FileName == file_path)
        if not res:
            table.insert({'FileName': file_path, 'status': 'doing'})
            success = tester.single_file_testing(file_path)
            if success:
                table.update({'status': 'done'}, session.FileName == file_path)
            else:
                table.remove(session.FileName == file_path)


