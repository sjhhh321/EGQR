import re
import random

class HintMemgraph:
    def __init__(self):
        self.create_statements = None
    
    def init(self, create_statements):
        self.create_statements = create_statements
    def get_node_create_statements(self):
        node_statements = []
        for single in self.create_statements:
            if "CREATE" in single:
                node_statements.append(single)
        return node_statements
    

    def __parse_clause(self, clause, query):
        pos = query.find(f"{clause} ")
        keywords = ["RETURN ", "OPTIONAL MATCH ", "WHERE ", "CONTAINS ", "WITHIN ",
                    "WITH ", "UNION ", "ALL ", "UNWIND ", "AS ", "MERGE ", "ON ",
                    "CREATE ", "SET ", "DETACH ", "DELETE ", "REMOVE ", "CALL ",
                    "YIELD ", "DISTINCT ", "ORDER ", "BY ", "L_SKIP ", "LIMIT ",
                    "ASCENDING ", "ASC ", "DESCENDING ", "DESC ","COUNT ", "CASE ", "ELSE ", "END ", "WHEN ", "THEN ",
                    "ANY ", "NONE ", "SINGLE ", "EXISTS ", "MATCH "
                    ]
        res = []
        while pos != -1:
            f = lambda x: query.find(x, pos + 1) if query.find(x, pos + 1) > -1 else len(query)
            next_pos = min(f(x) for x in keywords)
            res.append((pos + len(f"{clause} "), next_pos))
            pos = query.find(f"{clause} ", pos + 1)

        return res
    def create_and_delete_node_index(self, query:str):
        match_pos_list = self.__parse_clause("MATCH", query)
        where_pos_list = self.__parse_clause("WHERE", query)
        
        create_and_delete_index_list = []

        for where_pos in sorted(where_pos_list):
            where_content = query[where_pos[0]:where_pos[1]].strip(" ")
            regular_pattern = r'\b\w+\.\w+\b'
            where_result = list(set(re.findall(regular_pattern, where_content)))
            for single in where_result:
                rep = single.split(".")[0]
                property = single.split(".")[1]
                
                for match_pos in sorted(match_pos_list, reverse=True):
                    if match_pos[0] > where_pos[1]:
                        continue
                    match_pattern = query[match_pos[0]:match_pos[1]]
                    regular_pattern_match = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
                    match_result = re.findall(regular_pattern_match, match_pattern)
                    for entity in match_result:
                        
                        n_rep = None
                        n_labels = None
                        if entity[0] == '(':
                            entity = entity.strip(')').strip('(')
                        else:
                            entity = entity.strip('[').strip(']')
                        if ':' not in entity:
                            continue
                        n_rep = entity.split(':')[0].strip(" ")
                        n_labels = entity.split(':')[1:]
                        if len(n_labels) > 1:
                            continue
                        if n_rep == rep:
                            create_index = "CREATE INDEX ON:" + n_labels[0] + '(' + property +')'
                            delete_index = "DROP INDEX ON:" + n_labels[0] + '(' + property +')'
                            create_and_delete_index_list.append((create_index, delete_index))
                    
        
        return create_and_delete_index_list