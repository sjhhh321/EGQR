import re
import random


def is_literal(val: str) -> bool:
    
    if val is None:
        return False
    val = val.strip()
    return (
        re.match(r'^-?\d+(\.\d+)?$', val) or    # 数字
        re.match(r"^'.*'$", val) or             # 单引号字符串
        re.match(r'^".*"$', val)                # 双引号字符串
    )
def classify_index_usage(where_clause: str) -> dict:

    results = {}

    clause = re.sub(r'\(+', '(', where_clause)
    clause = re.sub(r'\)+', ')', clause)

    pattern_forward = re.compile(
        r'\(?\s*(\w+\.\w+)\s*\)?\s*(STARTS\s+WITH|IS\s+NOT\s+NULL|>=|<=|=|<|>|IN)\s*\(?\s*([^\(\)]+?)?\s*\)?(?=\s|$)',
        re.IGNORECASE
    )

    pattern_reverse = re.compile(
        r'\(?\s*([^\(\)\s]+)\s*\)?\s*(>=|<=|=|<|>)\s*\(?\s*(\w+\.\w+)\s*\)?(?=\s|$)',
        re.IGNORECASE
    )


    for match in pattern_forward.finditer(clause):
        prop, op, rhs = match.groups()
        op = op.upper().strip()

        if op == '=' and is_literal(rhs):
            results[prop] = 'equality'
        elif op == 'IN' and rhs and rhs.strip().startswith('['):
            results[prop] = 'in'
        elif op == 'IS NOT NULL':
            results[prop] = 'not_null'
        elif op in ('<', '>', '<=', '>=') and is_literal(rhs):
            results[prop] = 'range'
        elif op == 'STARTS WITH' and is_literal(rhs):
            results[prop] = 'starts_with'
        else:
            results[prop] = 'not_used'


    for match in pattern_reverse.finditer(clause):
        lhs, op, prop = match.groups()
        op = op.upper().strip()

        if op == '=' and is_literal(lhs):
            results[prop] = 'equality'
        elif op in ('<', '>', '<=', '>=') and is_literal(lhs):
            results[prop] = 'range'
        else:
            results[prop] = 'not_used'

    return results

def is_string_literal(s: str) -> bool:
    if s is None:
        return False
    s = s.strip()
    return bool(re.fullmatch(r"'[^']*'|\"[^\"]*\"", s))

def classify_text_index_usage(where_clause: str) -> dict:
    results = {}

    clause = re.sub(r'\(+', '(', where_clause)
    clause = re.sub(r'\)+', ')', clause)

    pattern_forward = re.compile(
        r'\(?\s*(\w+\.\w+)\s*\)?\s*(STARTS\s+WITH|ENDS\s+WITH|CONTAINS|IS\s+NOT\s+NULL|>=|<=|=|<|>|IN)\s*\(?\s*([^\(\)]+?)?\s*\)?(?=\s|$)',
        re.IGNORECASE
    )

    pattern_reverse = re.compile(
        r'\(?\s*([^\(\)\s]+)\s*\)?\s*(>=|<=|=|<|>)\s*\(?\s*(\w+\.\w+)\s*\)?(?=\s|$)',
        re.IGNORECASE
    )

    for match in pattern_forward.finditer(clause):
        prop, op, rhs = match.groups()
        op = op.upper().strip()

        if op == '=' and is_string_literal(rhs):
            results[prop] = 'equality'
        elif op == 'IN' and rhs and rhs.strip().startswith('['):
            results[prop] = 'in'
        # elif op == 'IS NOT NULL':
        #     results[prop] = 'not_null'
        # elif op in ('<', '>', '<=', '>=') and is_literal(rhs):
        #     results[prop] = 'range'
        elif op == 'STARTS WITH' and is_string_literal(rhs):
            results[prop] = 'starts_with'
        elif op == 'ENDS WITH' and is_string_literal(rhs):
            results[prop] = 'ends_with'
        elif op == 'CONTAINS' and is_string_literal(rhs):
            results[prop] = 'contains'
        else:
            results[prop] = 'not_used'

    for match in pattern_reverse.finditer(clause):
        lhs, op, prop = match.groups()
        op = op.upper().strip()

        if op == '=' and is_string_literal(lhs):
            results[prop] = 'equality'
        # elif op in ('<', '>', '<=', '>=') and is_literal(lhs):
        #     results[prop] = 'range'
        else:
            results[prop] = 'not_used'

    return results

class HintNeo4j:
    def __init__(self):
        self.create_statements = None
        self.node_index_id = 0
        self.edge_index_id = 0

        self.exist_index = {} 
        self.feedback_factor = 0.0001
        self.hint_rules = [
            self.query_using_node_index_hint,
            self.query_using_relation_index_hint,
            self.query_using_node_text_index_hint,
            self.query_using_relation_text_index_hint,
            self.query_using_node_scan_hint,
            self.query_using_relation_scan_hint,
            self.query_using_join_single_node_hint

        ]
        self.now_decode=None
        self.hints_table_pool = {}
        self.hints_to_processer = {"query_using_node_index_hint":self.query_using_node_index_hint,
                                   "query_using_relation_index_hint":self.query_using_relation_index_hint,
                                   "query_using_node_text_index_hint":self.query_using_node_text_index_hint,
                                   "query_using_relation_text_index_hint":self.query_using_relation_text_index_hint,
                                   "query_using_node_scan_hint":self.query_using_node_scan_hint,
                                   "query_using_relation_scan_hint":self.query_using_relation_scan_hint,
                                   "query_using_join_single_node_hint":self.query_using_join_single_node_hint}
        self.hints_table = {"query_using_node_index_hint":0.17,
                            "query_using_relation_index_hint":0.15,
                            "query_using_node_text_index_hint":0.05,
                            "query_using_relation_text_index_hint": 0.05,
                            "query_using_node_scan_hint": 0.18,
                            "query_using_relation_scan_hint": 0.20,
                            "query_using_join_single_node_hint":0.20
                            }


    def init(self, create_statements):
        self.create_statements = create_statements
        
    def feedback(self, hint_rule, type):
        hints_table = self.hints_table_pool[self.now_decode]
        processer = self.hints_to_processer[hint_rule]
        if type == "active":
            hints_table[processer] += self.feedback_factor
            negative = self.feedback_factor / (len(hints_table) - 1)
            for key in hints_table.keys():
                if key != processer:
                    hints_table[key] -= negative
        else:
            hints_table[processer] -= self.feedback_factor
            active = self.feedback_factor / (len(hints_table) - 1)
            for key in hints_table.keys():
                if key != processer:
                    hints_table[key] += active  
        self.hints_table_pool[self.now_decode] = hints_table     

    def get_node_create_statements(self):
        node_statements = []
        for single in self.create_statements:
            if "CREATE" in single:
                node_statements.append(single)
        return node_statements


    def get_edge_create_statements(self):
        edge_statements = []
        for single in self.create_statements:
            if "MERGE" in single:
                edge_statements.append(single)
        return edge_statements
    def is_integer(self, s):
        try:
            int(s)
            return True
        except ValueError:
            return False
        
    def create_node_index(self):
        node_create_statements = self.get_node_create_statements()
        label2property = {}

        for single in node_create_statements:
            labels = single.split('(')[1].split('{')[0]
            properties = single.split('(')[1].split('{')[1]

            if ':' not in labels:
                continue

            labels_list = labels.split(':')[1:]
            labels_list = tuple(labels_list)
            properties = properties.strip(';').strip(')').strip('}')
            
            properties_list = []
            for property in properties.split(','):
                property_name = property.split(':')[0]
                property_type = None
                type_str = property.split(':')[1].strip(" ")
                if type_str == "true" or type_str == "false":
                    property_type = "bool"
                elif self.is_integer(type_str):
                    property_type = "number"
                else:
                    property_type = "string"
                properties_list.append({property_name:property_type})
            if labels_list in label2property:

                new_properties = label2property[labels_list]
                for property in properties_list:
                    if property not in new_properties:
                        new_properties.append(property)
                
                label2property[labels_list] = new_properties
                    
            else:

                label2property[labels_list] = properties_list
        

        create_node_index_statements = []
        for key, value in label2property.items():
            for single in value:
                property_name, property_type = next(iter(single.items()))
                property_name = property_name.strip(" ")
                create_statement = ""
                if property_type == "string":
                    for label_name in key:
                        create_statement = "CREATE TEXT INDEX node_index_" + str(self.node_index_id) + " IF NOT EXISTS FOR (n"
                        create_statement += ':' + label_name
                        create_statement += ") ON (n." + property_name + ')'
                        self.node_index_id += 1
                        unique_rep = "node_" + label_name + "_" + property_name
                        self.exist_index[unique_rep] = 2
                        create_node_index_statements.append(create_statement)
                        
                else:

                    
                    for label_name in key:
                        create_statement = "CREATE INDEX node_index_" + str(self.node_index_id) + " IF NOT EXISTS FOR (n"
                        create_statement += ':' + label_name
                        create_statement += ") ON (n." + property_name + ')'
                        self.node_index_id += 1
                        unique_rep = "node_" + label_name + "_" + property_name
                        self.exist_index[unique_rep] = 1
                        create_node_index_statements.append(create_statement)
        
        return create_node_index_statements



    def create_edge_index(self):
        edge_create_statements = self.get_edge_create_statements()
        label2property = {}

        for single in edge_create_statements:
            labels = single.split('[')[1].split('{')[0]
            properties = single.split('[')[1].split('{')[1].split('}')[0]

            if ':' not in labels:
                continue

            labels_list = labels.split(':')[1:]
            labels_list = tuple(labels_list)
            properties = properties.strip(' ')
            
            properties_list = []
            for property in properties.split(','):
                property_name = property.split(':')[0]
                property_type = None
                type_str = property.split(':')[1].strip(" ")
                if type_str == "true" or type_str == "false":
                    property_type = "bool"
                elif type_str.isdigit():
                    property_type = "number"
                else:
                    property_type = "string"
                properties_list.append({property_name:property_type})
            if labels_list in label2property:

                new_properties = label2property[labels_list]
                for property in properties_list:
                    if property not in new_properties:
                        new_properties.append(property)
                
                label2property[labels_list] = new_properties
                    
            else:

                label2property[labels_list] = properties_list
        

        create_edge_index_statements = []
        for key, value in label2property.items():
            for single in value:
                property_name, property_type = next(iter(single.items()))
                property_name = property_name.strip(" ")
                create_statement = ""
                if property_type == "string":
                    
                    for label_name in key:
                        create_statement = "CREATE TEXT INDEX edge_index_" + str(self.edge_index_id) + " IF NOT EXISTS FOR ()-[r"
                        create_statement += ':' + label_name
                        create_statement += "]-() ON (r." + property_name + ')'
                        self.edge_index_id += 1
                        unique_rep = "edge_" + label_name + "_" + property_name
                        self.exist_index[unique_rep] = 2
                        create_edge_index_statements.append(create_statement)
                else:

                    
                    for label_name in key:
                        create_statement = "CREATE INDEX edge_index_" + str(self.edge_index_id) + " IF NOT EXISTS FOR ()-[r"
                        create_statement += ':' + label_name
                        create_statement += "]-() ON (r." + property_name + ')'
                        self.edge_index_id += 1
                        unique_rep = "edge_" + label_name + "_" + property_name
                        self.exist_index[unique_rep] = 1
                        create_edge_index_statements.append(create_statement)
        
        return create_edge_index_statements
    


    def __parse_clause(self, clause, query):
        pos = query.find(f"{clause} ")
        keywords = ["RETURN ", "OPTIONAL MATCH ", "WHERE ", "CONTAINS ", "WITHIN ",
                    "WITH ", "UNION ", "ALL ", "UNWIND ", "AS ", "MERGE ", "ON ",
                    "CREATE ", "SET ", "DETACH ", "DELETE ", "REMOVE ", "CALL ",
                    "YIELD ", "DISTINCT ", "ORDER ", "BY ", "L_SKIP ", "LIMIT ",
                    "ASCENDING ", "ASC ", "DESCENDING ", "DESC ", "COUNT ", "CASE ", "ELSE ", "END ", "WHEN ", "THEN ",
                    "ANY ", "NONE ", "SINGLE ", "EXISTS ", "MATCH "
                    ]
        res = []
        while pos != -1:
            f = lambda x: query.find(x, pos + 1) if query.find(x, pos + 1) > -1 else len(query)
            next_pos = min(f(x) for x in keywords)
            res.append((pos + len(f"{clause} "), next_pos))
            pos = query.find(f"{clause} ", pos + 1)

        return res

    def __parse_clause_for_where(self, clause, query):
        pos = query.find(f"{clause} ")
        keywords = ["RETURN ", "OPTIONAL MATCH ", "WHERE ", "CONTAINS ", "WITHIN ",
                    "UNION ", "ALL ", "UNWIND ", "AS ", "MERGE ", "ON ",
                    "CREATE ", "SET ", "DETACH ", "DELETE ", "REMOVE ", "CALL ",
                    "YIELD ", "DISTINCT ", "ORDER ", "BY ", "L_SKIP ", "LIMIT ",
                    "ASCENDING ", "ASC ", "DESCENDING ", "DESC ", "COUNT ", "CASE ", "ELSE ", "END ", "WHEN ", "THEN ",
                    "ANY ", "NONE ", "SINGLE ", "EXISTS ", "MATCH "
                    ]
        res = []
        while pos != -1:
            f = lambda x: query.find(x, pos + 1) if query.find(x, pos + 1) > -1 else len(query)
            next_pos = min(f(x) for x in keywords)
            res.append((pos + len(f"{clause} "), next_pos))
            pos = query.find(f"{clause} ", pos + 1)

        return res

    def is_property_independent(code: str, prop: str):

        prop_re = re.escape(prop)


        for match in re.finditer(rf'\(?\b{prop_re}\b\)?', code):
            start, end = match.span()


            before = code[max(0, start - 3):start].strip()
            after = code[end:end + 3].strip()


            if re.match(r'^(=|<|>|!|[\+\-\*/])', after) or re.match(r'(=|<|>|!|[\+\-\*/])$', before):
                return False  

        return True  

# --------------------------------------------------------------
    def query_using_node_index_hint(self, query):
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)
        where_pos_list = self.__parse_clause_for_where("WHERE", query)
        is_success = False
        for match_pos in sorted(match_pos_list):
            match_pattern = query[match_pos[0]:match_pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            rep_to_labels = {}    
            for single in result:
                rep = None
                labels = None
                if single[0] != '(':
                    continue
                if ':' not in single:
                    continue
                single = single.strip('(').strip(')')
                rep = single.split(':')[0].strip(" ")
                labels = single.split(':')[1:]
                rep_to_labels[rep] = labels
            for where_pos in sorted(where_pos_list):
                if match_pos[0] >= where_pos[1]:
                    continue
                if  where_pos[0] - match_pos[1] >= 7:
                    continue
                where_content = query[where_pos[0]:where_pos[1]].strip(" ")
                regular_pattern = r'\b\w+\.\w+\b'

                where_result = list(set(re.findall(regular_pattern, where_content)))    
                for e in where_result:
                    is_property_indepent = True

                    code = where_content
                    prop = e
                    # prop_re = re.escape(prop)
                    # for match in re.finditer(rf'\(?\b{prop_re}\b\)?', code):
                    #     start, end = match.span()

                    #     
                    #     before = code[max(0, start - 3):start].strip()
                    #     after = code[end:end + 3].strip()

                    #     
                    #     if re.match(r'^(=|<|>|!|[\+\-\*/])', after) or re.match(r'(=|<|>|!|[\+\-\*/])$', before):
                    #         is_property_indepent = False

                    # if is_property_indepent == True:
                    #     continue   
                    is_prop_index = classify_index_usage(code)
                    if prop not in is_prop_index or is_prop_index[prop] == "not_used":
                        continue
                    now_rep = e.split('.')[0].strip(" ")
                    now_property = e.split('.')[1].strip(" ")
                    if now_rep not in rep_to_labels:
                        continue
                    now_labels = rep_to_labels[now_rep]
                    now_label = now_labels[0].strip(" ")
                    index_statement = "USING INDEX " + now_rep +  ":" + now_label + '(' + now_property + ') '
                    id = ''.join(random.choices('0123456789', k=5))
                    create_index = "CREATE INDEX node_index_" + id + " IF NOT EXISTS FOR (n:" + now_label + ') ON (n.' + now_property + ')'
                    drop_index = "DROP INDEX node_index_" + id + " IF EXISTS"
                    while index < match_pos[1]:
                        new_query = new_query + query[index]
                        index = index + 1
                    new_query += " " + index_statement
                    while  index < len(query):
                        new_query = new_query + query[index]
                        index = index + 1
                    return ("query_using_node_index_hint", new_query, True, (create_index, drop_index))
                
        return ("query_using_node_index_hint", None, False, None)

    def query_using_relation_index_hint(self, query):
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)
        where_pos_list = self.__parse_clause_for_where("WHERE", query)
        is_success = False
        for match_pos in sorted(match_pos_list):
            match_pattern = query[match_pos[0]:match_pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            rep_to_labels = {}    
            for single in result:
                rep = None
                labels = None
                if single[0] != '[':
                    continue
                if ':' not in single:
                    continue
                single = single.strip('[').strip(']')
                rep = single.split(':')[0].strip(" ")
                labels = single.split(':')[1:]
                rep_to_labels[rep] = labels
            for where_pos in sorted(where_pos_list):
                if match_pos[0] >= where_pos[1]:
                    continue
                if  where_pos[0] - match_pos[1] >= 7:
                    continue
                where_content = query[where_pos[0]:where_pos[1]].strip(" ")
                regular_pattern = r'\b\w+\.\w+\b'
                
                where_result = list(set(re.findall(regular_pattern, where_content)))    
                for e in where_result:
                    # is_property_indepent = True
                   
                    code = where_content
                    prop = e
                    # prop_re = re.escape(prop)
                    # for match in re.finditer(rf'\(?\b{prop_re}\b\)?', code):
                    #     start, end = match.span()

                    #     
                    #     before = code[max(0, start - 3):start].strip()
                    #     after = code[end:end + 3].strip()

                    #     
                    #     if re.match(r'^(=|<|>|!|[\+\-\*/])', after) or re.match(r'(=|<|>|!|[\+\-\*/])$', before):
                    #         is_property_indepent = False

                    # if is_property_indepent == True:
                    #     continue 

                    is_prop_index = classify_index_usage(code)
                    if prop not in is_prop_index or is_prop_index[prop] == "not_used":
                        continue
                    now_rep = e.split('.')[0].strip(" ")
                    now_property = e.split('.')[1].strip(" ")
                    if now_rep not in rep_to_labels:
                        continue
                    now_labels = rep_to_labels[now_rep]
                    now_label = now_labels[0].strip(" ")
                    index_statement = "USING INDEX " + now_rep +  ":" + now_label + '(' + now_property + ') '
                    id = ''.join(random.choices('0123456789', k=5))
                    create_index = "CREATE INDEX relation_index_" + id + " IF NOT EXISTS FOR ()-[r:" + now_label + ']-() ON (r.' + now_property + ') '
                    drop_index = "DROP INDEX relation_index_" + id + " IF EXISTS"
                    while index < match_pos[1]:
                        new_query = new_query + query[index]
                        index = index + 1
                    new_query += " " + index_statement
                    while  index < len(query):
                        new_query = new_query + query[index]
                        index = index + 1
                    return ("query_using_relation_index_hint", new_query, True, (create_index, drop_index))
                
        return ("query_using_relation_index_hint", None, False, None)     


    def query_using_node_text_index_hint(self, query):
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)
        where_pos_list = self.__parse_clause_for_where("WHERE", query)
        is_success = False
        for match_pos in sorted(match_pos_list):
            match_pattern = query[match_pos[0]:match_pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            rep_to_labels = {}    
            for single in result:
                rep = None
                labels = None
                if single[0] != '(':
                    continue
                if ':' not in single:
                    continue
                single = single.strip('(').strip(')')
                rep = single.split(':')[0].strip(" ")
                labels = single.split(':')[1:]
                rep_to_labels[rep] = labels
            for where_pos in sorted(where_pos_list):
                if match_pos[0] >= where_pos[1]:
                    continue
                if  where_pos[0] - match_pos[1] >= 7:
                    continue
                where_content = query[where_pos[0]:where_pos[1]].strip(" ")
                regular_pattern = r'\b\w+\.\w+\b'
                
                where_result = list(set(re.findall(regular_pattern, where_content)))  
                is_prop_index = classify_text_index_usage(where_content)  
                for e in where_result:
                    is_property_indepent = True
                    

                    if e not in is_prop_index or is_prop_index[e] == "not_used":
                        continue
                    now_rep = e.split('.')[0].strip(" ")
                    now_property = e.split('.')[1].strip(" ")
                    if now_rep not in rep_to_labels:
                        continue
                    now_labels = rep_to_labels[now_rep]
                    now_label = now_labels[0].strip(" ")
                    index_statement = "USING TEXT INDEX " + now_rep +  ":" + now_label + '(' + now_property + ') '
                    id = ''.join(random.choices('0123456789', k=5))
                    create_index = "CREATE TEXT INDEX node_index_" + id + " IF NOT EXISTS FOR (n:" + now_label + ') ON (n.' + now_property + ')'
                    drop_index = "DROP INDEX node_index_" + id + " IF EXISTS"
                    while index < match_pos[1]:
                        new_query = new_query + query[index]
                        index = index + 1
                    new_query += " " + index_statement
                    while  index < len(query):
                        new_query = new_query + query[index]
                        index = index + 1
                    return ("query_using_node_text_index_hint", new_query, True, (create_index, drop_index))
                
        return ("query_using_node_text_index_hint", None, False, None)

    def query_using_relation_text_index_hint(self, query):
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)
        where_pos_list = self.__parse_clause_for_where("WHERE", query)
        is_success = False
        for match_pos in sorted(match_pos_list):
            match_pattern = query[match_pos[0]:match_pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            rep_to_labels = {}    
            for single in result:
                rep = None
                labels = None
                if single[0] != '[':
                    continue
                if ':' not in single:
                    continue
                single = single.strip('[').strip(']')
                rep = single.split(':')[0].strip(" ")
                labels = single.split(':')[1:]
                rep_to_labels[rep] = labels
            for where_pos in sorted(where_pos_list):
                if match_pos[0] >= where_pos[1]:
                    continue
                if  where_pos[0] - match_pos[1] >= 7:
                    continue
                where_content = query[where_pos[0]:where_pos[1]].strip(" ")
                regular_pattern = r'\b\w+\.\w+\b'
                
                where_result = list(set(re.findall(regular_pattern, where_content)))  
                is_prop_index = classify_text_index_usage(where_content)  
                for e in where_result:
                    is_property_indepent = True
                    

                    if e not in is_prop_index or is_prop_index[e] == "not_used":
                        continue
                    now_rep = e.split('.')[0].strip(" ")
                    now_property = e.split('.')[1].strip(" ")
                    if now_rep not in rep_to_labels:
                        continue
                    now_labels = rep_to_labels[now_rep]
                    now_label = now_labels[0].strip(" ")
                    index_statement = "USING TEXT INDEX " + now_rep +  ":" + now_label + '(' + now_property + ') '
                    id = ''.join(random.choices('0123456789', k=5))
                    create_index = "CREATE TEXT INDEX relation_index_" + id + " IF NOT EXISTS FOR ()-[r:" + now_label + ']-() ON (r.' + now_property + ')'
                    drop_index = "DROP INDEX relation_index_" + id + " IF EXISTS"
                    while index < match_pos[1]:
                        new_query = new_query + query[index]
                        index = index + 1
                    new_query += " " + index_statement
                    while  index < len(query):
                        new_query = new_query + query[index]
                        index = index + 1
                    return ("query_using_relation_text_index_hint", new_query, True, (create_index, drop_index))
                
        return ("query_using_relation_text_index_hint", None, False, None)


    def query_using_node_scan_hint(self, query):
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)   
        for pos in sorted(match_pos_list):
            match_pattern = query[pos[0]:pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            random.shuffle(result)
            for single in result:
                rep = None
                labels = None
                if single[0] != '(':
                    continue
                single = single.strip('(').strip(')')
                if ':' not in single:
                    continue
                rep = single.split(':')[0].strip(" ")
                labels = single.split(':')[1:]
                label = random.choice(labels).strip(" ")
                scan_statement = "USING SCAN " + rep + ':' + label + " "
                while index < pos[1]:
                    new_query = new_query + query[index]
                    index = index + 1
                new_query += " " + scan_statement
                while index < len(query):
                    new_query = new_query + query[index]
                    index = index + 1
                return ("query_using_node_scan_hint", new_query, True,None)
        
        return ("query_using_node_scan_hint", None, False,None)

    def query_using_relation_scan_hint(self, query):
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)   
        for pos in sorted(match_pos_list):
            match_pattern = query[pos[0]:pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            random.shuffle(result)
            for single in result:
                rep = None
                labels = None
                if single[0] != '[':
                    continue
                single = single.strip('[').strip(']')
                if ':' not in single:
                    continue
                rep = single.split(':')[0].strip(" ")
                labels = single.split(':')[1:]
                label = random.choice(labels).strip(" ")
                scan_statement = "USING SCAN " + rep + ':' + label + " "
                while index < pos[1]:
                    new_query = new_query + query[index]
                    index = index + 1
                new_query += " " + scan_statement
                while index < len(query):
                    new_query = new_query + query[index]
                    index = index + 1
                return ("query_using_relation_scan_hint", new_query, True,None)
        
        return ("query_using_relation_scan_hint", None, False,None)
    
    def query_using_join_single_node_hint(self, query):
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)
        for pos in sorted(match_pos_list):
            match_pattern = query[pos[0]:pos[1]]
            each_pattern_list = match_pattern.split(",")
            rep_count = {}
            for single_pattern in each_pattern_list:

                regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
                result = re.findall(regular_pattern, single_pattern)
                
                for i in range(len(result)):
                    is_success = False
                    single = result[i]
                    rep = None
                    if single[0] != '(':
                        continue
                    single = single.strip(')').strip('(')
                    rep = single.split(':')[0].strip(" ")
                    
                    if rep not in rep_count:
                        rep_count[rep] = 1
                    else:
                        if rep_count[rep] == 1:
                            is_success = True
                        rep_count[rep] += 1
                    
                    if i != 0 and i != (len(result) - 1):
                        is_success = True
                    if is_success == True:
                        join_hint = "USING JOIN ON " + rep + " "
                        while index < pos[1]:
                            new_query = new_query + query[index]
                            index = index + 1
                        new_query = new_query + " " + join_hint
                        while index < len(query):
                            new_query = new_query + query[index]
                            index = index + 1
                        return ("query_using_join_single_node_hint", new_query, True, None)
        
        return ("query_using_join_single_node_hint", None, False, None)
    
    def query_using_join_single_node_optional_hint(self, query):
        #TODO
        pass

# ------------------------------------------------------

    
    def rewrite_query_with_hint(self, query:str)->str:

        rep_type_labels_global = {}
        index = 0
        new_query = ""
        
        match_pos_list = self.__parse_clause("MATCH", query)
        where_pos_list = self.__parse_clause("WHERE", query)
        for pos in sorted(match_pos_list):
            
            available_hint_list = []
            
            
            match_pattern = query[pos[0]:pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            rep_type_labels_local = {}
            type_rep_label_map = {}
            
            for single in result:
                type = None
                rep = None
                labels = None
                if single[0] == '(':
                    type = "node"
                else:
                    type = "edge"
                if ':' in single:
                    if type == "node":
                        single = single.strip(')').strip('(')
                    else:
                        single = single.strip('[').strip(']')
                    rep = single.split(':')[0].strip(" ")
                    labels = single.split(':')[1:]
                    if rep not in rep_type_labels_global:
                        rep_type_labels_global[rep] = (type, labels)
                        rep_type_labels_local[rep] = (type, labels)
                    else:
                        continue
                else:
                    continue
            
            for where_pos in sorted(where_pos_list):
                
                if pos[0] >= where_pos[1]:
                    continue
                where_content = query[where_pos[0]:where_pos[1]].strip(" ")
                regular_pattern = r'\b\w+\.\w+\b'
                
                where_result = list(set(re.findall(regular_pattern, where_content)))
                
                for e in where_result:
                    is_property_indepent = True
                    
                    code = where_content
                    prop = e
                    prop_re = re.escape(prop)
                    for match in re.finditer(rf'\(?\b{prop_re}\b\)?', code):
                        start, end = match.span()

                        
                        before = code[max(0, start - 3):start].strip()
                        after = code[end:end + 3].strip()

                        
                        if re.match(r'^(=|<|>|!|[\+\-\*/])', after) or re.match(r'(=|<|>|!|[\+\-\*/])$', before):
                            is_property_indepent = False

                    if is_property_indepent == True:
                        continue
                    
                    
                    now_rep = e.split('.')[0].strip(" ")
                    now_property = e.split('.')[1]
                    if now_rep not in rep_type_labels_local:
                        continue
                    now_type = rep_type_labels_local[now_rep][0]
                    now_labels = rep_type_labels_local[now_rep][1]

                    for single_label in now_labels:
                        
                        single_label = single_label.strip(" ")
                        type_label_property = now_type + "_" + single_label + "_" + now_property
                        if type_label_property not in self.exist_index:
                            continue
                        index_type = "INDEX " if self.exist_index[type_label_property] == 1 else "TEXT INDEX "
                        
                        index_statement = "USING " + index_type + now_rep + ':' + single_label + '(' + now_property + ') '
                        available_hint_list.append(index_statement)

            

            
            k = random.randint(min(1, len(available_hint_list)), len(available_hint_list))
            chosed_hint = random.sample(available_hint_list, k)
            while index < pos[1]:
                new_query = new_query + query[index]
                index = index + 1
            for single_hint in chosed_hint:
                new_query += " " + single_hint
        
        while index < len(query):
            new_query = new_query + query[index]
            index = index + 1
        

        return new_query

    
    def rewrite_query_with_scan_hint(self, query:str)->str:
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)
        match2available_hint_list = {}
        for pos in sorted(match_pos_list):
            available_hint_list = []
            
            match_pattern = query[pos[0]:pos[1]]
            regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
            result = re.findall(regular_pattern, match_pattern)
            for single in result:
                type = None
                rep = None
                labels = None
                if single[0] == '(':
                    type = "node"
                else:
                    type = "edge"
                if ':' in single:
                    if type == "node":
                        single = single.strip(')').strip('(')
                    else:
                        single = single.strip('[').strip(']')
                    rep = single.split(':')[0].strip(" ")
                    labels = single.split(':')[1:]
                    single_label = random.choice(labels).strip(" ")
                    scan_index_statement = "USING SCAN " + rep + ":" + single_label + " "
                    available_hint_list.append(scan_index_statement)
            match2available_hint_list[tuple(pos)] = available_hint_list
            while index < pos[1]:
                new_query = new_query + query[index]
                index = index + 1
            
            if len(available_hint_list) >= 1:
                new_query += " " + random.choice(available_hint_list)
        while index < len(query):
            new_query = new_query + query[index]
            index = index + 1
        
        return new_query
    

    def rewrite_query_with_join_hint(self, query:str)->str:
        index = 0
        new_query = ""
        match_pos_list = self.__parse_clause("MATCH", query)
        for pos in sorted(match_pos_list):
            available_hint_list = []
            match_pattern = query[pos[0]:pos[1]]
            each_pattern_list = match_pattern.split(",")
            rep_count = {}
            for single_pattern in each_pattern_list:

                regular_pattern = r"(\([^\(\)]*\)|\[[^\[\]]*\])"
                result = re.findall(regular_pattern, single_pattern)
                
                for i in range(len(result)):
                    is_success = False
                    single = result[i]
                    rep = None
                    if single[0] != '(':
                        continue

                    single = single.strip(')').strip('(')
                    rep = single.split(':')[0].strip(" ")
                    
                    if rep not in rep_count:
                        rep_count[rep] = 1
                    else:
                        if rep_count[rep] == 1:
                            is_success = True
                        rep_count[rep] += 1
                    
                    if i != 0 and i != (len(result) - 1):
                        is_success = True
                    if is_success == True:
                        join_hint = "USING JOIN ON " + rep + " "
                        available_hint_list.append(join_hint)

            while index < pos[1]:
                new_query = new_query + query[index]
                index = index + 1
            
            
            if len(available_hint_list) >= 1:
                new_query += " " + random.choice(available_hint_list)
        while index < len(query):
            new_query = new_query + query[index]
            index = index + 1
        
        return new_query

    def select_rules(self, query:str)->str:
        decode = ""
        for i in range(len(self.hint_rules)):
            is_success = self.hint_rules[i](query)
            if is_success[2] == True:
                decode += "1"
            else:
                decode += "0"
        self.now_decode=decode
        if decode not in self.hints_table_pool:
            rule_num = 0
            for single in range(len(decode)):
                if decode[single] == '1':
                    rule_num += 1
            init_p = 1.0 / rule_num
            hints_table = {}
            for i in range(len(self.hint_rules)):
                if decode[i] == '1':
                    hints_table[self.hint_rules[i]] = init_p
            self.hints_table_pool[decode] = hints_table
        my_hints_table = self.hints_table_pool[decode]
        selected = random.random()
        sum = 0.0
        for key, value in my_hints_table.items():
            if selected < value + sum :
                return key(query)
            sum += value
        
        # if selected < self.hints_table["query_using_node_index_hint"]:
        #     sum += self.hints_table["query_using_node_index_hint"]
        #     return self.query_using_node_index_hint(query)
        # elif selected < sum + self.hints_table["query_using_relation_index_hint"]:
        #     sum += self.hints_table["query_using_relation_index_hint"]
        #     return self.query_using_relation_index_hint(query)
        # elif selected < sum + self.hints_table["query_using_node_text_index_hint"]:
        #     sum += self.hints_table["query_using_node_text_index_hint"]
        #     return self.query_using_node_text_index_hint(query)
        # elif selected < sum + self.hints_table["query_using_relation_text_index_hint"]:
        #     sum += self.hints_table["query_using_relation_text_index_hint"]
        #     return self.query_using_relation_text_index_hint(query)
        # elif selected < sum + self.hints_table["query_using_node_scan_hint"]:
        #     sum += self.hints_table["query_using_node_scan_hint"]
        #     return self.query_using_node_scan_hint(query)
        # elif selected < sum + self.hints_table["query_using_relation_scan_hint"]:
        #     sum += self.hints_table["query_using_relation_scan_hint"]
        #     return self.query_using_relation_scan_hint(query)
        # else :
        #     return self.query_using_join_single_node_hint(query)
        
        


     