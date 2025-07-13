import random
from mutator.schema import *
from collections import defaultdict


class PatternTransformer(AbstractASGOperator):

    def __traversal(self, G, u_id, depth):
        res = G.Nodes[u_id].content
        Availiable_edges = list()
        for edge in G.Nodes[u_id].edges:
            if edge["id"] not in G.DeletedEdge:
                Availiable_edges.append(edge)
        length = len(Availiable_edges)
        # logger.debug(len(G.Nodes[u_id].edges))
        if length == 0:
            G.DeletedNode.add(u_id)
            return res

        if depth > 0 and random.randint(0, length) == 0:
            return res
        if depth == 0 and random.randint(0, length * 3) == 0:
            return res

        go = random.choice(Availiable_edges)
        res = res + go["content"]
        G.DeletedEdge.add(go["id"])
        res = res + self.__traversal(G, go["v"], depth + 1)
        if length == 1:
            G.DeletedNode.add(u_id)
        return res

    def __pattern2list(self, pattern):
        patterns, result, isolated_nodes = pattern.split(","), [], []
        for pattern in patterns:
            pattern = pattern.strip(" ")
            pattern = pattern.strip("\n")
            v1, r, v2 = "", "", ""
            edge_counter = 0
            for i in range(0, len(pattern)):
                if not (v1.endswith(")")):
                    v1 = v1 + pattern[i]
                elif pattern[i] == "(" or v2 != "":
                    v2 = v2 + pattern[i]
                    if pattern[i] == ")":
                        result.append((v1, r, v2))
                        v1, r, v2 = v2, "", ""
                        edge_counter += 1
                else:
                    r = r + pattern[i]
            if edge_counter == 0 and len(v1) > 0:
                isolated_nodes.append(v1)
        # logger.debug(result)
        # logger.debug(isolated_nodes)
        return result, isolated_nodes

    def __pattern2node(self, pattern):
        pattern = pattern.strip(" ").strip(")").strip("(")
        patterns = pattern.split(":")
        result = {"name": patterns[0].strip(" ")}
        labels = set()
        for i in range(1, len(patterns)):
            labels.add(patterns[i].strip(" "))
        result["labels"] = labels
        return result

    def pattern2asg(self, pattern: str):
        G = ASG()
        Node2Labels, Node2Id, Id_index = dict(), dict(), 0
        patterns, isolated_nodes = self.__pattern2list(pattern)
        for edge in patterns:
            v1, v2 = edge[0], edge[2]
            r1, r2 = self.__pattern2node(v1), self.__pattern2node(v2)
            for r in r1, r2:
                name, labels = r["name"], r["labels"]
                if name not in Node2Labels.keys():
                    Node2Labels[name] = "ALL"
                    Node2Id[name] = Id_index
                    Id_index += 1
                if len(labels) > 0:
                    if Node2Labels[name] == "ALL":
                        Node2Labels[name] = labels
                    else:
                        Node2Labels[name] = Node2Labels[name] | labels

        for node in isolated_nodes:
            # logger.debug(node)
            r = self.__pattern2node(node)
            name, labels = r["name"], r["labels"]
            if name not in Node2Labels.keys():
                Node2Labels[name] = "ALL"
                Node2Id[name] = Id_index
                Id_index += 1
            if len(labels) > 0:
                if Node2Labels[name] == "ALL":
                    Node2Labels[name] = labels
                else:
                    Node2Labels[name] = Node2Labels[name] | labels

        for name in sorted(Node2Labels.keys(), key=lambda x: Node2Id[x]):
            labels, Id = Node2Labels[name], Node2Id[name]
            G.AddNode(Node(Id, name, labels))

        for edge in patterns:
            v1, v2 = edge[0], edge[2]
            r1, r2 = self.__pattern2node(v1), self.__pattern2node(v2)
            G.AddEdge(Node2Id[r1["name"]], Node2Id[r2["name"]], edge[1])

        return G
    def pattern2asgforllm(self, pattern: str):
        
        original_path = ""
        G = ASG()
        Node2Labels, Node2Id, Id_index = dict(), dict(), 0
        patterns, isolated_nodes = self.__pattern2list(pattern)
        for i in range(len(patterns)):
            edge = patterns[i]
            v1, v2 = edge[0], edge[2]
            r1, r2 = self.__pattern2node(v1), self.__pattern2node(v2)
            for r in r1, r2:
                name, labels = r["name"], r["labels"]
                if name not in Node2Labels.keys():
                    Node2Labels[name] = "ALL"
                    Node2Id[name] = Id_index
                    Id_index += 1
                if len(labels) > 0:
                    if Node2Labels[name] == "ALL":
                        Node2Labels[name] = labels
                    else:
                        Node2Labels[name] = Node2Labels[name] | labels
            if i == 0:
                name1 = r1["name"]
                name2 = r2["name"]
                original_path += "V"+ str(Node2Id[name1])
                if edge[1][0] == '<':
                    original_path += "<-"
                else:
                    original_path += "->"
                original_path += "V" + str(Node2Id[name2])
            else:
                pre_name2 = self.__pattern2node(patterns[i - 1][2])["name"]
                name1 = r1["name"]
                name2 = r2["name"]
                
                if Node2Id[pre_name2] == Node2Id[name1]:
                    
                    if edge[1][0] == '<':
                        original_path += "<-"
                    else:
                        original_path += "->"
                    original_path += "V" + str(Node2Id[name2])
                    
                else:
                    
                    original_path += ","
                    original_path += "V"+ str(Node2Id[name1])
                    if edge[1][0] == '<':
                        original_path += "<-"
                    else:
                        original_path += "->"
                    original_path += "V" + str(Node2Id[name2])

        for node in isolated_nodes:
            # logger.debug(node)
            r = self.__pattern2node(node)
            name, labels = r["name"], r["labels"]
            if name not in Node2Labels.keys():
                Node2Labels[name] = "ALL"
                Node2Id[name] = Id_index
                Id_index += 1
            if len(labels) > 0:
                if Node2Labels[name] == "ALL":
                    Node2Labels[name] = labels
                else:
                    Node2Labels[name] = Node2Labels[name] | labels
            
            if original_path == "": 
                original_path += "V" + str(Node2Id[name])
            else:
                original_path += "," + "V" + str(Node2Id[name])

        
        for name in sorted(Node2Labels.keys(), key=lambda x: Node2Id[x]):
            labels, Id = Node2Labels[name], Node2Id[name]
            G.AddNode(Node(Id, name, labels))

        for edge in patterns:
            v1, v2 = edge[0], edge[2]
            r1, r2 = self.__pattern2node(v1), self.__pattern2node(v2)
            G.AddEdge(Node2Id[r1["name"]], Node2Id[r2["name"]], edge[1])

        return G,original_path
    
    def asg2text(self, asg: ASG) -> str:
        vertex_num = asg.N
        
        result_str = "vertexs:"
        for i in range(vertex_num):
            result_str = result_str + "V" + str(i)
            if i != vertex_num - 1:
                result_str = result_str + ","
        result_str = result_str + ";"
        
        result_str += "edges:"
        
        used_edges = set()
        for i in range(vertex_num):
            edge_size = len(asg.Nodes[i].edges)
            for j in range(edge_size):
                temp = ""
                edge = asg.Nodes[i].edges[j]
                edge_id = edge["id"]
                if edge_id in used_edges:
                    continue
                used_edges.add(edge_id)
                target_node_id = edge["v"]
                content = edge["content"]
                if content[0] == '-':
                    temp += "V" + str(i) + "->" + "V" + str(target_node_id)
                elif content[0] == '<':
                    temp += "V" + str(i) + "<-" + "V" + str(target_node_id)
                result_str += temp + ","
            

        result_str = result_str.strip(",")
        return result_str
    
    def getNodeId(self, node:str)->int:
        last = node[-1]
        if last == '<':
            node_id = int(node.split('<')[0].split('V')[1])
        else:
            node_id = int(node.split('V')[1])
        return node_id


    def response2pattern(self, asg: ASG, response: str) -> str:
        
        result=""
        road_list = response.split(",")
        used_edge = []
        for road_num in range(len(road_list)):
            road = road_list[road_num]
            
            road = road.strip(" ")

            nodes = road.split("-")
            for i in range(len(nodes)):
                node = nodes[i]
                node_id = self.getNodeId(node)
                result += asg.Nodes[node_id].content
                
                if i != (len(nodes) - 1):
                    node_next = nodes[i + 1]
                    node_next_id = self.getNodeId(node_next)
                
                    edge_id = None
                    
                    for edge in asg.Nodes[node_id].edges:
                        if edge["id"] in used_edge:
                            continue
                        if edge["v"] == node_next_id:
                            edge_id = edge["id"]
                            used_edge.append(edge_id)
                            result += edge["content"]
                            break
            
            if road_num != (len(road_list) - 1):
                result += ','
        
        return result

    def asg2pattern(self, asg: ASG):
        result = []
        while len(asg.DeletedEdge) < asg.M or len(asg.DeletedNode) < asg.N:
            Availiable_Nodes = list()
            for i in range(0, asg.N):
                if i not in asg.DeletedNode:
                    Availiable_Nodes.append(i)
            start_id = random.choice(Availiable_Nodes)
            result.append(self.__traversal(asg, start_id, 0))
        result = ", ".join(result)
        # logger.debug(result)
        return result
    
    def get_partitions(self, collection):
        if not collection:
            yield []
            return
        if len(collection) == 1:
            yield [collection]
            return
        first = collection[0]
        for smaller in self.get_partitions(collection[1:]):
            for i, subset in enumerate(smaller):
                yield smaller[:i] + [[first] + subset] + smaller[i+1:]
            yield [[first]] + smaller       


    def get_edges(self, asg:ASG):
        vertex_num = asg.N
        
        used_edges = set()
        all_edges = []
        for i in range(vertex_num):
            edge_size = len(asg.Nodes[i].edges)
            for j in range(edge_size):
                edge = asg.Nodes[i].edges[j]
                edge_id = edge["id"]
                if edge_id in used_edges:
                    continue
                used_edges.add(edge_id)
                target_node_id = edge["v"]
                
                all_edges.append((i, target_node_id, edge_id))
        return all_edges

    def build_subgraph(self, edges):
        graph = defaultdict(set)
        for u, v, id in edges:
            graph[u].add(v)
            graph[v].add(u)
        return graph 
    
    def is_connected(self, edges):
        graph = self.build_subgraph(edges)
        visited = set()
        nodes = list(graph.keys())
        if not nodes:
            return False
        def dfs(node):
            visited.add(node)
            for nei in graph[node]:
                if nei not in visited:
                    dfs(nei)
        
        dfs(nodes[0])
        return len(visited) == len(graph)
    
    
    def is_eulerpath(self, edges):
        node_cnt = {}
        odd_num = 0
        for u,v, id in edges:
            if u not in node_cnt:
                node_cnt[u] = 1
            else:
                node_cnt[u] += 1
            if v not in node_cnt:
                node_cnt[v] = 1
            else:
                node_cnt[v] += 1
        
        for value in node_cnt.values():
            if value % 2 == 1:
                odd_num += 1
        return odd_num == 0 or odd_num == 2
    

    #   Hierholzer 
    def find_eulerian_path(self, edges):
        
        g = defaultdict(list)
        for u, v, id in edges:
            g[u].append(v)
            g[v].append(u)

        
        start = None
        odd_degree = [node for node in g if len(g[node]) % 2 == 1]
        if len(odd_degree) == 0:
            start = next(iter(g)) 
        elif len(odd_degree) == 2:
            start = odd_degree[0]
        else:
            return None
        path = []
        stack = [start]
        while stack:
            u = stack[-1]
            if g[u]:
                v = g[u].pop()
                g[v].remove(u) 
                stack.append(v)
            else:
                path.append(stack.pop())
        
        return path[::-1] 

        
    def asg2pattern_with_enum(self, asg:ASG):
        
        results = []
        collection = self.get_edges(asg)
        
        all_partitions = self.get_partitions(collection)
        for partition in all_partitions:
            is_all_success = True
            for group in partition:
                
                if not self.is_connected(group):
                    is_all_success = False
                    break
                
                if not self.is_eulerpath(group):
                    is_all_success = False
                    break

            
            if not is_all_success:
                continue
            
            
            pattern = ""
            
            is_node_used = {}
            is_edge_used = {}
            for group in partition:
                euler_path = self.find_eulerian_path(group)
                
                for i in range(len(euler_path) - 1):
                    node_id = euler_path[i]
                    next_node_id = euler_path[i + 1]
                    
                    if node_id not in is_node_used:
                        is_node_used[node_id] = 1
                        pattern += asg.Nodes[node_id].content
                    else:
                        pattern += "(" + asg.Nodes[node_id].name + ")"
                    
                    edge_id = None
                    for start, end, id in group:
                        if (start == node_id and end == next_node_id) or (start == next_node_id and end == node_id):
                            if id not in is_edge_used:
                                edge_id = id
                                is_edge_used[id] = 1
                                break
                            else:
                                continue
                    for edge in asg.Nodes[node_id].edges:
                        if edge["id"] == edge_id:
                            pattern += edge["content"]
                            break
                    
                    if i == (len(euler_path) - 2):
                        if next_node_id not in is_node_used:
                            is_node_used[next_node_id] = 1
                            pattern += asg.Nodes[next_node_id].content
                        else:
                            pattern += "(" + asg.Nodes[next_node_id].name + ")"
                pattern += ","
                
            
            for i in range(asg.N):
                
                if len(asg.Nodes[i].edges) == 0:
                    pattern += asg.Nodes[i].content + ","
            
            
            pattern = pattern.strip(",")
            results.append(pattern)
        
        return results

    