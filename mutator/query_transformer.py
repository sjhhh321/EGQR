import random
from mutator.pattern_transformer import PatternTransformer
import copy
from itertools import combinations, product


def _partition_mask(partition, edge_count):
    """Encode same-chain edge pairs using the original ASG edge IDs."""
    mask = 0
    for chain in partition:
        for i, j in combinations(sorted(chain), 2):
            bit = i * (2 * edge_count - i - 1) // 2 + j - i - 1
            mask |= 1 << bit
    return mask


def _select_diverse(candidates, base_mask, limit):
    """Greedy max-min selection, with the base query always in the reference set."""
    # All distances share sum(comb(m, 2)) across MATCH clauses, so Hamming
    # counts give exactly the same ranking as normalized distances, even when
    # there are no edge pairs. Strict ties retain candidate input order.
    scores = [(mask ^ base_mask).bit_count() for _, mask in candidates]
    selected = []
    for _ in range(min(max(0, limit), len(candidates))):
        best = max(range(len(candidates)), key=scores.__getitem__)
        query, mask = candidates[best]
        selected.append(query)
        scores[best] = -1
        for i, (_, other_mask) in enumerate(candidates):
            if scores[i] >= 0:
                scores[i] = min(scores[i], (mask ^ other_mask).bit_count())
    return selected


class QueryTransformer():
    def __parse_patterns(self, query):
        pos = query.find("MATCH ")
        keywords = ["RETURN ", "OPTIONAL MATCH ", "WHERE ", "CONTAINS ", "WITHIN ",
                    "WITH ", "UNION ", "ALL ", "UNWIND ", "AS ", "MERGE ", "ON ",
                    "CREATE ", "SET ", "DETACH ", "DELETE ", "REMOVE ", "CALL ",
                    "YIELD ", "DISTINCT ", "ORDER ", "BY ", "L_SKIP ", "LIMIT ",
                    "ASCENDING ", "ASC ", "DESCENDING ", "DESC ", "OR ", "XOR ",
                    "AND ", "NOT ", "STARTS ", "ENDS ", "CONTAINS ", "IN ", "IS ",
                    "NULL ", "COUNT ", "CASE ", "ELSE ", "END ", "WHEN ", "THEN ",
                    "ANY ", "NONE ", "SINGLE ", "EXISTS ", "MATCH "
                    ]
        res = []
        while pos != -1:
            f = lambda x: query.find(x, pos + 1) if query.find(x, pos + 1) > -1 else len(query)
            next_pos = min(f(x) for x in keywords)
            res.append((pos + len("MATCH "), next_pos))
            pos = query.find("MATCH ", pos + 1)

        return res

    def mutant_query_generator(self, query):
        patterns = self.__parse_patterns(query)
        patterns = random.sample(patterns, random.randint(1, len(patterns)))
        new_query = ""
        index = 0
        for pos in sorted(patterns):
            while index < pos[0]:
                new_query = new_query + query[index]
                index = index + 1
            pattern = query[pos[0]:pos[1]]
            P = PatternTransformer()
            asg = P.pattern2asg(pattern)

            new_pattern = P.asg2pattern(asg)
            new_query = new_query + new_pattern + " "
            index = pos[1]
        while index < len(query):
            new_query = new_query + query[index]
            index = index + 1
        return new_query
    

    def mutant_query_generator_with_enum(self, query):
        patterns = self.__parse_patterns(query)
        patterns = random.sample(patterns, random.randint(1, len(patterns)))
        new_query = ""
        index = 0
        for pos in sorted(patterns):
            while index < pos[0]:
                new_query = new_query + query[index]
                index = index + 1
            pattern = query[pos[0]:pos[1]]
            P = PatternTransformer()
            asg = P.pattern2asg(pattern)
            
            new_pattern_list = P.asg2pattern_with_enum(asg)
            print(len(new_pattern_list))
            new_pattern = new_pattern_list[0]
            new_query = new_query + new_pattern + " "
            index = pos[1]
        while index < len(query):
            new_query = new_query + query[index]
            index = index + 1
        return new_query
        
    def EGE(self, query, threshold, transform_times):
        """Generate EGE candidates and return up to transform_times in max-min order."""
        if transform_times <= 0:
            return []
        patterns = sorted(self.__parse_patterns(query))
        new_patterns_list = []
        base_mask, offset = 0, 0
        for pos in patterns:
            pattern = query[pos[0]:pos[1]]
            P = PatternTransformer()
            asg, base_partition = P.pattern2asg(pattern, with_partition=True)
            base_mask |= _partition_mask(base_partition, asg.M) << offset
            new_patterns = []
            if asg.M >= threshold:
                for i in range(transform_times):
                    asg_copy = copy.deepcopy(asg)
                    new_patterns.append(P.asg2pattern(asg_copy, with_partition=True))
            else:
                new_patterns = P.asg2pattern_with_enum(asg, with_partition=True)
            new_patterns_list.append([
                (text, _partition_mask(partition, asg.M) << offset)
                for text, partition in new_patterns
            ])
            # Separate bit ranges keep edge IDs local to their original MATCH.
            offset += asg.M * (asg.M - 1) // 2

        candidates = []
        for choice in product(*new_patterns_list):
            parts, index, mask = [], 0, 0
            for (start, end), (text, pattern_mask) in zip(patterns, choice):
                parts.extend((query[index:start], text, " "))
                index = end
                mask |= pattern_mask
            parts.append(query[index:])
            candidates.append(("".join(parts), mask))
        return _select_diverse(candidates, base_mask, transform_times)
        


if __name__ == "__main__":
    Q = QueryTransformer()
    print(Q.mutant_query_generator(
        "MATCH (n0)<-[r0 :T3]-(n1 :L1)-[r1 :T3]->(n2), (n4)-[r3 :T5]->(n5 :L0 :L2) WHERE (((((n1.k7) OR (n1.k7)) AND ((r0.id) <> (r1.id))) AND ((r0.id) <> (r3.id))) AND ((r1.id) <> (r3.id))) OPTIONAL MATCH (n3)<-[]-(n4)-[]->(n5 :L2), (n6 :L0)-[r4 :T0]->(n7 :L4)<-[r5 :T2]-(n1) WHERE (true AND ((r4.id) <> (r5.id))) OPTIONAL MATCH (n7 :L4), (n9 :L3 :L1)<-[r8 :T3]-(n10 :L0 :L3 :L2)-[r9 :T4]->(n11 :L4 :L1) WHERE ((n1.k9) AND ((r8.id) <> (r9.id))) WITH max('RD') AS a0, max('sJB3c') AS a1, max('R') AS a2 ORDER BY a0 DESC OPTIONAL MATCH (n12 :L0)-[r10 :T5]->(n13 :L4 :L2)<-[r11 :T4]-(n14 :L4 :L3 :L0) WHERE ((n13.k23) AND ((r10.id) <> (r11.id))) WITH DISTINCT a1, n13, (n12.k2) AS a3, r11 WHERE (NOT true) WITH DISTINCT n13, r11, avg(-935939402) AS a4 MATCH (n15 :L2 :L1)-[r12 :T5]->(n8)<-[r13 :T0]-(n16 :L2) WHERE (((r12.id) > -1) AND ((r12.id) <> (r13.id))) RETURN DISTINCT (n16.k12) AS a5, (r12.k65) AS a6;"))
