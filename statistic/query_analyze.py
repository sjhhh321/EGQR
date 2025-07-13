from datetime import datetime


class QueryAnalyze:
    def __init__(self, database_name, algorithm, system):
        self.database_name = database_name
        self.system = system
        self.different_query_plan = set()
        self.different_query_sum = set()
        
        self.algorithm = algorithm
        self.query_sum = 0

    
    def add_query_plan(self, query_plan):
        self.different_query_plan.add(query_plan)


    def add_query(self, query):
        self.query_sum += 1
        self.different_query_sum.add(query)

    def clear(self):

        self.different_query_plan.clear()
        self.different_query_sum.clear()
        self.query_sum = 0

    def save(self, file_path, open_method):
        with open(file_path, open_method) as file:

            sep = "-" * 100
            date_now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"algorithm: {self.algorithm}   date: {date_now}   system: {self.system}  database_name: {self.database_name}\n")
            file.write(f"query sum: {self.query_sum}\n")
            file.write(f"different query sum: {len(self.different_query_sum)}\n")
            file.write(f"different query plan sum: {len(self.different_query_plan)}\n")
            file.write(f"result: rate1(dqp/sum): {1.0 * len(self.different_query_plan) / self.query_sum : .2f}    rate2(dqp/dsum):{1.0 * len(self.different_query_plan) / len(self.different_query_sum) : .2f}\n")
            file.write(sep + "\n\n")
    
            

