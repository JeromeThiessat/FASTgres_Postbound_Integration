
import os
from mo_sql_parsing import parse
from ..log_utils import get_logger
from tqdm import tqdm
from sklearn.model_selection import train_test_split


class Workload:
    def __init__(self, path: str, name: str):
        if not os.path.exists(path):
            raise ValueError("Given Path does not exist.")
        if len(name) > 50:
            raise ValueError("Workload name exceeds character limit")
        self.path: str = path
        self.name: str = name
        self.queries: list[str] = None
        pass

    def read_query(self, query_name: str):
        with open(self.path + query_name, encoding='utf-8') as file:
            query = file.read()
        return query

    def get_queries(self):
        queries = list()
        for file in tqdm(os.scandir(self.path), desc="Loading Queries"):
            if os.path.isfile(os.path.join(self.path, file.name)):
                if file.name.endswith('sql'):
                    queries.append(file.name)
        return queries

    def load_queries(self):
        if self.queries is None:
            self.queries = self.get_queries()

    def parse_query(self, query_name: str):
        with open(self.path + query_name, encoding='utf-8') as file:
            q = file.read()
        try:
            parsed_query = parse(q)
        except:
            logger = get_logger()
            logger.info(f"Parsing error for query: {query_name}")
            raise ValueError('Could not parse query')
        return parsed_query

    def get_training_test_split(self, train_size: float, seed: int):
        if self.queries is None:
            self.load_queries()
        return train_test_split(self.queries, train_size=train_size, random_state=seed)
