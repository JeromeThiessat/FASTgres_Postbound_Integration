
import os.path
import utility as u

from models.train_model import Settings
from workloads.workload import Workload
from database_connection import DatabaseConnection
from query_encoding.feature_extractor import EncodingInformation


class Experiment:

    def __init__(self, workload: Workload, encoding_info: EncodingInformation, db_connection: DatabaseConnection,
                 archive_path: str, train_size: float, settings: Settings):
        self.workload = workload
        self.encoding_info = encoding_info
        self.db_connection = db_connection
        self.archive_path = archive_path

        self.archive = None
        self.get_archive()

        self.train_size = train_size
        self.settings = settings
        self.training_queries = None
        self.testing_queries = None
        self.split_queries()

    def get_archive(self):
        if not os.path.exists(self.archive_path):
            raise ValueError("Archive path does not exist")
        self.archive = u.load_json(self.archive_path)

    def split_queries(self):
        self.training_queries, self.testing_queries = \
            self.workload.get_training_test_split(self.train_size, self.settings.seed)
