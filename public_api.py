
import os

from .database_connection import DatabaseConnection
from .hint_sets import HintSet
from .models.sync_model import Synchronizer
from .query_encoding.feature_extractor import EncodingInformation
from .workloads.workload import Workload
from .query_encoding.query import Query
from .query_encoding.encoded_query import EncodedQuery
from .models.context import Context
from .definitions import ROOT_DIR
from .utility import load_pickle


class Fastgres:

    def __init__(self, workload: Workload, db_connection: DatabaseConnection):
        self.workload = workload
        self.db_c = db_connection
        self.enc_info = None
        self.models = None

    def get_encoding_information(self):
        if "stack" in self.workload.name.lower():
            path = ROOT_DIR + "/database_statistics/stack/"
        elif "job" in self.workload.name.lower():
            path = ROOT_DIR + "/database_statistics/job/"
        else:
            raise NotImplemented("Unsupported workload name")
        return EncodingInformation(self.db_c, path, self.workload)

    def predict(self, query_name: str, reload: bool = False):
        if self.models is None or reload:
            if "stack" in self.workload.name.lower():
                path = ROOT_DIR + "/models/saved_models/stack/fastgres.pkl"
            elif "job" in self.workload.name.lower():
                path = ROOT_DIR + "/models/saved_models/stack/fastgres.pkl"
            else:
                raise NotImplemented("Unsupported workload name")
            context_models = load_pickle(path)
            self.models = context_models
        if self.enc_info is None:
            encoding_info = self.get_encoding_information()
            encoding_info.load_encoding_info()
            self.enc_info = encoding_info

        query = Query(query_name, self.workload)
        context_set = query.context
        synchronizer: Synchronizer = self.models[context_set]

        context = Context()
        context.add_context(context_set)
        encoded_query = EncodedQuery(context, query, self.enc_info)
        encoded_query.encode_query()
        integer_hint_set = synchronizer.model.predict(encoded_query.encoded_query)
        return HintSet(integer_hint_set)
