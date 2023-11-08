import random
import time

import numpy as np

import utility as u
from hint_sets import HintSet
from log_utils import get_logger
from models.experience import Experience
from models.train_model import Settings
from workloads.workload import Workload
from database_connection import DatabaseConnection
from query_encoding.feature_extractor import EncodingInformation
from experiment import Experiment
from models.sync_model import Synchronizer
from models.context import Context
from query_encoding.query import Query
from query_encoding.encoded_query import EncodedQuery
from public_api import *
from definitions import PG_IMDB, PG_STACK_OVERFLOW


class TrainingPhase:

    def __init__(self, experiment: Experiment):
        self.training_queries = experiment.training_queries
        self.enc_info = experiment.encoding_info
        self.db_conn = experiment.db_connection
        self.archive = experiment.archive
        self.models = None
        self.workload = experiment.workload
        self.settings = experiment.settings

    def train(self):
        logger = get_logger()
        logger.info("Setting query information")
        global_experience = Experience(self.archive)
        for query_name in self.training_queries:
            query = Query(query_name, self.workload)
            global_experience.add_experience(query)

        logger.info("Training context models")
        context_queries = global_experience.sort_by_context()
        context_models = dict()
        for context_set in context_queries:
            context = Context()
            context.add_context(context_set)
            local_experience = Experience(self.archive)
            local_experience.add_experiences(context_queries[context_set])
            synchronizer = Synchronizer(context, self.db_conn, self.enc_info, local_experience, self.settings)
            synchronizer.build_model()
            context_models[context_set] = synchronizer
        self.models = context_models
        logger.info("Finished training context models")

    def save_models(self, save_path: str):
        if self.models is not None:
            u.save_pickle(self.models, save_path)
        else:
            raise ValueError("Trying to save None model")


class TestingPhase:

    def __init__(self, experiment: Experiment, context_models: dict):
        self.experiment = experiment
        self.archive = experiment.archive
        self.workload = experiment.workload
        self.testing_queries = experiment.testing_queries
        self.encoding_info = experiment.encoding_info
        self.context_models = context_models
        self.predictions = {'initial': {}, 'final': {}}

    def test(self):
        logger = get_logger()
        logger.info("Starting testing phase")
        for query_name in self.testing_queries[:10]:
            query = Query(query_name, self.workload)
            context_set = query.context
            context = Context()
            context.add_context(context_set)
            # encoder = Encoder(context, self.encoding_info)
            # encoded_query = encoder.encode_query(encoder.build_feature_dict_old(query))
            encoded_query = EncodedQuery(context, query, self.encoding_info)
            encoded_query.encode_query()

            synchronizer: Synchronizer = self.context_models[context_set]
            prediction = synchronizer.model.predict(encoded_query.encoded_query)
            logger.info(f"Query {query.name[:8]} with prediction: {prediction}")
            self.predictions['initial'][query_name] = prediction

            # synchronizer.pre_execution()
            # execution_time = synchronizer.run_query(query, HintSet(prediction), self.workload, use_timeout=True)
            # global_reduction = synchronizer.post_execution(encoded_query, execution_time)
            # for c_set, sync in self.context_models.items():
            #     sync.reduce_cooldown(global_reduction)


def run_workload():
    logger = get_logger()
    logger.info("Starting Fastgres Evaluation")

    # defining a workload
    workload = Workload("workloads/queries/job/", "job")
    workload.load_queries()
    database_connection = DatabaseConnection(PG_IMDB, "job_connection")

    encoding_info = EncodingInformation(database_connection, "database_statistics/job/", workload)
    # use if not built yet
    # encoding_info.build_encoding_info(database_connection, rebuild=True)
    encoding_info.load_encoding_info()

    logger.info("Finished Encoding Pre-Pass")

    archive_path = "archives/job/12.4/job_eval_dict_ax2_fixed.json"
    train_size = 0.9
    seed = 29
    percentile = 99
    absolute = 1.0
    settings = Settings(seed, percentile, absolute)

    # Setting random seeds for the run
    np.random.seed(settings.seed)
    random.Random(settings.seed)

    experiment = Experiment(workload, encoding_info, database_connection,
                            archive_path, train_size, settings)

    training_phase = TrainingPhase(experiment)
    training_phase.train()
    training_phase.save_models("models/saved_models/job/fastgres.pkl")

    testing_phase = TestingPhase(experiment, training_phase.models)
    testing_phase.test()

    return


if __name__ == "__main__":
    run_workload()

# def build_fastgres_models(query_path, seed, archive, enc_dict, mm_dict, wc_dict, db_string,
#                           skipped_dict, use_context, args_test_queries: list, query_object_dict: dict,
#                           estimators: int, estimator_depth: int):
#     # load queries
#     queries = u.get_queries(query_path)
#     # predetermine context
#     context_queries = get_context_queries(queries, query_path, query_object_dict)
#     # merge if needed
#     if not use_context:
#         context_queries, merged_contexts = merge_context_queries(context_queries)
#     else:
#         merged_contexts = {key: {key} for key in context_queries}
#
#     # split queries
#     train_queries, test_queries = get_query_split(queries, args_test_queries)
#
#     # init context model dict and build db meta data
#     context_models = dict()
#     d_type_dict = u.build_db_type_dict(db_string)
#
#     ###################################################################################################################
#
#     print("Training/Testing/All Queries: {} / {} / {}".format(len(train_queries), len(test_queries), len(queries)))
#     print("Training contexts")
#     for context in alive_it(merged_contexts):
#         print("Training Context {} / {}"
#               .format(list(context_queries.keys()).index(context) + 1, len(context_queries.keys())))
#
#         context_train_queries = list(sorted(set(context_queries[context]).intersection(set(train_queries))))
#         if not context_train_queries:
#             # No queries -> ignore
#             context_models[context] = None
#
#         f_dict = dict()
#         for query_name in context_train_queries:
#             # query = Query(query_name, query_path)
#             query = query_object_dict[query_name]
#             f_d = eu.build_feature_dict(query, db_string, mm_dict, enc_dict, wc_dict, set(), set(), skipped_dict)
#             f_dict[query_name] = eu.encode_query(context, f_d, d_type_dict)
#
#         experience = u.tree()
#         for query_name in context_train_queries:
#             experience[query_name]["featurization"] = f_dict[query_name]
#             experience[query_name]["label"] = archive[query_name]["opt"]
#
#         # catch one elementary labels
#         label_uniques = np.unique([experience[query_name]["label"] for query_name in context_train_queries])
#         if len(label_uniques) == 1:
#             context_models[context] = int(label_uniques[0])
#         else:
#             model = GradientBoostingClassifier(n_estimators=estimators, max_depth=estimator_depth, random_state=seed)
#             x_values = [experience[query_name]["featurization"] for query_name in experience]
#             y_values = [experience[query_name]["label"] for query_name in experience]
#             model = model.fit(x_values, y_values)
#
#             context_models[context] = model
#     print("Training phase done")
#     return context_models


