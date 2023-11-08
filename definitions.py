import os
from configparser import ConfigParser

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

cfg = ConfigParser()
cfg.read(ROOT_DIR + "/config.ini")

dbs = cfg["DBConnections"]
PG_IMDB = dbs["imdb"]
PG_STACK_OVERFLOW = dbs["stack_overflow"]
PG_STACK_OVERFLOW_REDUCED_16 = dbs["stack_overflow_reduced_16"]
PG_STACK_OVERFLOW_REDUCED_13 = dbs["stack_overflow_reduced_13"]
PG_STACK_OVERFLOW_REDUCED_10 = dbs["stack_overflow_reduced_10"]
PG_TPC_H = dbs["tpc_h"]
