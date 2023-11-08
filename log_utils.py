
import logging
from definitions import ROOT_DIR


def get_logger():
    logging.basicConfig(filename=ROOT_DIR + '/logs/workload.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger

