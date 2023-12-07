
import json
import pickle

import numpy as np

from typing import Any
from .hint_sets import HintSet


def load_json(path: str):
    with open(path, 'r') as file:
        loaded = json.load(file)
    return loaded


def save_json(to_save: Any, path: str) -> None:
    json_dict = json.dumps(to_save)
    with open(path, 'w') as f:
        f.write(json_dict)
    return


def load_pickle(path: str):
    with open(path, 'rb') as file:
        loaded = pickle.load(file)
    return loaded


def save_pickle(to_save: Any, path: str) -> None:
    with open(path, 'wb') as f:
        pickle.dump(to_save, f)
    return


def binary_to_int(bin_list: list[int]) -> int:
    return int("".join(str(x) for x in bin_list), 2)


def int_to_binary(integer: int) -> list[int]:
    return [int(i) for i in bin(integer)[2:].zfill(len(HintSet.operators))]


def one_hot_to_binary(one_hot_vector: list[int]) -> list[int]:
    ind = int(np.argmax(one_hot_vector))
    return int_to_binary(ind)
