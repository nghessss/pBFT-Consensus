# core/utils.py
import random

def random_uniform():
    """Return a float in [0,1). Just wrapper to keep code readable."""
    return random.random()

def log_term(log, index_one_based: int):
    """Return term at 1-based index. If out of range => 0."""
    if index_one_based < 1 or index_one_based > len(log):
        return 0
    return log[index_one_based - 1]["term"]
