from tree_index import TreeIndex
from kv_index import KVIndex
from memory_manager import MemoryManager

import pickle


class Client(object):
    def __init__(
        self,
        index_file=None,
        index_type=TreeIndex,
        conf_path=None,
        pool_folder=None,
        block_file=None,
    ):
        # If index_file is given
        if index_file:
            self._index = pickle.loads(open(index_file, "rb").read())
        else:
            # Create a new index
            self._index = index_type(MemoryManager(conf_path, pool_folder, block_file))
        assert isinstance(self._index, KVIndex)

    @property
    def index(self):
        """return backend index, other specific operations need to be delegated to index itself"""
        return self._index

    def load_index_from_file(self, filepath):
        with open(filepath, "rb") as f:
            self._index = pickle.load(f, encoding="utf-8")
        # in case we load some weird things...
        assert isinstance(self._index, KVIndex)

    def load_index_from_pickle_string(self, pickle_string):
        self._index = pickle.loads(pickle_string)
        # in case we load some weird things...
        assert isinstance(self._index, KVIndex)

    def dump_index_to_file(self, filepath):
        with open(filepath, "wb") as f:
            pickle.dump(self._index, f)

    def dump_index_to_pickle_string(self):
        return pickle.dumps(self._index)

    def set(self, key, value):
        self._index.set(key, value)

    def get(self, key, default=None):
        return self._index.get(key, default)

    def keys(self):
        return self._index.keys()

    def key_value_pairs(self):
        return self._index.key_value_pairs()

    def clear(self):
        self._index.clear()
