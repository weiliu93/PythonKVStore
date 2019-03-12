import sys
import os
import shutil
import inspect
import random

sys.path.append(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir), "kvdb")
)

from memory_manager import MemoryManager
from tree_index import TreeIndex

package_root_path = os.path.abspath(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir), "unit-packages")
)




def test_basic_set_and_get_without_persist():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = TreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    index.set(1, 10)
    index.set(3, 5)

    assert index.get(1) == 10
    assert index.get(3) == 5
    assert index.get(4, 100) == 100

    index.set(3, 20)
    assert index.get(1) == 10
    assert index.get(3) == 20

    _clean_up()

def test_set_and_get_with_persist():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = TreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    index.set(1, 10)
    index.set(3, 20)

    index.set(3, 100)

    assert index.get(1) == 10
    assert index.get(3) == 100

    assert index.persist() == 2
    assert index.persist() == 0

    index.set(1, 7)

    assert index.get(1) == 7
    assert index.get(3) == 100

    assert index.persist() == 1

    _clean_up()


def test_keys():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = TreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    index.set(1, 10)
    index.set(4, 4)
    index.set(9, 100)

    assert list(index.keys()) == [1, 4, 9]

    index.set(4, 7)
    assert list(index.keys()) == [1, 4, 9]

    index.set(20, 1)
    assert list(index.keys()) == [1, 4, 9, 20]

    index.clear()
    assert list(index.keys()) == []

    values = set()
    for i in range(10000):
        value = random.randint(1, 200)
        index.set(value, value)
        values.add(value)
    assert sorted(list(values)) == list(index.keys())

    _clean_up()


def test_key_value_pairs():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = TreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    pair_dict = {}
    for _ in range(10000):
        key = random.randint(1, 200)
        value = random.randint(1, 200)
        pair_dict[key] = value
        index.set(key, value)
    pairs = [(key, value) for key, value in pair_dict.items()]
    pairs.sort()

    assert list(index.key_value_pairs()) == pairs

    _clean_up()


def test_clear():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = TreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    index.set(1, 10)
    index.set(100, 8)
    index.clear()
    assert list(index.keys()) == []

    _clean_up()


def test_checkout():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = TreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    index.set(1, 10)
    index.set(2, 8)
    index.set(8, 100)
    index.set(2, 4)

    # Interesting time leap :-)
    retrospect_index_1 = index.checkout(backoff=0)
    assert retrospect_index_1.keys() == [1, 2, 8]

    retrospect_index_2 = index.checkout(backoff=1)
    assert retrospect_index_2.keys() == [1, 2, 8]

    retrospect_index_3 = index.checkout(backoff=2)
    assert retrospect_index_3.keys() == [1, 2]

    retrospect_index_4 = index.checkout(backoff=3)
    assert retrospect_index_4.keys() == [1]

    # Validate if all indexes are really isolated
    s = {index, retrospect_index_1, retrospect_index_2, retrospect_index_3, retrospect_index_4}
    assert len(s) == 5

    # If I update index_4, it won't affect current index's result
    retrospect_index_4.set(2, 7)
    assert retrospect_index_4.key_value_pairs() == [(1, 10), (2, 7)]
    assert index.key_value_pairs() == [(1, 10), (2, 4), (8, 100)]

    _clean_up()


def test_real_scenario():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = TreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    ops = ['set', 'get', 'persist', 'clear']

    # Now let's play!!
    comparison_dict = {}
    persist_dict = {}

    for _ in range(20000):
        op_type = random.randint(0, 10)
        # weight array is [4, 3, 2, 1], since we want to test more `set` and `get` operations
        if op_type <= 3:
            op_type = 0
        elif op_type <= 6:
            op_type = 1
        elif op_type == 8:
            op_type = 2
        else:
            op_type = 3
        if ops[op_type] == 'set':
            key, value = random.randint(1, 100), random.randint(1, 100)
            comparison_dict[key] = value
            persist_dict[key] = False
            index.set(key, value)
        elif ops[op_type] == 'get':
            key = random.randint(1, 100)
            assert index.get(key, - 1) == comparison_dict.get(key, - 1)
        elif ops[op_type] == 'persist':
            waiting_for_persist = sum([not value for key, value in persist_dict.items()])
            assert waiting_for_persist == index.persist()
            persist_dict.clear()
        else:
            index.clear()
            comparison_dict.clear()
            persist_dict.clear()
        assert sorted(list(comparison_dict.keys())) == sorted(list(index.keys()))
        assert sorted(list(comparison_dict.items())) == sorted(list(index.key_value_pairs()))

    _clean_up()


def _get_common_file_paths():
    check_name = None
    frame = inspect.currentframe()
    while frame:
        if frame.f_code.co_name.startswith('test_'):
            check_name = frame.f_code.co_name
            break
        frame = frame.f_back
    assert check_name and check_name.startswith('test_')

    pool_folder = os.path.abspath(
        os.path.join(
            package_root_path, "tree_index", check_name, "pools"
        )
    )
    conf_path = os.path.abspath(
        os.path.join(
            package_root_path,
            "tree_index",
            check_name,
            "storage_conf.ini",
        )
    )
    block_file = os.path.abspath(
        os.path.join(
            package_root_path,
            "tree_index",
            check_name,
            "block_file",
        )
    )
    return pool_folder, conf_path, block_file


def _clean_up():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)
