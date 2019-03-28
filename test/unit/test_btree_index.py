import shutil
import inspect
import random
import sys
import os

sys.path.append(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir), "kvdb")
)

from btree_index import BTreeIndex
from memory_manager import MemoryManager

package_root_path = os.path.abspath(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir), "unit-packages")
)


def test_basic_btree_set_and_get():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = BTreeIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    index.set(1, 10)
    index.set(3, 100)
    index.set(6, 8)

    assert index.get(3) == 100
    assert index.get(6) == 8
    assert index.get(8) == None

    index.set(3, 8)
    assert index.get(3) == 8

    assert list(index.key_value_pairs()) == [(1, 10), (3, 8), (6, 8)]

    index.set(10, 100)

    assert index.get(1) == 10
    assert index.get(3) == 8
    assert index.get(6) == 8
    assert index.get(10) == 100
    assert index.get(15) == None
    assert index.get(14) == None

    assert list(index.keys()) == [1, 3, 6, 10]

    _clean_up()


def test_basic_btree_remove():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = BTreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    values = [value for value in range(1, 11, 1)]
    for value in values:
        index.set(value, value)
    random.shuffle(values)
    for value in values:
        index.remove(value)
        values.remove(value)
        assert list(index.keys()) == sorted(list(values))

    _clean_up()


def test_btree_clear():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = BTreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    values = [value for value in range(1, 11, 1)]
    for value in values:
        index.set(value, value)
    assert len(list(index.keys())) == 10

    index.clear()
    assert list(index.keys()) == []

    index.set(1, 10)
    index.set(2, 100)

    assert len(list(index.keys())) == 2

    index.clear()
    assert list(index.keys()) == []

    _clean_up()


def test_btree_keys():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = BTreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    values, vis = [], set()
    for _ in range(100):
        value = random.randint(1, 10000)
        while value in vis:
            value = random.randint(1, 10000)
        vis.add(value)
        values.append(value)
    for ind, value in enumerate(values):
        index.set(value, value)
        assert list(index.keys()) == sorted(list(values[ : ind + 1]))

    _clean_up()


def test_btree_key_value_pairs():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = BTreeIndex(MemoryManager(pool_folder=pool_folder, conf_path=conf_path, block_file=block_file))
    values, vis = [], set()
    for _ in range(100):
        value = random.randint(1, 10000)
        while value in vis:
            value = random.randint(1, 10000)
        vis.add(value)
        values.append(value)
    for ind, value in enumerate(values):
        index.set(value, value)
        assert list(index.key_value_pairs()) == sorted(list(map(lambda value: (value, value), values[: ind + 1])))

    _clean_up()


def test_btree_real_scenario():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    comp_dict = {}
    index = BTreeIndex(MemoryManager(pool_folder = pool_folder, conf_path = conf_path, block_file = block_file))
    ops = ["set", "get" , "remove", "clear"]
    for _ in range(20000):
        op_index = random.randint(1, 10)
        # weight: [4, 3, 2, 1]
        if op_index <= 4:
            op_index = 0
        elif op_index <= 7:
            op_index = 1
        elif op_index <= 9:
            op_index = 2
        else:
            op_index = 3
        op = ops[op_index]
        if op == "set":
            key, value = random.randint(1, 100), random.randint(1, 100)
            index.set(key, value)
            comp_dict[key] = value
        elif op == "get":
            key = random.randint(1, 100)
            if key not in comp_dict:
                assert index.get(key) == None
            else:
                assert comp_dict[key] == index.get(key)
        elif op == "remove":
            key = random.randint(1, 100)
            if key in comp_dict:
                comp_dict.pop(key)
                assert index.remove(key) == True
            else:
                assert index.remove(key) == False
        elif op == "clear":
            comp_dict.clear()
            index.clear()

        key_value_pairs = sorted([(key, value) for key, value in comp_dict.items()])
        assert key_value_pairs == list(index.key_value_pairs())

    _clean_up()


def _get_test_case_package_path():
    check_name = None
    frame = inspect.currentframe()
    while frame:
        if frame.f_code.co_name.startswith("test_"):
            check_name = frame.f_code.co_name
            break
        frame = frame.f_back
    assert check_name and check_name.startswith("test_")
    return os.path.abspath(os.path.join(package_root_path, "btree_index", check_name))


def _get_common_file_paths():
    check_name = None
    frame = inspect.currentframe()
    while frame:
        if frame.f_code.co_name.startswith("test_"):
            check_name = frame.f_code.co_name
            break
        frame = frame.f_back
    assert check_name and check_name.startswith("test_")

    pool_folder = os.path.abspath(
        os.path.join(package_root_path, "btree_index", check_name, "pools")
    )
    conf_path = os.path.abspath(
        os.path.join(package_root_path, "btree_index", check_name, "storage_conf.ini")
    )
    block_file = os.path.abspath(
        os.path.join(package_root_path, "btree_index", check_name, "block_file")
    )
    return pool_folder, conf_path, block_file


def _clean_up():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)
