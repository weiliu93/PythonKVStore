import sys
import os
import shutil
import inspect
import random

sys.path.append(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir), "kvdb")
)

from skiplist_index import SkipListIndex
from memory_manager import MemoryManager

package_root_path = os.path.abspath(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir), "unit-packages")
)


def test_skiplist_basic_set_and_get():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    index.set(1, 10)
    index.set(10, 100)
    index.set(7, 19)

    assert index.get(1) == 10
    assert index.get(10) == 100
    assert index.get(7) == 19

    index.set(10, 3)

    assert index.key_value_pairs() == [(1, 10), (7, 19), (10, 3)]

    assert index.get(10) == 3

    _clean_up()


def test_repeated_set_and_get_same_key():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    comparison_dict = {}
    for _ in range(100):
        key, value = random.randint(1, 5), random.randint(1, 100)
        comparison_dict[key] = value
        index.set(key, value)
        assert (
            sorted([(key, value) for key, value in comparison_dict.items()])
            == index.key_value_pairs()
        )

    _clean_up()


def test_skiplist_real_scenario():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    comparision_dict = {}
    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    for _ in range(20000):
        ind = random.randint(1, 10)
        # set: 3, get: 3, remove: 3, clear: 1
        if ind <= 3:
            op = "set"
        elif ind <= 6:
            op = "get"
        elif ind <= 9:
            op = "remove"
        else:
            op = "clear"
        # dispatch operations
        if op == "set":
            key, value = random.randint(1, 50), random.randint(1, 1000)
            comparision_dict[key] = value
            index.set(key, value)
        elif op == "get":
            key = random.randint(1, 50)
            assert comparision_dict.get(key, -1) == index.get(key, -1)
        elif op == "remove":
            key = random.randint(1, 50)
            if key in comparision_dict:
                value1 = True
                comparision_dict.pop(key)
            else:
                value1 = False
            value2 = index.remove(key)
            assert value1 == value2
        elif op == "clear":
            comparision_dict.clear()
            index.clear()
        # compare key-value pairs
        assert sorted(list(index.key_value_pairs())) == sorted(
            [(key, value) for key, value in comparision_dict.items()]
        )
        # check skiplist lists
        cnt = sum(map(lambda head: not head.right, index._heads))
        # it could have at most one empty list, and the only scenario is empty SkipList
        assert cnt <= 1
        if cnt == 1:
            assert len(index._heads) == 1
    index._memory_manager.close()

    _clean_up()


def test_skiplist_keys():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    keys = []
    vis = set()
    for _ in range(100):
        key = random.randint(1, 10000)
        while key in vis:
            key = random.randint(1, 10000)
        vis.add(key)
        keys.append(key)
    keys.sort()
    assert len(keys) == 100 and len(set(keys)) == 100

    for key in keys:
        index.set(key, key)
    assert index.keys() == keys

    _clean_up()


def test_skiplist_key_value_pairs():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    key_value_pairs = []
    vis = set()
    for _ in range(100):
        key = random.randint(1, 10000)
        value = random.randint(1, 10000)
        while key in vis:
            key = random.randint(1, 10000)
        vis.add(key)
        key_value_pairs.append((key, value))
    key_value_pairs.sort()
    assert len(key_value_pairs) == 100 and len(set(key_value_pairs)) == 100

    for key, value in key_value_pairs:
        index.set(key, value)
    assert index.key_value_pairs() == key_value_pairs

    _clean_up()


def test_skiplist_remove():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    index.set(1, 10)
    index.set(10, 20)

    assert index.remove(1) == True
    assert index.remove(10) == True
    assert index.remove(1) == False
    assert index.remove(10) == False
    assert index.remove(100) == False

    assert index.keys() == []

    _clean_up()


def test_skiplist_clear():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    for i in range(100):
        index.set(random.randint(1, 100), random.randint(1, 1000))
    index.clear()

    assert index.keys() == []
    assert (
        len(index._heads) == 1
        and not index._heads[0].right
        and not index._heads[0].down
    )

    _clean_up()


def test_skiplist_compact():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    index.set(1, 10)
    index.set(100, 20)
    index.clear()
    index.compact()

    # all blocks are compacted
    for block in index._blocks:
        assert block.used_memory == 0 and block.free_memory == block.block_size

    index.set(1, 10)
    index.set(2, 100)
    free_memory_list_origin = [block.free_memory for block in index._blocks]

    index.remove(1)
    index.compact()
    free_memory_list_now = [block.free_memory for block in index._blocks]

    assert index.get(1, -1)
    assert index.get(2, 100)
    assert index.keys() == [2]

    assert len(free_memory_list_origin) == len(free_memory_list_now)

    cnt = 0
    for origin, now in zip(free_memory_list_origin, free_memory_list_now):
        assert origin <= now
        if origin < now:
            cnt += 1
    assert cnt > 0

    _clean_up()


def test_skiplist_real_scenario_with_compact():
    """almost all things are same as `test_skiplist_real_scenario` except randomly compaction"""
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    comparision_dict = {}
    index = SkipListIndex(
        MemoryManager(
            pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
        )
    )
    for _ in range(20000):
        ind = random.randint(1, 10)
        # set: 3, get: 3, remove: 3, clear: 1
        if ind <= 3:
            op = "set"
        elif ind <= 6:
            op = "get"
        elif ind <= 9:
            op = "remove"
        else:
            op = "clear"
        # dispatch operations
        if op == "set":
            key, value = random.randint(1, 50), random.randint(1, 1000)
            comparision_dict[key] = value
            index.set(key, value)
        elif op == "get":
            key = random.randint(1, 50)
            assert comparision_dict.get(key, -1) == index.get(key, -1)
        elif op == "remove":
            key = random.randint(1, 50)
            if key in comparision_dict:
                value1 = True
                comparision_dict.pop(key)
            else:
                value1 = False
            value2 = index.remove(key)
            assert value1 == value2
        elif op == "clear":
            comparision_dict.clear()
            index.clear()
        # do compaction randomly
        if random.random() < 0.2:
            index.compact()
        # compare key-value pairs
        assert sorted(list(index.key_value_pairs())) == sorted(
            [(key, value) for key, value in comparision_dict.items()]
        )
        # check skiplist lists
        cnt = sum(map(lambda head: not head.right, index._heads))
        # it could have at most one empty list, and the only scenario is empty SkipList
        assert cnt <= 1
        if cnt == 1:
            assert len(index._heads) == 1
    index._memory_manager.close()

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
    return os.path.abspath(
        os.path.join(package_root_path, "skiplist_index", check_name)
    )


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
        os.path.join(package_root_path, "skiplist_index", check_name, "pools")
    )
    conf_path = os.path.abspath(
        os.path.join(
            package_root_path, "skiplist_index", check_name, "storage_conf.ini"
        )
    )
    block_file = os.path.abspath(
        os.path.join(package_root_path, "skiplist_index", check_name, "block_file")
    )
    return pool_folder, conf_path, block_file


def _clean_up():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)
