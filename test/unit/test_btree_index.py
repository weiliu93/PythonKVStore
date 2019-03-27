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
    # TODO add more test cases


    pass


def test_btree_clear():

    pass


def test_btree_keys():

    pass


def test_btree_key_value_pairs():

    pass


def test_btree_real_scenario():

    pass


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
