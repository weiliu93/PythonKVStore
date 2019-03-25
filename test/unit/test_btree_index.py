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
    test_case_package_path = _get_test_case_package_path()
    _clean_up()
    # TODO add btree index conf



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
        os.path.join(package_root_path, "btree_index", check_name)
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
        os.path.join(package_root_path, "btree_index", check_name, "pools")
    )
    conf_path = os.path.abspath(
        os.path.join(
            package_root_path, "btree_index", check_name, "storage_conf.ini"
        )
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