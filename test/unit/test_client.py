import sys
import os
import shutil
import inspect
import random

sys.path.append(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir), "kvdb")
)

from client import Client

package_root_path = os.path.abspath(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir), "unit-packages")
)


def test_basic_client_set_and_get():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    client = Client(conf_path=conf_path, pool_folder=pool_folder, block_file=block_file)
    client.set(1, 10)
    client.set(3, 100)

    assert client.get(1) == 10
    assert client.get(3) == 100

    client.set(3, 7)
    assert client.get(3) == 7
    assert client.get(1) == 10

    _clean_up()


def test_basic_remove():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    client = Client(conf_path=conf_path, pool_folder=pool_folder, block_file=block_file)
    client.set(1, 10)
    client.set(100, 4)

    assert client.remove(1) == True
    assert client.remove(1) == False
    assert client.remove(10) == False
    assert client.remove(100) == True
    assert client.remove(1) == False
    assert client.remove(100) == False
    assert client.remove(10) == False

    _clean_up()


def test_load_index_from_file():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    temp_file = os.path.join(_get_test_case_package_path(), "index.tmp")

    client = Client(conf_path=conf_path, pool_folder=pool_folder, block_file=block_file)
    client.set(1, 10)
    client.set(2, 100)
    client.set(7, 8)

    client.dump_index_to_file(temp_file)

    # load index from file
    client = Client(
        index_file=temp_file,
        conf_path=conf_path,
        pool_folder=pool_folder,
        block_file=block_file,
    )
    assert sorted(list(client.keys())) == [1, 2, 7]

    # load index from file with another API
    client = Client(conf_path=conf_path, pool_folder=pool_folder, block_file=block_file)
    assert list(client.keys()) == []
    client.load_index_from_file(temp_file)
    assert sorted(list(client.keys())) == [1, 2, 7]

    _clean_up()
    os.remove(temp_file)


def test_load_index_from_string():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    client = Client(conf_path=conf_path, pool_folder=pool_folder, block_file=block_file)
    client.set(1, 10)
    client.set(100, 4)
    client.set(50, 100)

    string = client.dump_index_to_pickle_string()

    client = Client(conf_path=conf_path, pool_folder=pool_folder, block_file=block_file)
    assert list(client.keys()) == []
    client.load_index_from_pickle_string(string)
    assert sorted(list(client.keys())) == [1, 50, 100]

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
    return os.path.abspath(os.path.join(package_root_path, "client", check_name))


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
        os.path.join(package_root_path, "client", check_name, "pools")
    )
    conf_path = os.path.abspath(
        os.path.join(package_root_path, "client", check_name, "storage_conf.ini")
    )
    block_file = os.path.abspath(
        os.path.join(package_root_path, "client", check_name, "block_file")
    )
    return pool_folder, conf_path, block_file


def _clean_up():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)
