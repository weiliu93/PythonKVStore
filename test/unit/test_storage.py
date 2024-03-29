import sys
import os
import inspect
import shutil

sys.path.append(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir), "kvdb")
)

from memory_manager import MemoryManager

package_root_path = os.path.abspath(
    os.path.join(os.path.join(os.path.dirname(__file__), os.pardir), "unit-packages")
)


def test_memory_manager_allocate_block():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)

    manager = MemoryManager(
        conf_path=conf_path, pool_folder=pool_folder, block_file=block_file
    )

    block = manager.allocate_block(8)
    assert block.used_memory == 0 and block.free_memory == 8 and block.block_size == 8

    # check files
    file_paths = sorted(list(os.listdir(pool_folder)))
    assert file_paths == ["pool_0", "pool_1"]

    def check_content(file_path, target_content_string):
        with open(file_path, "rb") as f:
            bytes = f.read()
            assert bytes == target_content_string.encode("utf-8")

    # check content
    check_content(os.path.join(pool_folder, "pool_0"), "0001000000")
    check_content(os.path.join(pool_folder, "pool_1"), "0000800000")

    manager = MemoryManager(
        conf_path=conf_path, pool_folder=pool_folder, block_file=block_file
    )
    assert len(manager.pools) == 2
    assert len(manager.blocks) == 1

    block = manager.blocks[0]
    assert block.used_memory == 0 and block.free_memory == 8 and block.block_size == 8

    pools = manager.pools
    pools.sort(key=lambda pool: pool.pool_id)
    assert pools[0].filepath == os.path.join(pool_folder, "pool_0")
    assert pools[1].filepath == os.path.join(pool_folder, "pool_1")

    assert pools[0].pool_allocate_limit == 0
    assert pools[0].pool_allocate_offset == 10
    assert pools[1].pool_allocate_limit == 2
    assert pools[1].pool_allocate_offset == 8

    _clean_up()


def test_memory_manager_allocate_multi_blocks():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    manager = MemoryManager(
        conf_path=conf_path, pool_folder=pool_folder, block_file=block_file
    )
    block1 = manager.allocate_block(3)
    block2 = manager.allocate_block(9)

    assert (
        block1.used_memory == 0 and block1.free_memory == 3 and block1.block_size == 3
    )
    assert (
        block2.used_memory == 0 and block2.free_memory == 9 and block2.block_size == 9
    )

    def check_content(filepath, target_content_string):
        with open(filepath, "rb") as f:
            bytes = f.read()
            assert bytes == target_content_string.encode("utf-8")

    filepath1 = os.path.join(pool_folder, "pool_0")
    filepath2 = os.path.join(pool_folder, "pool_1")
    filepath3 = os.path.join(pool_folder, "pool_2")

    check_content(filepath1, "0001000000")
    check_content(filepath2, "0001000000")
    check_content(filepath3, "0000700000")

    # check second block which span three pools
    assert len(block2.memory_segments) == 3
    assert (
        block2.memory_segments[0].pool.pool_id == 0
        and block2.memory_segments[0].start_offset == 8
        and block2.memory_segments[0].end_offset == 10
        and block2.memory_segments[0].length == 2
    )

    assert (
        block2.memory_segments[1].pool.pool_id == 1
        and block2.memory_segments[1].start_offset == 5
        and block2.memory_segments[1].end_offset == 10
        and block2.memory_segments[1].length == 5
    )

    assert (
        block2.memory_segments[2].pool.pool_id == 2
        and block2.memory_segments[2].start_offset == 5
        and block2.memory_segments[2].end_offset == 7
        and block2.memory_segments[2].length == 2
    )

    # check first block which only occupy one block
    assert len(block1.memory_segments) == 1
    assert (
        block1.memory_segments[0].pool.pool_id == 0
        and block1.memory_segments[0].start_offset == 5
        and block1.memory_segments[0].end_offset == 8
    )

    _clean_up()


def test_write_data_to_block():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    def check_content(filepath, target_content_string):
        with open(filepath, "rb") as f:
            bytes = f.read()
            assert bytes == target_content_string.encode("utf-8")

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    block = manager.allocate_block(9)
    block.write("something")

    assert len(manager.pools) == 2
    assert len(manager.blocks) == 1

    filepath1 = os.path.join(pool_folder, "pool_0")
    filepath2 = os.path.join(pool_folder, "pool_1")
    check_content(filepath1, "00010somet")
    check_content(filepath2, "00009hing0")

    assert block.used_memory == 9 and block.free_memory == 0 and block.block_size == 9

    _clean_up()


def test_write_too_much_data_to_block():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    block = manager.allocate_block(5)

    assert len(manager.blocks) == 1
    assert len(manager.pools) == 1
    assert block.used_memory == 0 and block.free_memory == 5 and block.block_size == 5

    try:
        block.write("hahehe")
        assert False
    except:
        pass

    _clean_up()


def test_read_data_from_block():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    block = manager.allocate_block(10)
    block.write("helloworld")

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    block = manager.blocks[0]

    byte_data = block.read(3, 4)
    assert byte_data == "lowo".encode("utf-8")

    byte_data = block.read(1, 7)
    assert byte_data == "ellowor".encode("utf-8")

    _clean_up()


def test_block_rewind_and_write():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    def check_content(filepath, target_content_string):
        with open(filepath, "rb") as f:
            bytes = f.read()
            assert bytes == target_content_string.encode("utf-8")

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    pool_path = os.path.join(pool_folder, "pool_0")

    block = manager.allocate_block(5)
    block.write("hello")
    check_content(pool_path, "00010hello")

    try:
        block.write("hey")
        assert False
    except:
        pass

    block.rewind(2)
    block.write("hey")
    check_content(pool_path, "00010hehey")

    _clean_up()


def test_memory_manager_bootstrap():
    pool_folder, conf_path, block_file = _get_common_file_paths()

    pool_file_path = os.path.join(pool_folder, "pool_0")

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    assert len(manager.blocks) == 1
    assert len(manager.pools) == 1

    pool = manager.pools[0]
    block = manager.blocks[0]

    assert (
        pool.pool_id == 0
        and pool.filepath == pool_file_path
        and pool.pool_allocate_offset == 10
        and pool.pool_allocate_limit == 0
    )
    assert block.block_size == 5 and block.used_memory == 0 and block.free_memory == 5


def test_block_allocate_corner_case():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    for i in range(95):
        manager.allocate_block(1)

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    assert len(manager.blocks) == 95
    assert len(manager.pools) == 1
    for block in manager.blocks:
        assert block.used_memory == 0 and block.free_memory == 1

    _clean_up()


def test_block_current_offset():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    block = manager.allocate_block(10)

    block.write("hello")
    assert block.current_offset == 5

    block.write("hey")
    assert block.current_offset == 8

    block.write("a")
    assert block.current_offset == 9

    block.rewind(7)
    assert block.current_offset == 7

    _clean_up()


def test_block_dict():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    for i in range(10):
        manager.allocate_block(1)

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    assert len(manager.block_dict) == 10
    for i in range(10):
        assert i in manager.block_dict

    _clean_up()


def test_pool_dict():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    manager.allocate_block(100)

    assert len(manager.pool_dict) == 20
    for i in range(20):
        assert i in manager.pool_dict

    _clean_up()


def test_read_write_multi_blocks():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    _clean_up()

    pool_file_1 = os.path.join(pool_folder, "pool_0")
    pool_file_2 = os.path.join(pool_folder, "pool_1")

    def check_content(filepath, target_string):
        with open(filepath, "rb") as f:
            code = f.read()
            assert code == target_string.encode("utf-8")

    manager = MemoryManager(
        pool_folder=pool_folder, conf_path=conf_path, block_file=block_file
    )
    block1 = manager.allocate_block(6)
    block2 = manager.allocate_block(4)

    block1.write("hello")
    block2.write("hey")

    check_content(pool_file_1, "00010hello")
    check_content(pool_file_2, "000100hey0")

    assert block1.read(1, 4) == "ello".encode("utf-8")
    assert block2.read(2, 2) == "y0".encode("utf-8")
    assert block1.read(3, 2) == "lo".encode("utf-8")
    assert block2.read(0, 2) == "he".encode("utf-8")

    _clean_up()


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
        os.path.join(package_root_path, "storage", check_name, "pools")
    )
    conf_path = os.path.abspath(
        os.path.join(package_root_path, "storage", check_name, "storage_conf.ini")
    )
    block_file = os.path.abspath(
        os.path.join(package_root_path, "storage", check_name, "block_file")
    )
    return pool_folder, conf_path, block_file


def _clean_up():
    pool_folder, conf_path, block_file = _get_common_file_paths()
    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)
