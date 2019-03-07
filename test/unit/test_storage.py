import sys
import os
import shutil

sys.path.append(
    os.path.join(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir), "kvdb"
    )
)

from memory_manager import MemoryManager

package_root_path = os.path.abspath(os.path.join(os.path.join(os.path.dirname(__file__), os.pardir), 'unit-packages'))


def test_memory_manager_allocate_block():
    pool_folder = os.path.abspath(os.path.join(package_root_path, 'storage', 'test_memory_manager_allocate_block_pools'))
    conf_path = os.path.abspath(os.path.join(package_root_path, 'storage', 'test_memory_manager_allocate_block', 'storage_conf.ini'))
    block_file = os.path.abspath(os.path.join(package_root_path, 'storage', 'test_memory_manager_allocate_block', 'block_file'))

    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)

    manager = MemoryManager(conf_path = conf_path, pool_folder = pool_folder, block_file=block_file)

    block = manager.allocate_block(8)
    assert block.used_memory == 0 and block.free_memory == 8 and block.block_size == 8

    # check files
    file_paths = sorted(list(os.listdir(pool_folder)))
    assert file_paths == ['pool_0', 'pool_1']

    def check_content(file_path, target_content_string):
        with open(file_path, 'rb') as f:
            bytes = f.read()
            assert bytes == target_content_string.encode('utf-8')
    # check content
    check_content(os.path.join(pool_folder, 'pool_0'), '0001000000')
    check_content(os.path.join(pool_folder, 'pool_1'), '0000800000')

    manager = MemoryManager(conf_path = conf_path, pool_folder = pool_folder, block_file = block_file)
    assert len(manager.pools) == 2
    assert len(manager.blocks) == 1

    block = manager.blocks[0]
    assert block.used_memory == 0 and block.free_memory == 8 and block.block_size == 8

    pools = manager.pools
    pools.sort(key = lambda pool: pool.pool_id)
    assert pools[0].filepath == os.path.join(pool_folder, 'pool_0')
    assert pools[1].filepath == os.path.join(pool_folder, 'pool_1')

    assert pools[0].pool_allocate_limit == 0
    assert pools[1].pool_allocate_limit == 2

    if os.path.exists(pool_folder):
        shutil.rmtree(pool_folder)
    if os.path.exists(block_file):
        os.remove(block_file)