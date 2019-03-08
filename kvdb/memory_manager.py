import os
import re
import configparser
import pickle


from memory_pool import MemoryPool
from memory_block import MemoryBlock


class MemoryManager(object):
    """
    manage all memory storage
    """

    def __init__(self, conf_path=None, pool_folder=None, block_file=None):
        self._conf = configparser.ConfigParser()
        conf_path = conf_path or os.path.join(
            os.path.dirname(__file__), "conf", "storage_conf.ini"
        )
        self._conf.read(conf_path)
        self._pool_folder = pool_folder or os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), self._conf["MEMORY_MANAGER"]["POOL_FOLDER"]
            )
        )
        self._block_file = block_file or os.path.abspath(
            os.path.join(
                os.path.dirname(__file__), self._conf["MEMORY_MANAGER"]["BLOCK_FILE"]
            )
        )
        self._block_header_length = (
            int(self._conf.get("MEMORY_MANAGER", "BLOCK_HEADER_LENGTH", fallback=0))
            or 10
        )
        # initialize memory manager
        self._bootstrap()

    @property
    def pools(self):
        return self._pool_list

    @property
    def blocks(self):
        return self._block_list

    @property
    def pool_dict(self):
        return self._pool_dict

    @property
    def block_dict(self):
        return self._block_dict

    def allocate_block(self, block_size):
        total_size = block_size
        memory_segments = []
        while total_size > 0:
            # last pool is empty
            if not self._pool_list or self._pool_list[-1].pool_allocate_limit == 0:
                self._allocate_new_pool()
            # if current pool is big enough
            if total_size <= self._pool_list[-1].pool_allocate_limit:
                memory_segments.append(self._pool_list[-1].allocate(total_size))
                break
            else:
                allocate_size = self._pool_list[-1].pool_allocate_limit
                memory_segments.append(self._pool_list[-1].allocate(allocate_size))
                self._allocate_new_pool()
                total_size -= allocate_size

        # construct memory block and persist to disk
        block_f = open(self._block_file, "ab")
        block = MemoryBlock(self._next_block_id, block_size, memory_segments)
        self._block_list.append(block)
        self._next_block_id += 1
        string = pickle.dumps(block)
        # TODO without compression now
        output_bytearray = bytearray()
        output_bytearray.extend(
            (("%0{}d".format(self._block_header_length)) % len(string)).encode("utf-8")
        )
        output_bytearray.extend(string)
        block_f.write(bytes(output_bytearray))
        block_f.close()

        return block

    def _allocate_new_pool(self):
        pool_path = os.path.abspath(
            os.path.join(self._pool_folder, "pool_{}".format(self._next_pool_id))
        )
        pool = MemoryPool(pool_path, self._conf)
        self._pool_list.append(pool)
        self._next_pool_id += 1
        self._current_pool_index = len(self._pool_list) - 1

    def _bootstrap(self):
        # check if `pools` folder exists
        if not os.path.exists(self._pool_folder):
            os.mkdir(self._pool_folder)

        # load all pools
        self._pool_list = []
        self._pool_dict = {}
        max_pool_id = -1
        not_full_pool = None
        for path in os.listdir(self._pool_folder):
            absolute_path = os.path.abspath(os.path.join(self._pool_folder, path))
            match_result = re.search("pool_(\d+)", path)
            if match_result:
                pool_id = int(match_result.group(1))
                pool = MemoryPool(absolute_path, self._conf)
                # if current pool could be used to allocate memory
                if pool.pool_allocate_limit > 0:
                    assert not_full_pool is None
                    not_full_pool = pool
                else:
                    self._pool_list.append(pool)
                max_pool_id = max(max_pool_id, pool_id)
        # update next available pool id
        self._next_pool_id = max_pool_id + 1
        # if one of the pool is not full
        if not_full_pool:
            self._pool_list.append(not_full_pool)
            self._current_pool_index = len(self._pool_list) - 1
        for pool in self._pool_list:
            self._pool_dict[pool.pool_id] = pool

        # check if `block file` exists
        if not os.path.exists(self._block_file):
            open(self._block_file, "a").close()

        self._block_list = []
        self._block_dict = {}
        self._next_block_id = 0
        block_f = open(self._block_file, "rb")
        bytes = block_f.read()
        index, length = 0, len(bytes)
        while index < length:
            data_length = int(bytes[index : index + self._block_header_length])
            data = bytes[
                index
                + self._block_header_length : index
                + self._block_header_length
                + data_length
            ]
            block = pickle.loads(data)
            self._block_list.append(block)
            self._block_dict[block.block_id] = block
            index += self._block_header_length + data_length
            self._next_block_id = max(block.block_id + 1, self._next_block_id)
        block_f.close()
