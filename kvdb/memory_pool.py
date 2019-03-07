import mmap
import os
import re
import configparser

from memory_segment import MemorySegment


class MemoryPool(object):
    """
    Map a real file to a memory pool, use mmap
    """

    def __init__(self, filepath, conf):
        # load pool common const values
        self.__conf = conf
        if not conf:
            self.__conf = configparser.ConfigParser()
            self.__conf.read(os.path.join(os.path.dirname(__file__), 'conf', 'storage_conf.ini'))
        # cache some frequent used consts
        self.__pool_allocate_offset_header = int(self.__conf['MEMORY_POOL']['POOL_ALLOCATE_OFFSET_HEADER'])
        self.__pool_size = int(self.__conf['MEMORY_POOL']['POOL_SIZE'])
        assert self.__pool_size > self.__pool_allocate_offset_header, 'pool size should be greater than pool header size'
        # pool metadata
        self.__filepath = filepath
        self.__id = self.__extract_pool_id_from_filepath()
        # if file is not exists or it is empty
        if not os.path.exists(filepath) or os.stat(filepath).st_size == 0:
            # initialize memory pool file with 0 as placeholder
            with open(filepath, 'wb') as f:
                place_holder = '%0{}d'.format(self.__pool_allocate_offset_header) % self.__pool_allocate_offset_header
                place_holder += ('0' * (self.__pool_size - self.__pool_allocate_offset_header))
                f.write(place_holder.encode('utf-8'))
            self.__pool_allocate_offset = self.__pool_allocate_offset_header
        else:
            with open(filepath, 'rb') as f:
                bytes = int(f.read(self.__pool_allocate_offset_header))
                self.__pool_allocate_offset = bytes
        # open mmap
        self.__mmap_object = mmap.mmap(os.open(filepath, os.O_RDWR), 0)

    @property
    def pool_id(self):
        return self.__id

    @property
    def filepath(self):
        return self.__filepath

    @property
    def pool_allocate_offset(self):
        return self.__pool_allocate_offset

    @property
    def pool_allocate_limit(self):
        return self.__pool_size - self.__pool_allocate_offset

    def allocate(self, size):
        assert size > 0, 'memory allocated should be greater than zero'
        assert self.__pool_allocate_offset + size <= self.__pool_size
        segment = MemorySegment(self, self.__pool_allocate_offset, self.__pool_allocate_offset + size, size)
        self.__pool_allocate_offset += size
        self.__mmap_object.seek(0)
        self.__mmap_object.write(('%0{}d'.format(self.__pool_allocate_offset_header) % self.__pool_allocate_offset).encode('utf-8'))
        return segment

    def write(self, offset, byte_data):
        assert isinstance(byte_data, bytes)
        assert offset >= self.__pool_allocate_offset_header and \
               offset + len(byte_data) <= self.__pool_allocate_offset

        self.__mmap_object.seek(offset)
        self.__mmap_object.write(byte_data)

    def read(self, offset, length, skip_header=True):
        if skip_header:
            offset += self.__pool_allocate_offset_header
        assert offset >= self.__pool_allocate_offset_header and \
               offset < self.__pool_allocate_offset, 'Legal read offset range is (%d -> %d), given offset is %d' \
                    .format(self.__pool_allocate_offset_header, self.__pool_allocate_offset - 1, offset)
        self.__mmap_object.seek(offset)
        limit = self.__pool_allocate_offset - offset
        return self.__mmap_object.read(min(length, limit))

    def close(self):
        """
        close mmap object, remove file in disk
        """
        self.__mmap_object.close()
        os.remove(self.__filepath)

    def __extract_pool_id_from_filepath(self):
        match_result = re.search('pool_(\d+)', self.__filepath)
        if match_result:
            return int(match_result.group(1))
        else:
            return - 1

    def __str__(self):
        return 'filepath is: {}, pool allocate offset: {}, pool size: {}'.format(self.__filepath, self.__pool_allocate_offset, self.__pool_size)

    def __repr__(self):
        return self.__str__()

    def __getstate__(self):
        current_state = self.__dict__.copy()
        # exclude mmap in pickle
        del current_state['_MemoryPool__mmap_object']
        return current_state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # construct mmap again
        self.__dict__['_MemoryPool__mmap_object'] = mmap.mmap(os.open(state['_MemoryPool__filepath'], os.O_RDWR), 0)