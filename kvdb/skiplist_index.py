from kv_index import KVIndex
import random
import pickle


class SkipListIndex(KVIndex):
    def __init__(self, memory_manager):
        self._memory_manager = memory_manager
        self._current_block = None
        self._value_header_length = (
            int(
                self._memory_manager.conf.get(
                    "SKIPLIST_INDEX", "VALUE_HEADER_LENGTH", fallback=0
                )
            )
            or 10
        )
        self._memory_allocate_scale = (
            int(
                self._memory_manager.conf.get(
                    "SKIPLIST_INDEX", "MEMORY_ALLOCATE_SCALE", fallback=0
                )
            )
            or 10
        )
        # dummy heads
        self._heads = [SkipListNode(key=-1, value=-1)]

    def set(self, key, value):
        node_value = self._persist_value(value)
        predecessors = []
        current = self._heads[-1]
        while current:
            while current.right and current.right.key < key:
                current = current.right
            predecessors.append(current)
            current = current.down
        predecessors.reverse()
        # key exists in the bottom list
        if predecessors[0].right and predecessors[0].right.key == key:
            for predecessor in predecessors:
                if predecessor.right and predecessor.right.key == key:
                    predecessor.right.value = node_value
                else:
                    break
        else:
            # insert a new list node
            max_level = self._random_level()
            previous_node = None
            for level in range(max_level + 1):
                new_node = SkipListNode(key, node_value)
                if level < len(predecessors):
                    new_node.right = predecessors[level].right
                    predecessors[level].right = new_node
                else:
                    self._heads.append(SkipListNode(-1, -1))
                    self._heads[-1].right = new_node
                    self._heads[-1].down = self._heads[-2]
                if previous_node:
                    new_node.down = previous_node
                previous_node = new_node

    def get(self, key, default=None):
        current = self._heads[-1]
        while current:
            while current.right and current.right.key < key:
                current = current.right
            if current.right and current.right.key == key:
                return self._load_value(current.right.value)
            else:
                current = current.down
        return default

    def remove(self, key):
        current, predecessors, result = self._heads[-1], [], False
        while current:
            while current.right and current.right.key < key:
                current = current.right
            predecessors.append(current)
            current = current.down
        for predecessor in predecessors:
            if predecessor.right and predecessor.right.key == key:
                predecessor.right = predecessor.right.right
                result = True
            # we need to check top list only, and we need to guarantee
            # we have at least one list
            if self._heads[-1].right is None and len(self._heads) > 1:
                self._heads.pop()
        return result

    def keys(self):
        current, keys = self._heads[0], []
        while current.right:
            keys.append(current.right.key)
            current = current.right
        return keys

    def key_value_pairs(self):
        current, pairs = self._heads[0], []
        while current.right:
            pairs.append((current.right.key, self._load_value(current.right.value)))
            current = current.right
        return pairs

    def clear(self):
        self._heads = [SkipListNode(key=-1, value=-1)]

    def height(self):
        return len(self._heads)

    def _random_level(self):
        level = 0
        while random.random() <= 0.5:
            level += 1
        return level

    def _persist_value(self, value):
        """persist value to disk"""
        string = pickle.dumps(value)
        length = len(string)

        byte_array = bytearray()
        byte_array.extend(
            ("%0{}d".format(self._value_header_length) % length).encode("utf-8")
        )
        byte_array.extend(string)
        # memory is exhausted
        if not self._current_block or (
            self._current_block.free_memory < len(byte_array)
        ):
            self._current_block = self._memory_manager.allocate_block(
                len(byte_array) * self._memory_allocate_scale
            )

        block_id, address = (
            self._current_block.block_id,
            self._current_block.current_offset,
        )
        write_bytes = self._current_block.write(bytes(byte_array))

        assert write_bytes == len(byte_array)

        return SkipListNodeValue(block_id, address)

    def _load_value(self, node_value):
        """load value from disk"""
        assert isinstance(node_value, SkipListNodeValue)
        block = self._memory_manager.block_dict[node_value.block_id]
        length = int(block.read(node_value.address, self._value_header_length))
        data = block.read(node_value.address + self._value_header_length, length)
        value = pickle.loads(data)
        return value


class SkipListNode(object):
    """
    head2    ->    n2    ->    n4    n4 is tail2
    head1 -> n1 -> n2 -> n3 -> n4    n4 is tail1
    """

    def __init__(self, key, value, right=None, down=None):
        self.key = key
        self.value = value
        self.right = right
        self.down = down

    def __str__(self):
        return "({}, {})".format(self.key, self.value)

    # for debugging
    def print_right_list(self):
        result, node = [], self
        while node:
            result.append(str(node))
            node = node.right
        print(", ".join(result))

    def print_down_list(self):
        result, node = [], self
        while node:
            result.append(str(node))
            node = node.down
        print(", ".join(result))


class SkipListNodeValue(object):
    def __init__(self, block_id, address):
        self.block_id = block_id
        self.address = address

    def __str__(self):
        return "({}, {})".format(self.block_id, self.address)
