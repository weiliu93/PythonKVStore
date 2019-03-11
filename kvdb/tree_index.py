from collections import deque
import pickle

from kv_index import KVIndex


class TreeIndex(KVIndex):
    def __init__(self, memory_manager):
        self._memory_manager = memory_manager
        self._current_block = None
        self._root = None
        self._value_header_length = (
            int(self._memory_manager.conf.get("TREE_INDEX", "VALUE_HEADER_LENGTH", fallback=0))
            or 10
        )
        self._memory_allocate_scale = (
            int(self._memory_manager.conf.get("TREE_INDEX", "MEMORY_ALLOCATE_SCALE", fallback=0))
            or 10
        )
        self._index_history = []

    def set(self, key, value):
        node = self._set_traverse(self._root, key, value)
        self._update_node(node)

    def _set_traverse(self, node, key, value):
        if node:
            if node.key == key:
                new_node = TreeNode(key, value, left=node.left, right=node.right)
                return new_node
            else:
                if key < node.key:
                    new_node = TreeNode(
                        node.key,
                        node.value,
                        self._set_traverse(node.left, key, value),
                        node.right,
                    )
                else:
                    new_node = TreeNode(
                        node.key,
                        node.value,
                        node.left,
                        self._set_traverse(node.right, key, value),
                    )
                return new_node
        else:
            return TreeNode(key, value)

    def get(self, key, default=None):
        node = self._root
        while node:
            if node.key == key:
                # load value from disk or memory
                if isinstance(node.value, TreeValue):
                    return self._load_value_from_disk(node.value)
                else:
                    return node.value
            elif node.key < key:
                node = node.right
            else:
                node = node.left
        return default

    def persist(self):
        """Just update last valid tree inplace, return how many values have been persisted"""
        return self._persist_traverse()

    def _persist_traverse(self):
        queue = deque([self._root] if self._root else [])
        total = 0
        while queue:
            node = queue.popleft()
            # if we need to persist current node's value, create a new tree node
            if not isinstance(node.value, TreeValue):
                node.value = self._persist_value_to_disk(node.value)
                total += 1
            if node.left:
                queue.append(node.left)
            if node.right:
                queue.append(node.right)
        return total

    def checkout(self, version=None, backoff=None):
        if version is not None:
            return self._checkout_version(version)
        if backoff is not None:
            assert backoff >= 0
            return self._checkout_version(len(self._index_history) - backoff - 1)
        raise Exception(
            "You need to specify a version number or backoff value before checkout a previous index snapshot"
        )

    def keys(self):
        """collect all keys with iterative in-order traversal"""
        keys, stack = [], []
        node = self._root
        while node:
            stack.append(node)
            node = node.left
        while stack:
            node = stack.pop()
            keys.append(node.key)
            node = node.right
            while node:
                stack.append(node)
                node = node.left
        return keys

    def key_value_pairs(self):
        """collect all key-value pairs with iterative in-order traversal"""
        pairs, stack = [], []
        node = self._root
        while node:
            stack.append(node)
            node = node.left
        while stack:
            node = stack.pop()
            if isinstance(node.value, TreeValue):
                pairs.append(
                    (node.key, self._load_value_from_disk(node.value))
                )
            else:
                pairs.append((node.key, node.value))
            node = node.right
            while node:
                stack.append(node)
                node = node.left
        return pairs

    def _checkout_version(self, version):
        assert version >= 0 and version < len(self._index_history)
        index = TreeIndex(self._memory_manager)
        # copy node to index
        for node in self._index_history[0 : version + 1]:
            index._index_history.append(node)
        index._root = index._index_history[-1] if index._index_history else None
        return index

    def clear(self):
        self._root = None

    def _persist_value_to_disk(self, value):
        """value -> TreeValue"""
        value_string = pickle.dumps(value)
        length = len(value_string)

        byte_array = bytearray()
        byte_array.extend(
            ("%0{}d".format(self._value_header_length) % length).encode("utf-8")
        )
        byte_array.extend(value_string)

        # current block's capacity is not enough
        if (
            self._current_block is None
            or len(byte_array) > self._current_block.free_memory
        ):
            self._current_block = self._memory_manager.allocate_block(
                len(byte_array) * self._memory_allocate_scale
            )

        block_id, address = (
            self._current_block.block_id,
            self._current_block.current_offset,
        )
        self._current_block.write(bytes(byte_array))
        return TreeValue(block_id, address)

    def _load_value_from_disk(self, tree_value):
        """tree_value -> original object"""
        assert isinstance(tree_value, TreeValue)
        block = self._memory_manager.block_dict[tree_value.block_id]
        # fetch object length first
        length = int(block.read(tree_value.address, self._value_header_length))
        # then fetch object directly
        bytes = block.read(tree_value.address + self._value_header_length, length)
        return pickle.loads(bytes)

    def _update_node(self, node):
        if node != self._root:
            self._index_history.append(node)
            self._root = node


class TreeNode(object):
    def __init__(self, key, value, left=None, right=None):
        self.left = left
        self.right = right
        self.key = key
        self.value = value

    def __str__(self):
        return "key: {}, value: {}, memory address: {}".format(
            self.key, self.value, id(self)
        )

    def __repr__(self):
        return self.__str__()


class TreeValue(object):
    def __init__(self, block_id, address):
        self.block_id = block_id
        self.address = address

    def __str__(self):
        return "block id: {}, address: {}".format(self.block_id, self.address)

    def __repr__(self):
        return self.__str__()
