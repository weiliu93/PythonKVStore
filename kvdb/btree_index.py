from kv_index import KVIndex
from memory_manager import MemoryManager

import pickle


class BTreeIndex(KVIndex):
    """Tree node structure

              node1     -    key1    -   node2
            /   |   \                  /   |   \
       node1 - key1 - node2       node1 - key1 - node2
    """

    def __init__(self, memory_manager, btree_rank=5):
        self._root = BTreeNode()
        self._btree_rank = btree_rank
        self._memory_manager = memory_manager

    def set(self, key, value):
        # turn object into persistence storage
        value = TreeValue.from_value(value, self._memory_manager)
        current, current_list_node = self._root, None
        # current BTree node
        while current:
            # key node always in even positions
            key_node, prev_list_node = (
                current.list_head.next.next,
                current.list_head.next,
            )
            while key_node and key_node.key < key:
                prev_list_node = prev_list_node.next.next
                key_node = key_node.next.next
            if key_node and key_node.key == key:
                key_node.value = value
                return
            else:
                # jump to next BTree node
                if prev_list_node.next_btree_node:
                    current = prev_list_node.next_btree_node
                else:
                    # jump to TreeListNode
                    current_list_node = prev_list_node
                    break
        # insert key-value in current TreeListNode
        key_node = KeyListNode(key, value)
        left_tree_list_node, right_tree_list_node = (
            TreeListNode(current),
            TreeListNode(current),
        )
        self._replace_next_node_with_nodes(
            current_list_node.prev,
            [left_tree_list_node, key_node, right_tree_list_node],
        )
        current.refresh()
        # start to split when necessary
        while current.size == self._btree_rank:
            parent_tree_list_node = current.parent_tree_list_node
            parent_btree_node = (
                parent_tree_list_node.current_btree_node
                if parent_tree_list_node
                else None
            )
            left_btree_root_node, (key, value), right_btree_root_node = self._split(
                current
            )
            left_tree_list_node, right_tree_list_node = (
                TreeListNode(parent_btree_node, next_btree_node=left_btree_root_node),
                TreeListNode(parent_btree_node, next_btree_node=right_btree_root_node),
            )
            left_btree_root_node.parent_tree_list_node = left_tree_list_node
            right_btree_root_node.parent_tree_list_node = right_tree_list_node
            key_node = KeyListNode(key=key, value=value)
            # if parent node exists
            if parent_tree_list_node:
                self._replace_next_node_with_nodes(
                    parent_tree_list_node.prev,
                    [left_tree_list_node, key_node, right_tree_list_node],
                )
                current = parent_btree_node
                # since we swim a key to parent node
                current.size += 1
            else:
                # need to create a new root
                root = BTreeNode(size=1)
                self._replace_next_node_with_nodes(
                    root.list_head,
                    [left_tree_list_node, key_node, right_tree_list_node],
                )
                self._root = root
                self._root.refresh()
                break

    def get(self, key, default=None):
        current = self._root
        while current:
            head, prev_list_node = current.list_head.next.next, current.list_head.next
            while head:
                if head.key == key:
                    return head.value.load_value(self._memory_manager)
                elif head.key > key:
                    break
                head = head.next.next
                prev_list_node = prev_list_node.next.next
            current = prev_list_node.next_btree_node
        return default

    def remove(self, key):
        btree_node = self._find_btree_node_with_given_key(key)
        if btree_node:
            key_node = btree_node.find_key_node(key)
            # use predecessor first
            predecessor_key_node = self._find_predecessor_key_node(key_node)
            if predecessor_key_node:
                current = predecessor_key_node.prev.current_btree_node
                # replace current key_value with predecessor
                key_node.key = predecessor_key_node.key
                key_node.value = predecessor_key_node.value
                # delete predecessor key_node
                predecessor_key_node.prev.next = predecessor_key_node.next.next
                if predecessor_key_node.prev.next:
                    predecessor_key_node.prev.next.prev = predecessor_key_node.prev
            else:
                # then use successor
                successor_key_node = self._find_successor_key_node(key_node)
                if successor_key_node:
                    current = successor_key_node.prev.current_btree_node
                    # replace current key_value with successor
                    key_node.key, key_node.value = successor_key_node.key, successor_key_node.value
                    # delete successor key_node
                    successor_key_node.prev.next = successor_key_node.next.next
                    if successor_key_node.prev.next:
                        successor_key_node.prev.next.prev = successor_key_node.prev
                else:
                    # current btree node become empty, no predecessors or successors found
                    key_node.prev.next = None
                    current = key_node.prev.current_btree_node
            current.refresh()
            # re-balance layer by layer
            threshold = (self._btree_rank + 1) // 2 - 1
            while not current.is_root() and current.size < threshold:
                # try to steal key from left sibling
                left_sibling = current.left_sibling_btree_node()
                if left_sibling:
                    left_key_node = current.parent_tree_list_node.prev
                    if left_sibling.size > threshold:
                        key_node, tree_list_node = left_sibling.pop_last_key()
                        # swap key_node and left_key_node
                        left_key_node.key, key_node.key = key_node.key, left_key_node.key
                        left_key_node.value, key_node.value = key_node.value, left_key_node.value
                        # add key_node and tree_list_node ahead
                        current.add_key_ahead(key_node, tree_list_node)
                        break
                # try to steal key from right sibling
                right_sibling = current.right_sibling_btree_node()
                if right_sibling:
                    right_key_node = current.parent_tree_list_node.next
                    if right_sibling.size > threshold:
                        key_node, tree_list_node = right_sibling.pop_first_key()
                        # swap key_node and right_key_node
                        right_key_node.key, key_node.key = key_node.key, right_key_node.key
                        right_key_node.value, key_node.value = key_node.value, right_key_node.value
                        # append key_node and tree_list_node
                        current.append_key(key_node, tree_list_node)
                        break
                # merge with left sibling
                if left_sibling:
                    left_key_node = current.parent_tree_list_node.prev
                    parent_bree_node = current.parent_tree_list_node.current_btree_node
                    left_key_node.prev.next = left_key_node.next.next
                    if left_key_node.prev.next:
                        left_key_node.prev.next.prev = left_key_node.prev
                    left_sibling.merge(left_key_node, current)
                    parent_bree_node.refresh()
                    left_sibling.refresh()
                    # if root is empty
                    if parent_bree_node.size == 0:
                        left_sibling.parent_tree_list_node = None
                        self._root = left_sibling
                        current = left_sibling
                    # jump to parent node
                    else:
                        current = parent_bree_node
                # merge with right sibling
                elif right_sibling:
                    right_key_node = current.parent_tree_list_node.next
                    parent_bree_node = current.parent_tree_list_node.current_btree_node
                    right_key_node.prev.next = right_key_node.next.next
                    if right_key_node.prev.next:
                        right_key_node.prev.next.prev = right_key_node.prev
                    current.merge(right_key_node, right_sibling)
                    parent_bree_node.refresh()
                    current.refresh()
                    # if root is empty
                    if parent_bree_node.size == 0:
                        current.parent_tree_list_node = None
                        self._root = current
                    # jump to parent node
                    else:
                        current = parent_bree_node
                else:
                    raise Exception("No siblings could be merged")
            return True
        else:
            return False

    def keys(self):
        return map(lambda pair: pair[0], self.key_value_pairs())

    def key_value_pairs(self):
        stack = []
        node = self._root.list_head.next
        while node:
            stack.append(node)
            node = node.next
        stack.reverse()
        while stack:
            next_layer = []
            node = stack.pop()
            if isinstance(node, TreeListNode):
                node = node.next_btree_node
                if node:
                    node = node.list_head.next
                    while node:
                        next_layer.append(node)
                        node = node.next
                    next_layer.reverse()
                    stack.extend(next_layer)
            else:
                yield node.key, node.value.load_value(self._memory_manager)

    def clear(self):
        self._root = BTreeNode()

    def _split(self, btree_node):
        # split btree node into `left_btree_root_node`, `(key, value)`, `right_btree_root_node`
        # find pivot key_node
        index = (btree_node.size + 1) // 2
        node = btree_node.list_head.next.next
        while index > 1:
            node = node.next.next
            index -= 1
        assert isinstance(node, KeyListNode)
        key, value = node.key, node.value
        left_list_head = btree_node.list_head
        # add a dummy head for right tree
        right_list_head = ListNode(next=node.next)
        right_list_head.next.prev = right_list_head
        # remove key_node from linked list
        node.prev.next = None
        # build left, right btrees
        left_btree_root = BTreeNode(left_list_head)
        right_btree_root = BTreeNode(right_list_head)
        left_btree_root.refresh()
        right_btree_root.refresh()

        return left_btree_root, (key, value), right_btree_root

    def _replace_next_node_with_nodes(self, list_node, nodes):
        assert list_node.next is not None
        self._remove_list_node(list_node.next)
        for node in nodes:
            self._insert_list_node_after(list_node, node)
            list_node = node

    def _insert_list_node_after(self, node, insert_node):
        insert_node.next = node.next
        insert_node.prev = node
        insert_node.prev.next = insert_node
        if insert_node.next:
            insert_node.next.prev = insert_node

    def _remove_list_node(self, node):
        node.prev.next = node.next
        if node.next:
            node.next.prev = node.prev

    def _find_btree_node_with_given_key(self, key):
        current = self._root
        while current:
            head, prev_list_node = current.list_head.next.next, current.list_head.next
            while head and head.key < key:
                head = head.next.next
                prev_list_node = prev_list_node.next.next
            if head and head.key == key:
                return prev_list_node.current_btree_node
            else:
                current = prev_list_node.next_btree_node
        return None

    def _find_predecessor_key_node(self, key_node):
        current = key_node.prev
        while current:
            if current.next_btree_node:
                current = current.next_btree_node.last_tree_node()
            else:
                break
        # tricky part here, current.prev could be a ListNode
        if isinstance(current.prev, KeyListNode):
            return current.prev
        else:
            return None

    def _find_successor_key_node(self, key_node):
        current = key_node.next
        while current:
            if current.next_btree_node:
                current = current.next_btree_node.first_tree_node()
            else:
                break
        # current.next will return legal key_node or None
        return current.next

class BTreeNode(object):
    def __init__(self, list_head=None, parent_tree_list_node=None, size=0):
        # be careful, list_head has a dummy head
        if list_head:
            self.list_head = list_head
        else:
            self.list_head = ListNode()
            self.list_head.next = TreeListNode(self, prev=self.list_head, next=None)
        self.parent_tree_list_node = parent_tree_list_node
        self.size = size

    def refresh(self):
        """refresh all list node's current_btree_node and btree_node's size"""
        node, ans = self.list_head.next.next, 0
        # first update key_nodes in even positions
        while node:
            ans += 1
            node = node.next.next
        # then update tree_nodes's current_btree_node in odd positions
        node = self.list_head.next
        while node:
            node.current_btree_node = self
            if node.next:
                node = node.next.next
            else:
                break
        self.size = ans

    def is_leaf(self):
        return not self.list_head.next.next_btree_node

    def is_root(self):
        return not self.parent_tree_list_node

    def first_key_node(self):
        return self.list_head.next.next

    def first_tree_node(self):
        return self.first_key_node().prev

    def last_key_node(self):
        node, ans = self.list_head.next.next, None
        while node:
            ans = node
            node = node.next.next
        return ans

    def last_tree_node(self):
        return self.last_key_node().next

    def find_key_node(self, key):
        node = self.list_head.next.next
        while node and node.key != key:
            node = node.next.next
        return node

    def parent_btree_node(self):
        if self.parent_tree_list_node:
            return self.parent_tree_list_node.current_btree_node
        else:
            return None

    def left_sibling_btree_node(self):
        if self.parent_tree_list_node:
            # jump to parent btree node
            node = self.parent_tree_list_node.prev.prev
            if node:
                return node.next_btree_node
        return None

    def right_sibling_btree_node(self):
        if self.parent_tree_list_node and self.parent_tree_list_node.next:
            # jump to parent btree node
            node = self.parent_tree_list_node.next.next
            if node:
                return node.next_btree_node
        return None

    def pop_last_key(self):
        key_node = self.last_key_node()
        tree_list_node = self.last_tree_node()
        key_node.prev.next = None
        # reset all pointers
        key_node.prev = key_node.next = tree_list_node.prev = tree_list_node.next = None
        self.refresh()
        return key_node, tree_list_node

    def pop_first_key(self):
        key_node = self.first_key_node()
        tree_list_node = self.first_tree_node()
        self.list_head.next = key_node.next
        self.list_head.next.prev = self.list_head
        # reset all pointers
        key_node.prev = key_node.next = tree_list_node.prev = tree_list_node.next = None
        self.refresh()
        return key_node, tree_list_node

    def append_key(self, key_node, tree_list_node):
        last_tree_list_node = self.last_tree_node()
        # first key_node, then tree_list_node
        key_node.next = tree_list_node
        tree_list_node.prev = key_node
        last_tree_list_node.next = key_node
        key_node.prev = last_tree_list_node
        tree_list_node.next = None
        self.refresh()

    def add_key_ahead(self, key_node, tree_list_node):
        next_node = self.list_head.next
        # first tree_list_node, then key_node
        tree_list_node.next = key_node
        tree_list_node.next.prev = tree_list_node
        # link tree_list_node with list_head
        tree_list_node.prev = self.list_head
        tree_list_node.prev.next = tree_list_node
        # link key_node with list_head.next
        key_node.next = next_node
        key_node.next.prev = key_node
        self.refresh()

    def merge(self, key_node, another_bree_node):
        last_node = self.last_tree_node()
        # append key_node first
        last_node.next = key_node
        key_node.prev = last_node
        last_node = key_node
        # append all remaining nodes
        node = another_bree_node.list_head.next
        while node:
            last_node.next = node
            node.prev = last_node
            last_node = node
            node = node.next
        last_node.next = None
        self.refresh()

    def __str__(self):
        output_list = []
        node = self.list_head.next
        while node:
            output_list.append(str(node))
            node = node.next
        return "(" + ", ".join(output_list) + ")"

class ListNode(object):
    """Doubly Linked List Node"""

    def __init__(self, prev=None, next=None):
        self.prev = prev
        self.next = next


class KeyListNode(ListNode):
    def __init__(self, key, value, prev=None, next=None):
        super().__init__(prev, next)
        self.key = key
        self.value = value

    def __str__(self):
        return "(key: {}, value: {})".format(self.key, self.value)

class TreeValue(object):
    def __init__(self, block_id, address):
        self.block_id = block_id
        self.address = address

    @staticmethod
    def from_value(value, memory_manager):
        value_string = pickle.dumps(value)
        length = len(value_string)
        value_header_length = int(memory_manager.conf.get("BTREE_INDEX", "VALUE_HEADER_LENGTH"))
        output_string = ("%0{}d".format(value_header_length) % length).encode("utf-8") + value_string
        block = memory_manager.allocate_block(len(output_string))
        block.write(output_string)
        return TreeValue(block.block_id, 0)

    def load_value(self, memory_manager):
        value_header_length = int(memory_manager.conf.get("BTREE_INDEX", "VALUE_HEADER_LENGTH"))
        block = memory_manager.block_dict[self.block_id]
        length = int(block.read(self.address, value_header_length))
        byte_string = block.read(self.address + value_header_length, length)
        return pickle.loads(byte_string)

    def __str__(self):
        return "(block_id: {}, address: {})".format(self.block_id, self.address)

class TreeListNode(ListNode):
    def __init__(self, current_btree_node, next_btree_node=None, prev=None, next=None):
        super().__init__(prev, next)
        self.current_btree_node = current_btree_node
        self.next_btree_node = next_btree_node
