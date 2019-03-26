from kv_index import KVIndex
from memory_manager import MemoryManager


class BTreeIndex(KVIndex):
    """Tree node structure

              node1     -    key1    -   node2
            /   |   \                  /   |   \
       node1 - key1 - node2       node1 - key1 - node2
    """

    def __init__(self, memory_manager, btree_rank=5):
        self._root = BTreeNode()
        self._btree_rank = btree_rank

    def set(self, key, value):
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
        current.size += 1
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
                    return head.value
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
            predecessor_key_node = self._find_predecessor_key_node(key_node)
            current = predecessor_key_node.prev.current_btree_node
            # replace current key_value with predecessor
            key_node.key = predecessor_key_node.key
            key_node.value = predecessor_key_node.value
            # delete predecessor key_node
            predecessor_key_node.prev.next = predecessor_key_node.next.next
            predecessor_key_node.prev.next.prev = predecessor_key_node.prev
            current.refresh()
            # re-balance layer by layer
            threshold = (self._btree_rank + 1) // 2 - 1
            while current.size < threshold:
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
                        current.add_key_ahead(left_key_node, tree_list_node)
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
                        current.append_key(right_key_node, tree_list_node)
                        break
                # TODO then merge current node with sibling node


                pass
        else:
            return False

    def keys(self):
        return map(lambda pair: pair[0], self.key_value_pairs())

    def key_value_pairs(self):
        stack, next_layer = [], []
        current = self._root.list_head
        while current:
            next_layer.append(current.next)
            current = current.next.next
        next_layer.reverse()
        stack.extend(next_layer)
        while stack:
            current = stack.pop()
            if not current.next_btree_node:
                # otherwise it is a dummy ListNode
                if isinstance(current.prev, KeyListNode):
                    yield current.prev.key, current.prev.value
            else:
                current = current.next_btree_node.list_head
                next_layer = []
                while current:
                    next_layer.append(current.next)
                    current = current.next.next
                next_layer.reverse()
                stack.extend(next_layer)

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
        node.prev.next = node.next.prev = None
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
        return current.prev


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

class ListNode(object):
    """Doubly Linked List Node"""

    def __init__(self, prev=None, next=None):
        self.prev = prev
        self.next = next


class KeyListNode(ListNode):
    def __init__(self, key, value, prev=None, next=None):
        super().__init__(prev, next)
        self.key = key
        # TODO data should be persisted in disk
        self.value = value


class TreeListNode(ListNode):
    def __init__(self, current_btree_node, next_btree_node=None, prev=None, next=None):
        super().__init__(prev, next)
        self.current_btree_node = current_btree_node
        self.next_btree_node = next_btree_node
