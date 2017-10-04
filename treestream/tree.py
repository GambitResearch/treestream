from __future__ import absolute_import

from functools import wraps
from threading import RLock

from treestream.utils import trunc_str

def synchronise(func):
	@wraps(func)
	def wrapped_func(self, *args, **kwargs):
		with self.lock:
			return func(self, *args, **kwargs)
	return wrapped_func

class Tree(object):
	"""A mutable tree (represented recursively), with labeled edges, where each leaf
	optionally holds a value.

		children: { (edge_label: str) -> (tree: Tree) }
		value: str
	"""

	__slots__ = ('children', 'value', 'lock')

	def __init__(self, children=None, value=None, lock=None):
		self.children = children or {}
		self.value = value
		self.lock = RLock() if lock is None else lock
		assert not self.children or self.value is None

	@classmethod
	def from_adjacency_dict_and_values(cls, adjacency_dict, values, root_ptr):
		"""
			Params:
				adjacency_dict: { (node_ptr: T) -> {edge_label -> (child_ptr: T)} }
				values: { (node_ptr: T) -> value }
				root_ptr: T

			Example:
				Tree.from_adjacency_dict_and_values(
					adjacency_dict={
						'.': {'x': '.x', 'y': '.y'},
						'.x': {'z': '.x.z'},
					},
					values={
						'.x.z': 'foo',
						'.y': 'bar'
					},
					root_ptr='.',
				).inline_str() == "(x=(z=|'foo'|), y=|'bar'|)"
		"""
		used_node_ptrs = set()
		tree = cls._from_adjacency_dict_and_values(
			adjacency_dict,
			values,
			root_ptr,
			used_node_ptrs)

		unused_adjacency_lists = adjacency_dict.viewkeys() - used_node_ptrs
		assert not unused_adjacency_lists, (
			'Adjacency list defined for some unreachable nodes',
			unused_adjacency_lists)

		unused_values = values.viewkeys() - used_node_ptrs
		assert not unused_values, (
			'Values defined for some unreachable nodes',
			unused_values)

		return tree

	@classmethod
	def _from_adjacency_dict_and_values(cls, adjacency_dict, values, root_ptr, used_node_ptrs):
		used_node_ptrs.add(root_ptr)
		direct_arcs = adjacency_dict.get(root_ptr, None)
		children = {}
		if direct_arcs is not None:
			for arc_label, child_ptr in direct_arcs.iteritems():
				child = cls._from_adjacency_dict_and_values(
					adjacency_dict,
					values,
					child_ptr,
					used_node_ptrs)
				# No point in adding empty leaves, since all the interesting information is
				# contained in leaf values
				if child.children or child.value is not None:
					children[arc_label] = child

		return cls(
			children=children,
			value=values.get(root_ptr, None)
		)


	def __eq__(self, other):
		raise NotImplementedError

	def __hash__(self):
		raise NotImplementedError

	def __cmp__(self, other):
		raise NotImplementedError

	@synchronise
	def pretty_print(self, depth=0, width=80):
		padding = '| ' * depth

		for arc_name, child_tree in self.children.items():
			node_part = padding + '\\ ' + arc_name
			value_part = trunc_str(repr(child_tree.value), 16) \
				if child_tree.value is not None else ''

			separator = ' ' * max(1, width - len(node_part))
			print node_part + separator + value_part

			child_tree.pretty_print(depth=depth + 1, width=width)

	def inline_str(self):
		children_str = ', '.join((
			str(edge_label) + '=' + child.inline_str()
			for edge_label, child in sorted(self.children.items())
		))
		if self.value is not None and children_str:
			return '|%r|(%s)' % (self.value, children_str)
		elif self.value is not None:
			return '|%r|' % (self.value,)
		elif children_str:
			return '(%s)' % (children_str,)
		else:
			return '-'

	@synchronise
	def apply_update(self, tree_path, value):
		"""Updates the value at `tree_path`.
		Returns:
			the overwritten value.
		Thread-safe: Yes
		"""
		parent = None
		current = self

		for edge_label in tree_path:
			parent, current = current, current.children.get(edge_label, None)
			if current is None:
				current = parent.children[edge_label] = Tree(lock=self.lock)

		if current.children:
			raise KeyError('Internal nodes cannot hold values')
		old = current.value
		current.value = value
		return old

	@synchronise
	def apply_delete(self, tree_path):
		"""Deletes the subtree at `tree_path`.
		Returns:
			the deleted subtree.
		Thread-safe: Yes
		"""
		trace = []
		current = self

		# Traverse `tree_path` downwards
		for edge_label in tree_path:
			if current is None:
				return
			trace.append(current)
			current = current.children.get(edge_label, None)

		if not trace: # Delete the whole tree
			old_tree = Tree(self.children, self.value)
			self.children = {}
			self.value = None
			return old_tree

		# Traverse upwards, deleting while there're no children left
		tree_path = list(tree_path) # copy
		deleted_subtree = None
		while trace:
			parent = trace.pop()
			edge_label = tree_path.pop()
			child = parent.children.pop(edge_label, None)
			if not deleted_subtree:
				deleted_subtree = child
			if parent.children:
				break # we've still got children left, stop deleting upwards

		return deleted_subtree

	@synchronise
	def copy(self):
		"""
		Recursively copies the entire tree.
		Returns: Tree
		Thread-safe: Yes
		"""
		return Tree(
			children={
				edge_label: child.copy()
				for (edge_label, child) in self.children.iteritems()
			},
			value=self.value,
		)

	@synchronise
	def get_value(self, path):
		"""
		Get a subtree by traversing `path`.

		Params:
			path: Sequence[str]
		Returns: Maybe[Tree]
		Thread-safe: Yes
		"""
		node = self.traverse(path)
		if node is None:
			return node
		return node.value

	@synchronise
	def traverse(self, path):
		"""Get the value of the leaf at `path`.

		Params:
			path: Sequence[str]
		Returns: Maybe[str]
		Thread-safe: Yes
		"""
		current = self
		for edge_label in path:
			current = current.children.get(edge_label, None)
			if current is None:
				return
		return current

	def get_leaves_with_values(self, tree_path):
		"""Generates all values stored in the tree.

		Params:
			tree_path: List[str]
				- a mutable parameter; everytime this generator yields a value, the list will
				contain the path to the said value
				- You typically want to initialize this with the current node's path
				from root (or [] if you're traversing the root)

		Note: the returned `tree_path` is modified whilst iterating
		Returns: Iterable[value: str]
		Thread-safe: No. Generators are generally thread unsafe.
		"""
		if self.value is not None:
			yield self.value

		for edge_label, child in self.children.iteritems():
			tree_path.append(edge_label)
			for val in child.get_leaves_with_values(tree_path):
				yield val
			tree_path.pop()
