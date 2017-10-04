from __future__ import absolute_import
from msgpack import loads as msgpack_loads

from treestream.utils import identity

# TODO handle_latency_update, so that users can plot it
class TreeReader(object):
	tree = None # type: Maybe[Tree]
	_decode_value = staticmethod(identity)
	_decode_edge_label = staticmethod(identity)

	def is_synced(self):
		return False

	def get_xid(self):
		return 0

	@property
	def lag(self):
		return 0.0

	def handle_update(self, timestamp, tree_path, old_value, new_value):
		"""Called once for every writer.update().

		If send_update_callbacks_on_sync=True, then this is also called immediately after a
		sync, once for each value stored in the tree.

		This call occurs *after* self.tree is updated. This callback runs in the same thread
		that processes the stream. Thus, the tree does not change throughout the call. Make sure
		not to do any heavy lifting here though.

		self.get_xid() contains the id of this update.

		Precondition: self.tree.get_value(tree_path) == value

		Args:
			timestamp: float
				The timestamp at the time update() was called, as measured on the writer's machine
			tree_path: list[str]
			old_value: str
			new_value: str
		"""
		raise NotImplementedError

	def handle_delete_subtree(self, timestamp, tree_path, deleted_subtree):
		"""Called once for every writer.delete().

		If clear_tree_on_sync_lost=True, then this is also called once when the sync is
		lost, with tree_path=[], indicating the deletion of the entire tree.

		Same semantics as `handle_update`: called *after* self.tree is updated.

		self.get_xid() contains the id of this subtree delete.

		Precondition: self.tree.traverse(tree_path) is None

		Args:
			timestamp: float
				writer's ts at the time of delete()
			tree_path: list[str]
			deleted_subtree: Maybe[Tree]
				if store_tree=True, this contains the now-gone subtree.

		Returns: bool
			If True, then the subsequent calls to `handle_delete` are suppressed.
		"""
		raise NotImplementedError

	def handle_delete(self, timestamp, tree_path, old_value):
		"""Called after `handle_delete_subtree` (unless it returns True), once for every value
		that was effectively removed from the tree by the `writer.delete()` call.

		self.get_xid() contains the id of the corresponding subtree delete. You may get multiple
		callbacks with the same xid, if the underlying `delete()` resulted in multiple values
		being deleted.

		Precondition: self.tree.get_value(tree_path) is None
		"""
		raise NotImplementedError

	def handle_synced(self, tree):
		raise NotImplementedError

	def handle_sync_lost(self):
		raise NotImplementedError

	def run(self):
		"""Syncs a tree and calls the callbacks.
		Blocks, so should run in a separate thread.
		"""
		raise NotImplementedError

	def stop(self):
		raise NotImplementedError

	def restart(self):
		raise NotImplementedError


class MsgpackReaderMixin(object):
	_decode_edge_label = staticmethod(msgpack_loads)
	_decode_value = staticmethod(msgpack_loads)
