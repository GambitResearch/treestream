from __future__ import absolute_import

from time import sleep
from Queue import Queue

from treestream.exceptions import TreeWriterError
from treestream.utils import ThreadBase

from msgpack import dumps as msgpack_dumps

NONTRANSACTIONAL = 0
WEAKTRANSACTIONAL = 1
STRONGTRANSACTIONAL = 2

# TODO callback on redis crash, s.t. the writer knows to resend everything
# TODO allow pipelining multiple updates & deletes
# TODO implement a WritesBatcherMixin, s.t. we send more than one operation per TCP packet
class TreeWriter(object):
	"""
	Allows sharing a hierarchical data store (`Tree`) with `TreeReader`-s.
	The edges of the tree are labelled, and the leaf nodes can store values.
	"""

	def update(self, tree_path, value):
		"""
		Updates the value of the leaf node obtained by following the edge labels in `tree_path`,
		creating the required internal node(s) along the way.
		Nodes may have children or a value, but not both.

		If you're calling this from your critical path and do not want your current thread to
		block until the update is complete, have a look at NonblockingWriterMixin.update_nowait.

		Args:
			tree_path: Sequence[str]
			value: str
		Returns:
			xid: Either[int, NoneType]
		Raises:
			* TreeWriterError
				* RetryableTreeWriterError
		Complexity: O(|tree_path| + |value|)
		"""
		raise NotImplementedError

	def delete(self, tree_path):
		"""
		Deletes the subtree rooted at `tree_path`, including all of its descendants and leaf
		values.

		Like update(), blocks until completion. See also NonblockingWriterMixin.delete_nowait.

		Args:
			tree_path: Sequence[str]
			value: str
		Returns:
			xid: Either[int, NoneType]
		Raises:
			* TreeWriterError
				* RetryableTreeWriterError
		Complexity: O(|tree_path|)
		"""
		raise NotImplementedError

	def update_many(self, base_tree_path, values, level=NONTRANSACTIONAL):
		"""
		Performs all the updates in `values` to the subtree rooted at `base_tree_path`.

		Like update(), blocks until completion. See also NonblockingWriterMixin.update_many_nowait.

		If NONTRANSACTIONAL:
			(1) updates may be interleaved with others
			(2) no all-or-nothing guarantees are made if an exception is raised,
				i.e. it may be that only a subset of the values were updated

		If WEAKTRANSACTIONAL:
			(1) updates may NOT be interleaved with others
			(2) no all-or-nothing guarantees

		If STRONGTRANSACTIONAL:
			(1) updates may NOT be interleaved with others
			(2) all-or-nothing guaranteed

		See also NonblockingWriterMixin.update_many_nowait.

		Args:
			base_tree_path: Sequence[str]
			values: Sequence[(tree_path: str, value: str)]
		Returns:
			xid: Sequence[Either[int, NoneType]]
		Raises:
			see update()
		"""
		assert transactional == NONTRANSACTIONAL, 'Transactional updates are not supported'

		# The default implementation, if the chosen backend does not provide a more efficient one
		base_tree_path = list(base_tree_path)
		ret_values = []
		for tree_path, value in values:
			ret_values.append(
				self.update(base_tree_path + list(tree_path), value)
			)

		return ret_values

	# TODO def autoexpire(self, tree_path, seconds)
	#      would allow implementing heartbeats / storing short lived data
	#      e.g. "autoexpire(x, 2.0); *wait 2s*" --> delete(x)

	def run(self):
		raise NotImplementedError

	def stop(self):
		raise NotImplementedError

	def restart(self):
		raise NotImplementedError


class MsgpackWriterMixin(object):
	def update(self, tree_path, value, *args, **kwargs):
		super(MsgpackWriterMixin, self).update(
			map(msgpack_dumps, tree_path),
			msgpack_dumps(value),
			*args,
			**kwargs
		)

	def delete(self, tree_path, *args, **kwargs):
		super(MsgpackWriterMixin, self).delete(
			map(msgpack_dumps, tree_path),
			*args,
			**kwargs
		)


class NonblockingWriterThread(ThreadBase):
	name = 'treestream_writes_pusher'

	def __init__(self,
			exc_handler,
			sleep_seconds_on_exception,
			logger,
			update_func,
			update_many_func,
			delete_func):
		self._exc_handler = exc_handler
		self.sleep_seconds_on_exception = sleep_seconds_on_exception
		self.ops_queue = Queue(maxsize=10000)
		self._logger = logger
		self._update_func = update_func
		self._update_many_func = update_many_func
		self._delete_func = delete_func

	def run(self):
		update_func = self._update_func
		delete_func = self._delete_func
		update_many_func = self._update_many_func
		exc_handler = self._exc_handler
		sleep_seconds_on_exception = self.sleep_seconds_on_exception
		while True:
			op = self.ops_queue.get()
			op_type = op[0]
			try:
				if op_type == 'update':
					update_func(op[1], op[2])
				elif op_type == 'delete':
					delete_func(op[1])
				elif op_type == 'update_many':
					update_many_func(op[1], op[2])
				else:
					raise ValueError(op)
			except TreeWriterError:
				self._logger.exception('')
				if exc_handler:
					exc_handler()
				if sleep_seconds_on_exception is not None:
					sleep(sleep_seconds_on_exception)


class NonblockingWriterMixin(object):
	def __init__(self, exc_handler=None, sleep_seconds_on_exception=None, **kwargs):
		super(NonblockingWriterMixin, self).__init__(**kwargs)
		self.pusher_thread = NonblockingWriterThread(
			exc_handler=exc_handler,
			sleep_seconds_on_exception=sleep_seconds_on_exception,
			logger=self._logger,
			update_func=self.update,
			update_many_func=self.update_many,
			delete_func=self.delete,
		)

	def update_nowait(self, tree_path, value):
		self.pusher_thread.ops_queue.put_nowait(('update', tree_path, value))

	def update_many_nowait(self, base_tree_path, values):
		self.pusher_thread.ops_queue.put_nowait(('update_many', base_tree_path, values))

	def delete_nowait(self, tree_path):
		self.pusher_thread.ops_queue.put_nowait(('delete', tree_path))

	def _get_threads_to_run(self):
		threads = super(NonblockingWriterMixin, self)._get_threads_to_run()
		return threads + [self.pusher_thread]
