from __future__ import absolute_import

import logging
import six
import sys
import weakref
from threading import Event
from redis.exceptions import TimeoutError, RedisError, ConnectionError
from time import time

from treestream.exceptions import (
	TreeWriterError,
	RetryableTreeWriterError,
)
from treestream.writer import (
	TreeWriter,
	NONTRANSACTIONAL,
	WEAKTRANSACTIONAL,
)

from treestream.utils import (
	RunnableAndStoppableMixin,
	ThreadBase,
)
from treestream.backends.redis_lua.utils import (
	RedisScriptsManager,
	bfs_redis_tree,
	make_redis,
	get_redis_keys,
)
from treestream.backends.redis_lua.lua_scripts import (
	UPDATE as LUA_UPDATE_SCRIPT,
	DELETE as LUA_DELETE_SCRIPT,
	RPOP_SADD_MANY as LUA_RPOP_SADD_MANY_SCRIPT,
	LIST_INTO_SET as LUA_LIST_INTO_SET_SCRIPT,
)
from treestream.backends.redis_lua.constants import (
	GC_QUEUE_POP_SIZE,
	GC_DELETE_BATCH_SIZE,
	GC_BUFFERING_WAIT,
)


class GcThread(ThreadBase):
	"""
	When delete()-ing a subtree, only the link between the parent and the subtree to be deleted
	is cleaned. That still leaves us with an entire subtree that is now inaccessible and needs
	to be removed from redis.

	delete() doesn't need to wait for that entire subtree to be freed from memory, so we perform
	that asynchronously, in this thread, instead.
	"""

	name = 'treestream_redis_gc'
	must_always_run = True

	def __init__(self, writer):
		self._writer = weakref.proxy(writer)

		self._keys = writer._keys
		self._logger = writer._logger
		self._use_redis4_features = writer.use_redis4_features
		self._redis = writer.redis
		self._socket_timeout = writer.socket_timeout
		self._scripts_manager = writer.scripts_manager

	def run(self):
		while True:
			for sleep_time in self._run_gc_once(GC_BUFFERING_WAIT):
				yield sleep_time
			yield

	def _get_next_node_to_gc(self, move_from, move_into):
		# BRPOP only accepts integer seconds timeout
		soft_timeout = int(self._socket_timeout - 1) or int(self._socket_timeout)
		try:
			return self._redis.brpoplpush(
				move_from,
				move_into,
				timeout=soft_timeout)
		except TimeoutError:
			# redispy drops the connection if we reach here, so we want the soft timeout above to
			# be slightly smaller than the `socket_timeout`. Otherwise we'll create a new connection
			# every time this function is run.
			return None

	def _run_gc_once(self, wait):
		move_from = self._keys['deferred_delete_queue']
		move_into_tmp = self._keys['nodes_being_deleted_queue']
		move_into = self._keys['nodes_being_deleted_set']

		self._logger.debug('Asking for gc-able nodes')
		# We would like to do a BRPOPSADD_MANY here, but that's not a thing, so we emulate it by
		# doing:
		#    BRPOPLPUSH a b
		#    RPOPSADD^* b c
		#    RPOPSADD^100 a c
		# Where `a` is the queue of gc-able nodes, `b` is a temporary list and `c` is the set
		# of currently-being-gced nodes
		# "RPOPSADD^x" is a lua script

		node_ptr = self._get_next_node_to_gc(move_from, move_into_tmp)
		if not node_ptr:
			return

		# Wait a bit, maybe there's more stuff coming. Also, don't wanna use all the CPU
		yield wait

		# BRPOPLPUSH should've moved to the `move_into` set, but it actually moved to the
		# `move_into_tmp` list (since it can't do sets). Let's correct that.
		# Move everything from the `move_into_tmp` list into the `move_into` set.
		self._scripts_manager.evalsha('list_into_set', [move_into_tmp, move_into], [])

		node_ptrs = [node_ptr]
		node_ptrs += self._scripts_manager.evalsha(
			'rpop_sadd_many',
			[move_from, move_into],
			[GC_QUEUE_POP_SIZE - 1])
		self._logger.debug('Got nodes to gc %s', node_ptrs)

		all_node_ptrs = set(node_ptrs)
		for (node_ptr, child_ptr, _label) in bfs_redis_tree(
				self._redis,
				node_ptrs,
				self._keys['children_prefix']):
			all_node_ptrs.add(node_ptr)
			all_node_ptrs.add(child_ptr)

		# Bucko: delete the nodes in the reverse order they were created in (since we don't have
		# a dfs_redis_tree implementation)
		all_node_ptrs = sorted(all_node_ptrs, key=int, reverse=True)

		self._logger.info('GC-ing %d nodes', len(all_node_ptrs))
		self._delete_nodes(all_node_ptrs)
		self._logger.info('GC-ed %d nodes', len(all_node_ptrs))

	def _delete_nodes(self, node_ptrs):
		children_key_prefix = self._keys['children_prefix']
		del_command = 'UNLINK' if self._use_redis4_features else 'DEL'
		values = self._keys['values']
		move_into = self._keys['nodes_being_deleted_set']
		for i in six.moves.xrange(0, len(node_ptrs), GC_DELETE_BATCH_SIZE):
			nodes_batch = node_ptrs[i:i + GC_DELETE_BATCH_SIZE]
			keys_batch = [children_key_prefix + node_ptr for node_ptr in nodes_batch]
			with self._redis.pipeline(transaction=False) as pipeline:
				pipeline.execute_command(
					del_command,
					*keys_batch)
				pipeline.hdel(values, *nodes_batch)
				pipeline.srem(move_into, *nodes_batch)
				pipeline.execute()



class GcFixerThread(ThreadBase):
	"""
	It is technically possible for the gc thread to pop a bunch of nodes to gc from the "gc-able
	nodes queue", and then crash before it manages to complete the work it needed to do.

	This thread occasionally checks for nodes that have stayed in the process of being gc-ed for
	a long time, but haven't actually been gc-ed. If it detects any, it readds them to the gc
	queue.
	"""

	name = 'treestream_redis_gc_fixer'
	must_always_run = True

	def __init__(self, writer):
		self._writer = weakref.proxy(writer)

		self._redis = writer.redis
		self._logger = writer._logger
		self._keys = writer._keys

	def run(self):
		while True:
			ongoing_deletions1 = self._get_ongoing_deletions()
			# What are the ongoing deletions 10s later?
			yield 10.0
			ongoing_deletions2 = self._get_ongoing_deletions()

			# self._logger.debug('ongoing_deletions1=%r ongoing_deletions2=%r',
			# 	ongoing_deletions1, ongoing_deletions2)

			slow_deletions = (ongoing_deletions1 & ongoing_deletions2)
			if slow_deletions:
				self._requeue_slow_deletions(slow_deletions)

			# Wait 10 mins before running again
			yield 600.0

	def _get_ongoing_deletions(self):
		key = self._keys['nodes_being_deleted_set']
		cursor = 0
		ongoing_deletions = set()
		while True:
			# SSCAN doesn't guarantee we'll get a snapshot view of the set,
			# but we don't really care in this case.
			cursor, extra = self._redis.sscan(key, cursor)
			ongoing_deletions |= set(extra)
			if cursor == 0:
				break
		return ongoing_deletions

	def _requeue_slow_deletions(self, slow_deletions):
		with self._redis.pipeline(transaction=False) as pipeline:
			for node_ptr in slow_deletions:
				self._logger.warn(
					'Node %s stuck in ongoing deletion for 10s, has the gc '
					'thread crashed midway? Readding to the deletion queue',
					node_ptr)
				pipeline.rpush(self._keys['deferred_delete_queue'], node_ptr)
				pipeline.srem(self._keys['nodes_being_deleted_set'], node_ptr)
			pipeline.execute()


# TODO replay_my_writes_on_redis_crash?
class RedisTreeWriter(RunnableAndStoppableMixin, TreeWriter):
	"""
	See the documentation of TreeWriter.
	"""
	LOGGER = logging.getLogger('treestream.RedisTreeWriter')

	def __init__(self,
			redis_host='localhost',
			redis_port=6379,
			socket_timeout=2.0,
			logger=None,
			tree_name='',
			use_redis4_features=True,
			extra_redis_connection_pool_opts=None,
			**kwargs):
		super(RedisTreeWriter, self).__init__(**kwargs)

		self.redis = make_redis(
			host=redis_host,
			port=redis_port,
			# Our commands shouldn't take longer than this to run, and we don't want things like
			# BRPOPLPUSH hanging forever, so that stop()/restart() can be honoured.
			# redispy doesn't expose its socket fds, so wouldn't work with event loops
			socket_timeout=socket_timeout,
			**(extra_redis_connection_pool_opts or {})
		)
		self._logger = logger or self.LOGGER
		self._stop_requested = Event()
		self._keys = get_redis_keys(tree_name)

		self.use_redis4_features = use_redis4_features
		self.socket_timeout = socket_timeout
		self.tree_name = tree_name

		self.scripts_manager = RedisScriptsManager(
			redis=self.redis,
			use_redis4_features=self.use_redis4_features,
			logger=self._logger,
			scripts=(
				('update', LUA_UPDATE_SCRIPT),
				('delete', LUA_DELETE_SCRIPT),
				('rpop_sadd_many', LUA_RPOP_SADD_MANY_SCRIPT),
				('list_into_set', LUA_LIST_INTO_SET_SCRIPT),
			)
		)

	def update(self, tree_path, value, pipeline=None):
		"""
		See the documentation of TreeWriter.update.
		"""
		assert isinstance(tree_path, (list, tuple))
		assert all(isinstance(label, six.string_types) for label in tree_path)
		assert isinstance(value, six.string_types)
		try:
			xid_or_error = self.scripts_manager.evalsha(
				'update',
				keys=[],
				argv=list(tree_path) + [self.tree_name, str(time()), value],
				pipeline=pipeline,
			)

			if isinstance(xid_or_error, six.integer_types):
				return xid_or_error
			elif isinstance(xid_or_error, str):
				raise TreeWriterError(xid_or_error)
			elif xid_or_error is not None:
				raise ValueError(xid_or_error)
		except (ConnectionError, TimeoutError) as e:
			six.reraise(RetryableTreeWriterError(e.message), None, sys.exc_info()[2])
		except RedisError as e:
			six.reraise(TreeWriterError(e.message), None, sys.exc_info()[2])

	def delete(self, tree_path, pipeline=None):
		"""
		See the documentation of TreeWriter.delete.
		"""
		assert isinstance(tree_path, (list, tuple))
		assert all(isinstance(label, six.string_types) for label in tree_path)
		try:
			xid = self.scripts_manager.evalsha(
				'delete',
				keys=[],
				argv=list(tree_path) + [self.tree_name, time()],
				pipeline=pipeline,
			)

			return xid
		except (ConnectionError, TimeoutError) as e:
			six.reraise(RetryableTreeWriterError(e.message), None, sys.exc_info()[2])
		except RedisError as e:
			six.reraise(TreeWriterError(e.message), None, sys.exc_info()[2])

	def update_many(self, base_tree_path, values, level=NONTRANSACTIONAL):
		"""
		See the documentation of TreeWriter.update_many.

		Supported levels: NONTRANSACTIONAL, WEAKTRANSACTIONAL
		"""
		assert level in (NONTRANSACTIONAL, WEAKTRANSACTIONAL), 'unsupported level'

		base_tree_path = list(base_tree_path)
		with self.redis.pipeline(transaction=level > NONTRANSACTIONAL) as pipeline:
			for tree_path, value in values:
				self.update(base_tree_path + list(tree_path), value, pipeline=pipeline)
			ret_values = pipeline.execute()

		for ret_value in ret_values:
			if isinstance(ret_value, str):
				raise TreeWriterError(ret_value)

		return ret_values

	def _pre_run(self):
		self.scripts_manager.register_scripts()

	def _get_threads_to_run(self):
		threads = super(RedisTreeWriter, self)._get_threads_to_run()
		return threads + [GcThread(self), GcFixerThread(self)]
