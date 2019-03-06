from __future__ import absolute_import

import logging
import six
import weakref
from six.moves import queue
from threading import Event
from redis.exceptions import TimeoutError
from time import time
from msgpack import loads

from treestream.exceptions import TreeReaderError
from treestream.tree import Tree
from treestream.reader import TreeReader
from treestream.utils import RunnableAndStoppableMixin, ThreadBase
from treestream.backends.redis_lua.utils import (
	make_redis,
	get_redis_keys,
	get_adjacency_dict,
)
from treestream.backends.redis_lua.constants import (
	PROTO_VERSION,
	OP_UPDATE,
	OP_DELETE,
	SYNC_MULTIGET_BATCH_SIZE,
)

class SharedState(object):
	"""
	Stores state shared by multiple of the threads below.

	Having a separate class means it's easy to start with a clean state if the reader crashes
	and is restarted.
	"""
	def __init__(self,
			redis_host,
			redis_port,
			max_buffer_size,
			socket_timeout,
			extra_redis_connection_pool_opts):
		self.pubsub_queue = queue.Queue(maxsize=max_buffer_size)
		self.synced = Event()
		self.is_subscribed = Event()
		self.lag = 0.0
		self.tree = None
		self.current_tree_xid = 0
		self.current_stream_xid = 0
		self.redis = make_redis(
			host=redis_host,
			port=redis_port,
			socket_timeout=socket_timeout,
			**(extra_redis_connection_pool_opts or {})
		)


class StreamProcessorThread(ThreadBase):
	"""
	This thread queues messages from a pubsub channel onto state.pubsub_queue.

	It also periodically pings redis over the same connection for computing latency and
	heartbeating the connection.
	"""

	name = 'treestream_redis_stream_processor'
	must_always_run = True

	def __init__(self, reader, state):
		self._reader = weakref.proxy(reader)
		self._state = state

		self._redis = state.redis
		self._keys = reader._keys
		self._logger = reader._logger
		self._stream_ping_every = reader.stream_ping_every
		self._max_stream_lag = reader.max_stream_lag

	def run(self):
		pubsub = None
		try:
			pubsub = self._redis.pubsub()
			channel_name = self._keys['pubsub_channel']
			pubsub.subscribe(channel_name)

			response = pubsub.parse_response(block=True)
			if response[:2] != ['subscribe', channel_name]:
				raise TreeReaderError('could not subscribe: ' + repr(response))

			yield
			self._state.is_subscribed.set()
			self._read_pubsub(pubsub)
		finally:
			if pubsub:
				pubsub.close()

	def _read_pubsub(self, pubsub):
		ping_sent_at = None
		state = self._state
		got_pong_at = 0
		on_pubsub_message = self._on_pubsub_message
		stop_requested = self._reader._stop_requested
		connection = pubsub.connection
		# This is a nasty hack, but it's the only way I've found to receive a socket.timeout
		# without dropping the connection. Connection.read_response does a disconnect() in an
		# except block (and a timeout results in a TimeoutError).
		# Ideally, we want to wait until either there's a redis response, there's a ping timeout,
		# or there's a stop request, whichever comes first.
		read_response = connection._parser.read_response
		handle_message = pubsub.handle_message

		while not stop_requested.is_set():
			# self._logger.debug('stream: waiting')

			try:
				response = read_response()
			except TimeoutError:
				# Raised if the read timed out after `socket_timeout`. The connection is preserved,
				# though.
				response = None
			# self._logger.debug('stream: got %r', response)

			if response is None and ping_sent_at:
				raise TreeReaderError('ping timeout')

			now = time()
			if not ping_sent_at and now - got_pong_at > self._stream_ping_every:
				self._logger.debug('stream: ping?')
				ping_sent_at = time()
				pubsub.execute_command('PING')

			if response is None:
				continue

			# redispy's handle_message can't handle pubsub pong responses
			if response[0] == 'pong':
				got_pong_at = now
				state.lag = got_pong_at - ping_sent_at
				self._logger.debug('stream: pong! (%.3fms roundtrip)', state.lag * 1000.)
				# Check stream's not too slow (only when we're synced)
				if state.synced.is_set() and state.lag > self._max_stream_lag:
					raise TreeReaderError('lag is %.2fs (> %.2fs)' % (
						state.lag,
						self._max_stream_lag))
				ping_sent_at = None
				continue

			message = handle_message(response)
			if message['type'] == 'message':
				on_pubsub_message(loads(message['data']))

	def _on_pubsub_message(self, data):
		proto_version = data[0]
		if proto_version != PROTO_VERSION:
			raise ValueError(proto_version)
		state = self._state

		xid = data[1]
		current_xid = state.current_stream_xid
		if current_xid and xid != current_xid + 1:
			raise TreeReaderError('stream jumped from xid %d to %d' % (current_xid, xid))
		state.current_stream_xid = xid

		# raises an exception if Full, making us crash
		state.pubsub_queue.put_nowait(data[1:])


class SyncerThread(ThreadBase):
	"""
	This thread runs once per connection and exits once it's synced.

	It:
		1. Waits until the stream connection is connected
		2. Extracts the state of the tree from redis
		3. Replays the stream until it's empty without running callbacks
		4. Sets the synced event so that the callbacks thread can take over

	Upon completion, state.tree will contain the state of the tree
	at xid state.current_tree_xid (regardless of the value of `store_tree`). We need
	to compute the initial tree to be able to call all the callbacks.
	"""
	name = 'treestream_redis_syncer'
	must_always_run = False # stops once we're synced

	def __init__(self, reader, state):
		self._reader = reader
		self._state = state

		self._redis = state.redis
		self._keys = reader._keys
		self._logger = reader._logger
		self._stop_check_period = reader.stop_check_period
		self._decode_value = reader._decode_value
		self._decode_edge_label = reader._decode_edge_label

	def run(self):
		for _ in self._sync():
			yield
		yield
		self._logger.info('syncer: synced')
		# Allow callbacks to be sent
		self._state.synced.set()

	def _sync(self):
		state = self._state
		assert not state.synced.is_set()

		while not state.is_subscribed.wait(timeout=self._stop_check_period):
			yield
		self._logger.info("syncer: looks like we've subscribed, proceeding")

		start_xid, root_ptr = self._get_xid_and_root_ptr()

		if not root_ptr:
			state.tree = Tree()
			state.current_tree_xid = start_xid or 0
			self._logger.info(
				"syncer: no root ptr in redis, claiming we're synced (xid=%d)",
				state.current_tree_xid)
			return

		assert isinstance(root_ptr, str)

		self._logger.info(
			'syncer: asking for tree with root_ptr=%s (start_xid=%s)',
			root_ptr,
			start_xid)

		state.tree = self._get_tree(root_ptr)
		end_xid, end_root_ptr = self._get_xid_and_root_ptr()
		self._logger.info('syncer: got tree: start_xid=%s end_xid=%s', start_xid, end_xid)

		yield

		# This is probably fine, just being extra cautious. It'll just retry the sync if the root
		# changes.
		if end_root_ptr != root_ptr:
			msg = 'syncer: root_ptr changed while syncing ' \
				'(from %s to %s), ' % (root_ptr, end_root_ptr)
			self._logger.warn(msg)
			raise TreeReaderError(msg)

		# The `tree` is now in a mixture of states in [start_xid, end_xid], so we need to reapply
		# all the changes in that interval.

		if start_xid < end_xid:
			self._logger.info(
				'syncer: replaying all operations in [%s, %s] onto the tree',
				start_xid + 1,
				end_xid)

		state.current_tree_xid = start_xid

		# Replay all the operations we've accumulated whilst syncing
		while state.current_tree_xid < end_xid or not state.pubsub_queue.empty():
			yield
			# self._logger.debug('syncer: waiting for %d', start_xid)
			try:
				data = state.pubsub_queue.get(timeout=self._stop_check_period)
			except queue.Empty:
				continue
			self._reader._process_message(data, run_callbacks=False)

		yield
		self._logger.info(
			'syncer: replayed ops accumulated during sync (now at xid=%d)',
			start_xid)

	# TODO support tree filters
	def _get_tree(self, root_ptr):
		redis = self._redis
		decode_value_func = self._decode_value
		adjacency_dict = get_adjacency_dict(
			redis=redis,
			starting_nodes=[root_ptr],
			children_key_prefix=self._keys['children_prefix'],
			edge_decoding_func=self._decode_edge_label)
		self._logger.info('syncer: got adjacency dict')

		all_node_ptrs = {
			node
			for children_of_node in six.itervalues(adjacency_dict)
			for node in six.itervalues(children_of_node)
		}
		all_node_ptrs.add(root_ptr) # root_ptr is no-one's child
		all_node_ptrs = list(all_node_ptrs)

		all_node_values = []

		values_key = self._keys['values']
		self._logger.info('syncer: querying values')
		for i in range(0, len(all_node_ptrs), SYNC_MULTIGET_BATCH_SIZE):
			batch = all_node_ptrs[i:i + SYNC_MULTIGET_BATCH_SIZE]
			all_node_values += self._redis.hmget(values_key, batch)
		self._logger.info('syncer: got values')

		assert len(all_node_ptrs) == len(all_node_values)
		values = {
			node_ptr: decode_value_func(value) if value is not None else None
			for node_ptr, value in zip(all_node_ptrs, all_node_values)
		}

		self._logger.info('syncer: building in-memory tree')
		ret = Tree.from_adjacency_dict_and_values(adjacency_dict, values, root_ptr)
		self._logger.info('syncer: built in-memory tree')
		return ret

	def _get_xid_and_root_ptr(self):
		with self._redis.pipeline(transaction=True) as pipeline:
			pipeline.get(self._keys['xid'])
			pipeline.get(self._keys['root_ptr'])
			xid, root_ptr = pipeline.execute()

		xid = int(xid) if xid else 0
		return xid, root_ptr


class CallbacksThread(ThreadBase):
	"""This thread takes over from the sync thread.
	It reads messages from the stream, and runs the `_process_message` callback for each.
	"""

	name = 'treestream_redis_callbacks'
	must_always_run = True

	def __init__(self, reader, state):
		self._reader = reader
		self._state = state

		self._logger = reader._logger
		self._stop_check_period = reader.stop_check_period
		self._send_update_callbacks_on_sync = reader.send_update_callbacks_on_sync
		self._clear_tree_on_sync_lost = reader.clear_tree_on_sync_lost
		self._decode_edge_label = reader._decode_edge_label
		self._decode_value = reader._decode_value
		self._store_tree = reader.store_tree

	def run(self):
		state = self._state
		try:
			# Don't send callbacks before syncing
			while not state.synced.wait(timeout=self._stop_check_period):
				yield

			# Send all callbacks by traversing the tree
			if self._send_update_callbacks_on_sync:
				self._logger.info('runner: Calling update callbacks')
				now = time()
				handle_update = self._reader.handle_update
				tree_path = []
				for value in state.tree.get_leaves_with_values(tree_path):
					handle_update(now, tree_path, None, value)
				self._logger.info('runner: Called update callbacks')

			self._logger.info('runner: Calling handle_synced()')
			self._reader.handle_synced(state.tree)
			self._logger.info('runner: Called handle_synced()')

			if not self._store_tree:
				state.tree = None

			while not self._reader.should_stop():
				try:
					message = state.pubsub_queue.get(timeout=self._stop_check_period)
				except queue.Empty:
					continue

				self._reader._process_message(message)
		finally:
			if state.synced.is_set():
				self._logger.info('runner: Calling handle_sync_lost()')
				self._reader.handle_sync_lost()
				self._logger.info('runner: Called handle_sync_lost()')
				if self._clear_tree_on_sync_lost:
					self._logger.info('runner: Calling delete(())')
					self._reader._process_message((0, time(), OP_DELETE, []))
					self._logger.info('runner: Called delete(())')


class RedisTreeReader(RunnableAndStoppableMixin, TreeReader):
	"""
	See the documentation of TreeReader.
	"""
	LOGGER = logging.getLogger('treestream.RedisTreeReader')

	# TODO add unix sockets support
	def __init__(self,
			redis_host='localhost',
			redis_port=6379,

			logger=None,
			socket_timeout=2.0,
			tree_name='',
			max_stream_lag=2.0,
			stream_ping_every=1.0,
			store_tree=True,
			max_buffer_size=100000,
			send_update_callbacks_on_sync=True,
			clear_tree_on_sync_lost=True,
			send_recursive_delete_callbacks=True,
			extra_redis_connection_pool_opts=None,
			stop_check_period=1.0,
			**kwargs):
		"""
		Params:
			- `redis_host` and `redis_port`
		Optional (tuning) params:
			- logger
				Defaults to logging.getLogger('treestream.RedisTreeReader').
			- socket_timeout
				we need a timeout on the redis sockets so that we can implement stop(), since
				redispy is not designed to cooperate with any event loop (barring monkey patching).
			- tree_name: str
				allows storing multiple trees in the same redis instance; doesn't really add a
				lot, since the name can just be embedded in the first level of the tree.
			- max_stream_lag: float
				crash and start syncing from scratch if, when sending a PING to pubsub, we process
				the PONG at least this many seconds later.
				TODO this currently makes it impossible for us to sync very large trees, since
				the "building in-memory tree" bit of the sync seems to make us lag some large
				number of seconds. It's possibly because of the GIL.
			- stream_ping_every: float
				Send a PING command to the pubsub socket every this many seconds
			- store_tree: bool
				Keep track of `self.tree` after sync.
				Required by `send_recursive_delete_callbacks`.
			- max_buffer_size: int
				Crash & restart if we end up with more than this many unprocessed pubsub messages.
			- send_update_callbacks_on_sync: bool
				Call `handle_update` for each value in `self.tree` after a sync.
			- clear_tree_on_sync_lost: bool
				If we lose a sync for whatever reason, simulate a delete(()) call.
			- send_recursive_delete_callbacks: bool
				Whenever a `delete()` happens, call `handle_delete` once for each element deleted
				from `self.tree`. Needs `store_tree`.
			- extra_redis_connection_pool_opts: dict
				Pass more things to redispy's ConnectionPool (auth password etc.).
			- stop_check_period: float
				Try to see if `stop()` was called every this many seconds.
		"""
		super(RedisTreeReader, self).__init__(**kwargs)

		assert store_tree or not send_recursive_delete_callbacks, \
			"I can't send recursive delete callbacks w/o storing the tree"

		self._logger = logger or self.LOGGER
		self._keys = get_redis_keys(tree_name)

		self.redis_host = redis_host
		self.redis_port = redis_port
		self.socket_timeout = socket_timeout
		self.extra_redis_connection_pool_opts = extra_redis_connection_pool_opts
		self.store_tree = store_tree
		self.send_update_callbacks_on_sync = send_update_callbacks_on_sync
		self.clear_tree_on_sync_lost = clear_tree_on_sync_lost
		self.send_recursive_delete_callbacks = send_recursive_delete_callbacks
		self.max_stream_lag = max_stream_lag
		self.max_buffer_size = max_buffer_size
		self.stop_check_period = stop_check_period
		self.stream_ping_every = stream_ping_every

		self.state = None

		if self.store_tree:
			self._get_tree = lambda: self.state.tree if self.state is not None else None
		else:
			self._get_tree = lambda: None

	@property
	def lag(self):
		return self.state.lag if self.state else 0.0

	def is_synced(self):
		return self.state and self.state.synced.is_set()

	def get_xid(self):
		return self.state.current_tree_xid if self.state else 0

	@property
	def tree(self):
		return self._get_tree()

	def _pre_run(self):
		self.state = SharedState(
			redis_host=self.redis_host,
			redis_port=self.redis_port,
			max_buffer_size=self.max_buffer_size,
			socket_timeout=self.socket_timeout,
			extra_redis_connection_pool_opts=self.extra_redis_connection_pool_opts,
		)

	def _get_threads_to_run(self):
		threads = super(RedisTreeReader, self)._get_threads_to_run()
		return threads + [
			StreamProcessorThread(self, self.state),
			SyncerThread(self, self.state),
			CallbacksThread(self, self.state),
		]

	# TODO move inside Tree
	def _process_message(self, data, run_callbacks=True):
		xid = data[0]
		timestamp = data[1]
		op = data[2]
		tree_path = [self._decode_edge_label(l) for l in data[3]]
		state = self.state

		# We sometimes generate a fake message, which has xid=0
		expected_xid = state.current_tree_xid + 1
		if xid > 0 and xid != expected_xid:
			if xid < expected_xid:
				self._logger.info(
					'got xid %d from the stream, but tree is already at %d, skipping',
					xid,
					state.current_tree_xid)
				return
			else:
				raise TreeReaderError(
					'got xid %d from stream, but tree is at xid %d' % (
						xid,
						state.current_tree_xid,
				))
		state.current_tree_xid = xid

		if op == OP_UPDATE:
			value = self._decode_value(data[4])
			if state.tree is not None:
				old_value = state.tree.apply_update(tree_path, value)
			else:
				old_value = None
			if run_callbacks:
				self.handle_update(timestamp, tree_path, old_value, value)
		elif op == OP_DELETE:
			if state.tree is None:
				deleted_subtree = None
			else:
				deleted_subtree = state.tree.apply_delete(tree_path)

			if run_callbacks:
				should_suppress = self.handle_delete_subtree(
					timestamp,
					tree_path,
					deleted_subtree)

				if deleted_subtree and \
						self.send_recursive_delete_callbacks and not should_suppress:
					handle_delete = self.handle_delete
					for value in deleted_subtree.get_leaves_with_values(tree_path):
						handle_delete(timestamp, tree_path, value)
		else:
			raise ValueError(op)
