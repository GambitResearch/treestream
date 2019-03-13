from __future__ import absolute_import

import six
from collections import deque
from threading import RLock
from socket import TCP_KEEPIDLE, TCP_KEEPINTVL

from redis import Redis, ConnectionPool
from redis.exceptions import NoScriptError

from treestream.backends.redis_lua.constants import (
	BFS_MAX_QUERIES_PER_PIPELINE
)

# This has been copy-pasted from `redistasks/redistasks.py` to avoid putting them in the same repo
def make_redis(**connection_pool_opts):
	"""This is the preferred way to construct the Redis object before passing it to other
	redistasks classes. """

	all_connection_pool_opts = {
		# Not supplying these options makes repr(redis_obj) raise an exception, despite being
		# optional
		'host': 'localhost',
		'port': 6379,
		'db': 0,

		# Prevent pubsub/blocking connections from waiting forever when a connection is dropped
		# without giving us a FIN packet.
		'socket_keepalive': True,
		'socket_keepalive_options': {TCP_KEEPIDLE: 10, TCP_KEEPINTVL: 1},
		'socket_connect_timeout': 10,

		# Prevent connection leak bugs from DoS-ing redis
		'max_connections': 10,
	}
	all_connection_pool_opts.update(connection_pool_opts)
	return Redis(connection_pool=ConnectionPool(**all_connection_pool_opts))

def get_redis_keys(tree_name):
	return {
		'xid': tree_name + '::xid',
		'root_ptr': tree_name + '::root',
		'ptr_counter': tree_name + '::ptr_counter',
		'pubsub_channel': tree_name + '::pubsub',
		'children_prefix': tree_name + '::children::',
		'values': tree_name + '::values',
		'deferred_delete_queue': tree_name + '::deferred_delete_queue',
		'nodes_being_deleted_queue': tree_name + '::nodes_being_deleted_moveme',
		'nodes_being_deleted_set': tree_name + '::nodes_being_deleted',
	}

# TODO support filter functions
# TODO this is horrendously slow for syncing a large tree. My example took >30s
#      on CPython (~3s on PyPy). The vast majority of time is spent building
#      large HSCAN pipelines. Redispy is probably doing loads of string
#      manipulation in pure python to construct those. There are two fixes:
#      1) Rewrite this function (and maybe other slow bits) in C, using the
#         hiredis library, then execute with cffi.
#         I suspect it'll be *MUCH* faster.
#      2) When syncing, get all the keys from redis via SCAN, instead of
#         traversing the tree, then throw away the stuff we don't need. We may
#         occasionally get more data than we need, but we'll be *sending* much
#         less data to redis.
#      3) Rewrite in Lua, by creating two functions: BFS_INIT (returning a cursor),
#         and BFS_STEP <cursor> <n>, processing <n> queue items
#      4) Rewrite everything as a C redis module with commands:
#           TGET <tree_name> <tree_path> / TMGET <tree_name> <tree_path>
#           TSUBSCRIBE <tree_name> - get a stream of ops
#           TSYNC <tree_name> - get the sync of ops
#           TUPDATE <tree_name> <value> <tree_path...>
#           TDELETE <tree_name> <tree_path...>
#           TEXPIRE <tree_name> <seconds> <tree_path>
#   I think we can leave this as is for the initial version. I'd personally do (4).
def bfs_redis_tree(
		redis,
		starting_nodes,
		children_key_prefix,
		max_queries_per_pipeline=BFS_MAX_QUERIES_PER_PIPELINE):
	"""
		Params:
			redis: Redis
			starting_nodes: List[(node_ptr: str)]
			children_key_prefix: str

		Returns: Iterable[(from_node_ptr: str, to_node_ptr: str, arc_label: str)]
	"""
	queue = deque([(str(node), 0) for node in starting_nodes]) # [(node_ptr: str, cursor: int)]
	while queue:
		queries = []
		for _ in six.moves.xrange(max_queries_per_pipeline):
			try:
				queries.append(queue.popleft())
			except IndexError:
				break

		with redis.pipeline(transaction=False) as pipeline:
			for node_ptr, cursor in queries:
				# we're using socket_timeout, so can't block forever here
				pipeline.hscan(children_key_prefix + str(node_ptr), cursor)
			query_responses = pipeline.execute()
		#print '%d hscans took %.3fs' % (len(queries), time() - t)

		assert len(queries) == len(query_responses)
		for (node_ptr, cursor), (new_cursor, children) in zip(queries, query_responses):
			if new_cursor > 0:
				# We didn't get all children of this node yet
				queue.append((node_ptr, new_cursor))

			for child_name, child_ptr in six.iteritems(children):
				queue.append((child_ptr, 0))
				yield (node_ptr, child_ptr, child_name)

def get_adjacency_dict(redis, starting_nodes, children_key_prefix, edge_decoding_func):
	"""
	Params: see bfs_redis_tree
	Returns: adjacency_dict: { (node_ptr: str) -> {(arc_label: str) -> (child_ptr: str)} }
	"""
	# Guarantee that each node appears at least once in this dict (either as a key or as a value)
	adjacency_dict = {node: {} for node in starting_nodes}

	for (node_ptr, subnode_ptr, subnode_name) in bfs_redis_tree(
			redis,
			starting_nodes,
			children_key_prefix):

		adjacent_nodes = adjacency_dict.get(node_ptr, None)
		if adjacent_nodes is None:
			adjacency_dict[node_ptr] = adjacent_nodes = {}
		adjacent_nodes[edge_decoding_func(subnode_name)] = subnode_ptr

	return adjacency_dict


class RedisScriptsManager(object):
	def __init__(self, redis, scripts, use_redis4_features, logger):
		self.redis = redis
		self.named_scripts = scripts # Iterable[(script_name, script_code)]
		self.use_redis4_features = use_redis4_features
		self._logger = logger or self.LOGGER
		self._register_scripts_lock = RLock()
		self._script_shas = {} # {script_name -> script_sha}

	def evalsha(self, script_name, keys, argv, pipeline=None):
		if not self._script_shas:
			self.register_scripts()

		if pipeline is None: # bool(Pipeline) is False for some reason
			redis = self.redis
		else:
			redis = pipeline

		script_sha = self._script_shas[script_name]
		all_args = [script_sha, len(keys)] + keys + argv
		try:
			ret = redis.evalsha(*all_args)
		except NoScriptError:
			self.register_scripts()
			ret = redis.evalsha(*all_args)

		if not pipeline:
			return ret

	def _process_script(self, script):
		if self.use_redis4_features:
			# UNLINK is better than DEL, since it performs the actual deletion in a separate
			# thread, so it only blocks for O(1), rather than O(n), time.
			# It's only available in redis4, though.
			return script.replace("'DEL'", "'UNLINK'")
		return script

	def _ensure_scripts_are_loaded(self):
		with self.redis.pipeline(transaction=False) as pipeline:
			for _script_name, script_code in self.named_scripts:
				pipeline.script_load(self._process_script(script_code))
			shas = pipeline.execute()

		assert len(shas) == len(self.named_scripts)

		self._script_shas = {
			script_name: sha
			for (script_name, _script_code), sha in zip(self.named_scripts, shas)
		}

	def register_scripts(self):
		with self._register_scripts_lock:
			self._ensure_scripts_are_loaded()

			log = ['Registered scripts:'] + [
				'\t{script_name} ({sha})'.format(script_name=script_name, sha=sha)
				for script_name, sha in self._script_shas.items()
			]

			self._logger.info('\n'.join(log))
