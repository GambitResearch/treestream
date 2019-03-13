import unittest
import logging
import sys
import mock
import six
from six.moves import queue
from uuid import uuid1
from threading import Thread, Event
from time import sleep, time

from treestream import (
	MsgpackReaderMixin,
	MsgpackWriterMixin,
	RedisTreeReader,
	RedisTreeWriter,
	Tree,
	TreeWriterError,
)

_GREEN = '\x1B[32m'
_YELLOW = '\x1B[33m'
_BLUE = '\x1B[34m'
_RESET = '\x1B[39m'


class TreeTest(unittest.TestCase):
	LEFT_TREE  = "(1=|'x'|, 2=|'y'|)"
	RIGHT_TREE = "(1=|'z'|, 2=|'t'|)"
	WHOLE_TREE = "(1=%s, 2=%s)" % (LEFT_TREE, RIGHT_TREE)
	LEAVES = [
		(('1', '1'), 'x'),
		(('1', '2'), 'y'),
		(('2', '1'), 'z'),
		(('2', '2'), 't'),
	]

	def setUp(self):
		self.tree = Tree()
		for (tree_path, value) in self.LEAVES:
			self.tree.apply_update(tree_path, value)


	def test_apply_update(self):
		old_value = self.tree.apply_update(('1', '1'), 'xx')
		self.assertEqual('x', old_value)
		self.assertEqual(self.WHOLE_TREE.replace("'x'", "'xx'"), self.tree.inline_str())

	def test_apply_delete0(self):
		deleted_subtree = self.tree.apply_delete(())
		self.assertEqual(self.WHOLE_TREE, deleted_subtree.inline_str())
		self.assertEqual("-", self.tree.inline_str())

	def test_apply_delete1(self):
		deleted_subtree = self.tree.apply_delete(('1',))
		self.assertEqual(self.LEFT_TREE, deleted_subtree.inline_str())
		self.assertEqual("(2=%s)" % self.RIGHT_TREE, self.tree.inline_str())

	def test_apply_delete2(self):
		for (tree_path, value) in self.LEAVES:
			deleted_subtree = self.tree.apply_delete(tree_path)
			self.assertEqual("|'%s'|" % value, deleted_subtree.inline_str())
		self.assertEqual("-", self.tree.inline_str())

	def test_no_internal_values(self):
		with self.assertRaises(KeyError):
			self.tree.apply_update(('1',), 'x')

	def test_apply_delete_deep(self):
		# Given the tree 1->1->1->1, ensure that delete((1,1,1)) deletes the whole path, rather
		# than leak dangling internal nodes
		for with_extra_path in (False, True):
			for i in range(1, 5):
				tree = Tree()
				tree.apply_update(['1'] * 4, 'x')
				if with_extra_path:
					tree.apply_update(('2',), 'y')

				deleted_subtree = tree.apply_delete(['1'] * i)
				expected_deleted_subtree = "(1=" * (4-i) + "|'x'|" + ")" * (4-i)
				self.assertEqual(expected_deleted_subtree, deleted_subtree.inline_str())
				if with_extra_path:
					self.assertEqual("(2=|'y'|)", tree.inline_str())
				else:
					self.assertEqual("-", tree.inline_str())

	def test_get_value(self):
		for exp, tree_path in (
				('y', ('1', '2')),
				(None, ('2',)),
			):
			self.assertEqual(exp, self.tree.get_value(tree_path))

	def test_traverse(self):
		for exp, tree_path in (
				(self.WHOLE_TREE, ()),
				(self.LEFT_TREE, ('1',)),
				("|'z'|", ('2', '1')),
			):
			self.assertEqual(exp, self.tree.traverse(tree_path).inline_str())

	def test_from_adjacency_dict(self):
		tree = Tree.from_adjacency_dict_and_values(
			{
				'': {'1': '1', '2': '2'},
				'1': {'1': '1.1', '2': '1.2'},
				'2': {'1': '2.1', '2': '2.2'},
			},
			{'1.1': 'x', '1.2': 'y', '2.1': 'z', '2.2': 't'},
			''
		)
		self.assertEqual(self.WHOLE_TREE, tree.inline_str())

	def test_bad_from_adjacency_dict(self):
		with self.assertRaises(AssertionError):
			Tree.from_adjacency_dict_and_values({'y': {}}, {}, '')

		with self.assertRaises(AssertionError):
			Tree.from_adjacency_dict_and_values({}, {'x': 'y'}, '')

	def test_copy(self):
		self.assertEqual(self.tree.copy().inline_str(), self.tree.inline_str())

	def test_get_leaves_with_values(self):
		tree_path = []
		actual_leaves = [
			(tuple(tree_path), value)
			for value in self.tree.get_leaves_with_values(tree_path)
		]
		self.assertEqual(self.LEAVES, actual_leaves)


class MyReader(RedisTreeReader):
	with_msgpack = False

	def __init__(self, **kwargs):
		super(MyReader, self).__init__(**kwargs)
		self.all_trees = {0: Tree()}
		self.got_new_tree = Event()
		self.syncs = queue.Queue()

		self.clear_recorded_callbacks()

	def clear_recorded_callbacks(self):
		self.update_callbacks = []
		# these callbacks are obtained by DFS-ing deleted_subtree, so there are
		# no order guarantees
		self.delete_callbacks = []
		self.delete_subtree_callbacks = []

	def handle_delete_subtree(self, timestamp, tree_path, deleted_subtree):
		assert self.get_xid() >= 0
		assert isinstance(tree_path, list)
		assert time() - timestamp < 0.1
		assert self.lag >= 0 and self.lag < 0.5
		self._logger.info(
			'got delete subtree xid=%d ts=%.2f tree_path=%r (%s)',
			self.get_xid(), timestamp, tree_path, self.tree.inline_str())
		self._store_tree()
		self.delete_subtree_callbacks.append((tuple(tree_path), deleted_subtree.inline_str()))

	def handle_delete(self, timestamp, tree_path, old_value):
		assert self.get_xid() >= 0
		assert isinstance(tree_path, list)
		assert time() - timestamp < 0.1
		assert self.lag >= 0 and self.lag < 0.5
		assert old_value is not None
		self._logger.info(
			'got delete xid=%d ts=%.2f tree_path=%r old_value=%r (%s)',
			self.get_xid(), timestamp, tree_path, old_value, self.tree.inline_str())
		self.delete_callbacks.append((tuple(tree_path), old_value))

	def _store_tree(self):
		if not self.is_synced():
			return

		if self.get_xid() in self.all_trees:
			# Make sure we didn't end up with two different trees and the same xid
			expected_tree = self.tree.inline_str()
			actual_tree = self.all_trees[self.get_xid()].inline_str()
			assert expected_tree == actual_tree, (expected_tree, actual_tree)
			return

		self.got_new_tree.set()
		self.all_trees[self.get_xid()] = self.tree.copy()

	def handle_update(self, timestamp, tree_path, old_value, value):
		assert self.get_xid() >= 0
		assert isinstance(tree_path, list)
		if not self.with_msgpack:
			assert isinstance(value, str)
		assert self.tree.get_value(tree_path) == value
		assert time() - timestamp < 0.1
		assert self.lag >= 0 and self.lag < 0.5
		self._logger.info(
			'got update xid=%d ts=%.2f tree_path=%r value=%r (%s)',
			self.get_xid(), timestamp, tree_path, value, self.tree.inline_str())
		self._store_tree()
		self.update_callbacks.append((tuple(tree_path), old_value, value))

	def handle_sync_lost(self):
		self._logger.info('sync lost')
		self.all_trees.clear() # shouldn't rely on xid-s being meaningful across different syncs
		self.clear_recorded_callbacks()

	def handle_synced(self, tree):
		sync_xid = self.get_xid()
		assert sync_xid >= 0
		self.syncs.put(sync_xid)
		self._store_tree()


class BasicTest(unittest.TestCase):
	def setUp(self):
		# Threads sometimes take a while to stop, and stop() in tearDown is non-blocking
		# Rather than waiting for threads to stop after each test, let's just make sure each
		# test writes to a different tree, thus making it impossible for a test to interfere
		# with another
		self.tree_name = uuid1().hex

		self.monkey_patches = [
			# GC faster, so we can test it
			mock.patch('treestream.backends.redis_lua.writer.GC_BUFFERING_WAIT', 0.0),
		]
		for patch in self.monkey_patches:
			patch.start()

		self.writer = RedisTreeWriter(
			logger=logging.getLogger(_GREEN + 'wr1' + _RESET),
			tree_name=self.tree_name,
			socket_timeout=0.2, # react to stop() faster
		)
		keys_to_delete = self.get_redis_keys()
		if keys_to_delete:
			self.writer.redis.delete(*keys_to_delete)

		# Use this to get a new sync everytime, creating the tree from scratch
		self.reader1 = MyReader(
			socket_timeout=0.2, # react to stop() faster
			logger=logging.getLogger(_YELLOW + 'rd1' + _RESET),
			tree_name=self.tree_name,
			sleep_time_when_restarting=0.0,
			stop_check_period=0.01,
		)

		# Only get a sync in the beginning, then update the tree from the stream
		self.reader2 = MyReader(
			logger=logging.getLogger(_BLUE + 'rd2' + _RESET),
			tree_name=self.tree_name,
			stop_check_period=0.01,
			socket_timeout=0.2, # react to stop() faster
		)

		self.threads = []
		for func in (self.reader1.run, self.reader2.run, self.writer.run):
			thread = Thread(target=func)
			thread.setDaemon(True)
			thread.start()
			self.threads.append(thread)

		# Wait & discard initial sync
		self.reader1.syncs.get()
		self.assert_tree('-')

	def get_tree_from_stream(self):
		while not self.reader2.synced or self.reader2.get_xid() < self.reader1.get_xid():
			sleep(0.001)
		return self.reader2.tree

	def get_tree(self):
		self.reader1.restart()
		xid = self.reader1.syncs.get(timeout=1.0)
		while xid not in self.reader2.all_trees:
			if not self.reader2.got_new_tree.wait(timeout=1.0):
				raise RuntimeError('did not get tree %d' % xid)
			self.reader2.got_new_tree.clear()

		tree1 = self.reader1.all_trees[xid]
		tree2 = self.reader2.all_trees[xid]

		tree1_str = tree1.inline_str()
		tree2_str = tree2.inline_str()

		assert tree1_str == tree2_str, (xid, tree1_str, tree2_str)
		return tree1_str

	def assert_tree(self, exp_tree):
		actual_tree = self.get_tree()
		assert exp_tree == actual_tree, (exp_tree, actual_tree)

	def assert_xid(self, xid):
		"""Should only be called after `assert_tree`, to guarantee that the readers have caught
		up."""
		assert xid == self.reader1.get_xid(), (xid, self.reader1.get_xid())
		assert xid == self.reader2.get_xid(), (xid, self.reader2.get_xid())

	def update(self, tree_path, value):
		return self.writer.update(tree_path, value)

	def update_many(self, base_tree_path, values):
		return self.writer.update_many(base_tree_path, values)

	def delete(self, tree_path):
		return self.writer.delete(tree_path)

	def test_change_value(self):
		self.update(('a', 'b', 'c'), '1')
		xid = self.writer.update(('a', 'b', 'c'), '2')
		self.assert_tree("(a=(b=(c=|'2'|)))")
		self.assert_xid(xid)

	def test_update_many(self):
		xids = self.update_many(['x', 'y'], TreeTest.LEAVES)

		self.assert_tree("(x=(y=%s))" % TreeTest.WHOLE_TREE)
		assert len(xids) == 4
		assert all(isinstance(xid, six.integer_types) for xid in xids)

	def get_redis_keys(self):
		return set(self.writer.redis.keys(self.tree_name + '::*'))

	def test_delete_whole_tree(self):
		my_tree = Tree()
		expected_update_callbacks = []
		expected_delete_callbacks = set()
		for i in range(32):
			# ('0', '0', '0', '0', '0') .. ('1', '1', '1', '1', '1')
			tree_path = tuple(bin(i)[2:].rjust(5, '0'))
			my_tree.apply_update(tree_path, str(i))
			self.update(tree_path, str(i))
			expected_update_callbacks.append((tree_path, None, str(i)))
			expected_delete_callbacks.add((tree_path, str(i)))

		# Ensure the tree we've constructed is the same as the one the reader has constructed
		self.assert_tree(my_tree.inline_str())
		# reader1 callbacks came from a sync, so we can't guarantee they came in the same order,
		# since the xid of the update is not stored in the tree.
		self.assertEqual(set(expected_update_callbacks), set(self.reader1.update_callbacks))
		self.assertEqual(expected_update_callbacks, self.reader2.update_callbacks)

		# delete whole tree
		self.delete(())
		self.assert_tree('-')
		self.assertEqual([((), my_tree.inline_str())], self.reader2.delete_subtree_callbacks)
		self.assertEqual(expected_delete_callbacks, set(self.reader2.delete_callbacks))

		sleep(0.5) # wait for the gc to run
		self.assert_redis_is_empty()

	def assert_redis_is_empty(self):
		redis_keys = self.get_redis_keys()
		assert redis_keys == {
			self.tree_name + '::xid', self.tree_name + '::ptr_counter'}, redis_keys

	def assert_redis_eventually_empty(self):
		for i in range(10):
			sleep(0.1)
			if len(self.get_redis_keys()) <= 2:
				break
		self.assert_redis_is_empty()

	def test_no_leaks(self):
		tree = TreeTest.LEAVES

		for tree_path, value in tree:
			self.update(tree_path, value)

		for tree_path, _value in tree:
			self.delete(tree_path)

		sleep(0.5) # wait for the gc to run
		self.assert_redis_is_empty()

	def test_callbacks(self):
		tree = TreeTest.LEAVES
		self.reader2.clear_recorded_callbacks()
		for tree_path, value in tree:
			self.update(tree_path, value)
		self.delete(('2',))

		self.assert_tree("(1=%s)" % TreeTest.LEFT_TREE)

		self.assertEqual(
			{
				(('1', '1'), None, 'x'),
				(('1', '2'), None, 'y'),
			},
			set(self.reader1.update_callbacks))
		self.assertEqual(
			[
				(tree_path, None, value)
				for tree_path, value in tree
			],
			self.reader2.update_callbacks)

		self.assertEqual(
			[(('2',), TreeTest.RIGHT_TREE)],
			self.reader2.delete_subtree_callbacks)

		self.assertEqual(
			{
				(('2', '1'), 'z'),
				(('2', '2'), 't'),
			},
			set(self.reader2.delete_callbacks))

	def test_no_internal_values(self):
		self.update(('a', 'b', 'c'), '1')

		with self.assertRaises(TreeWriterError):
			self.update(('a', 'b'), '1')

		with self.assertRaises(TreeWriterError):
			self.update(('a', 'b', 'c', 'd'), '1')

	def test_delete(self):
		self.update(('a', 'b', 'c', 'd'), '1')
		xid = self.delete(('a', 'b', 'c', 'd'))
		self.assert_tree('-')
		self.assert_xid(xid)

		self.update(('a', 'b', 'c'), '1')
		self.update(('a', 'b', 'd'), '2')
		self.update(('x',), '3')
		xid = self.delete(('a', 'b', 'c'))
		self.assert_tree("(a=(b=(d=|'2'|)), x=|'3'|)")
		self.assert_xid(xid)

	def test_delete_callback(self):
		self.update(('a', 'b', 'c', 'd'), '1')
		self.delete(('a', 'b'))
		self.assert_tree("-")

		# We should get a delete callback for the subtree we've called delete() on, not the
		# subtree that effectively got deleted.
		self.assertEqual(
			[(('a', 'b'), "(c=(d=|'1'|))")],
			self.reader2.delete_subtree_callbacks)

	def test_update(self):
		self.update(('a', 'b', 'c'), '1')
		self.update(('a', 'b', 'd'), '2')
		self.update(('a', 'c'), '3')
		self.update(('x', 'b', 'c'), '4')
		xid = self.update(('x', 'b', 'c'), '5')
		self.assert_tree("(a=(b=(c=|'1'|, d=|'2'|), c=|'3'|), x=(b=(c=|'5'|)))")
		self.assert_xid(xid)

		xid = self.delete(('a',))
		self.assert_tree("(x=(b=(c=|'5'|)))")
		self.assert_xid(xid)

	def tearDown(self):
		self.reader1.stop()
		self.reader2.stop()

		self.delete(())
		self.assert_redis_eventually_empty()

		self.writer.stop()

		for patch in self.monkey_patches:
			patch.stop()


class MyMsgpackReader(MsgpackReaderMixin, MyReader):
	with_msgpack = True


class MyMsgpackWriter(MsgpackWriterMixin, RedisTreeWriter):
	pass


class MsgpackTest(unittest.TestCase):
	def setUp(self):
		self.writer = MyMsgpackWriter(tree_name='msgpack-test')
		self.reader = MyMsgpackReader(tree_name='msgpack-test')

		for func in (self.writer.run, self.reader.run):
			thread = Thread(target=func)
			thread.setDaemon(True)
			thread.start()

	def test_msgpack(self):
		self.reader.syncs.get()
		value1 = [1, 2]
		value2 = {1: 2, 3: 4}
		self.writer.update_many(
			[1, 2],
			[
				((5, 6.0, '7'), value1),
				((3, 4.5), value2),
			])
		self.writer.delete((1,))
		sleep(0.1)
		path1 = (1, 2, 5, 6.0, '7')
		path2 = (1, 2, 3, 4.5)
		self.assertEqual(
			[(path1, None, value1), (path2, None, value2)],
			self.reader.update_callbacks)

		expected_deletions = [(path1, value1), (path2, value2)]
		self.assertEqual(sorted(expected_deletions), sorted(self.reader.delete_callbacks))

	def tearDown(self):
		redis = self.writer.redis
		redis.delete(*redis.keys('msgpack-test::*'))

if __name__ == '__main__':
	logging.basicConfig(
		stream=sys.stderr,
		level=logging.DEBUG,
		format='%(name)3s %(asctime)-15s %(message)s')
	unittest.main()
