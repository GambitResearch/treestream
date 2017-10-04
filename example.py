from __future__ import print_function

from treestream import RedisTreeWriter

from threading import Thread, Lock
from time import sleep
from random import random, randrange

from treestream import (
	RedisTreeReader,
	RedisTreeWriter,
)

print_lock = Lock()

def start_thread(func):
	thread = Thread(target=func)
	thread.setDaemon(True)
	thread.start()
	return thread


def tprint(*args):
	with print_lock:
		print(*args)


class MyReader(RedisTreeReader):
	def  __init__(self, reader_name, **kwargs):
		super(MyReader, self).__init__(**kwargs)
		self.reader_name = reader_name

	def handle_update(self, timestamp, tree_path, old_value, value):
		tprint(self.reader_name, tree_path, '->', value)

	def handle_delete(self, timestamp, tree_path, old_value):
		tprint(self.reader_name, tree_path, '-> None')

	def handle_delete_subtree(self, timestamp, tree_path, deleted_subtree):
		pass

	def handle_sync_lost(self):
		tprint(self.reader_name, 'sync-lost')

	def handle_synced(self, tree):
		tprint(self.reader_name, 'synced')
		with print_lock:
			tree.pretty_print(depth=4)


def writer1():
	'''updates the value of a leaf node every ~100ms'''
	wr = RedisTreeWriter()
	start_thread(wr.run)

	wr.delete(()) # clear the tree

	def loop():
		while True:
			tree_path = [str(randrange(3)) for _ in xrange(3)]
			value = str(randrange(1000,10000))
			wr.update(tree_path, value)
			sleep(random() * 0.1)
	start_thread(loop)
	return wr


def writer2():
	'''deletes a subtree every ~5s'''
	wr = RedisTreeWriter()
	start_thread(wr.run)

	def loop():
		while True:
			tree_path_prefix = [str(randrange(3)) for _ in xrange(randrange(4))]
			tprint('DELETING', tree_path_prefix)
			wr.delete(tree_path_prefix)
			sleep(random() * 5.0)
	start_thread(loop)
	return wr


def reader1():
	rd = MyReader('reader1')
	start_thread(rd.run)
	return rd


def reader2():
	rd = MyReader('reader2')
	start_thread(rd.run)
	return rd


def main():
	wr1 = writer1()
	wr2 = writer2()
	rd1 = reader1()
	sleep(5.0)
	tprint('reader2 starting')
	rd2 = reader2() # start later, so that we get a sync first
	sleep(5.0)
	wr1.stop()
	wr2.stop()
	rd1.stop()
	rd2.stop()

if __name__ == '__main__':
	main()
