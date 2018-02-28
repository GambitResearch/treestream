from __future__ import absolute_import

from threading import current_thread, Thread, Event, RLock
from time import sleep
from functools import partial

def trunc_str(val, max_size):
	if len(val) > max_size - 2:
		return val[:max_size - 2] + '..'
	return val

identity = lambda val: val

def run_threads(targets):
	threads = []
	for name, target in targets:
		thread = Thread(name=name, target=target)
		thread.setDaemon(True)
		thread.start()
		threads.append(thread)

	for thread in threads:
		thread.join()


class ThreadBase(object):
	name = NotImplemented
	must_always_run = True # stop all other threads if this one stops

	def run(self):
		raise NotImplementedError


class RunnableAndStoppableMixin(object):
	"""Helps manage a bunch of threads that can be restarted and stopped.

	Users need to implement:
		_get_threads_to_run()
		_logger

	Optional:
		_pre_run()
		_stop_or_restart()
	"""
	STOP_RESTART = object()
	STOP_NORESTART = object()

	def __init__(self, sleep_time_when_restarting=1.0, **kwargs):
		self.is_running = False
		self._stop_requested = Event()
		self._stop_requested_type = None
		self._runnable_lock = RLock()
		self._sleep_time_when_restarting = sleep_time_when_restarting
		super(RunnableAndStoppableMixin, self).__init__(**kwargs)

	def get_threads_to_run(self):
		"""
		Get threads and synchronous runnable functions for running tree stream.
		"""
		self._pre_run()
		for thread in self._get_threads_to_run():
			yield thread, partial(self._run_iterator, thread)

	def _run_iterator(self, thread):
		for sleep_time in thread.run():
			sleep(sleep_time or 0)

	def _get_threads_to_run(self):
		"""
		Returns: List[thread: ThreadBase]
		"""
		return []

	def _pre_run(self):
		pass

	def _stop_or_restart(self):
		pass

	def _thread_main(self, func, must_always_run):
		"""
			:param func: A generator containing the main body of the thread
				We check if the thread should stop every-time it yields.
				If it yields with a number, we sleep for that amount.
		"""
		try:
			thread_name = current_thread().getName()
			logger = self._logger
			logger.info('%s: STARTING', thread_name)
			# TODO this construct doesn't work very well in PyPy, because the "finally"
			# block is only executed upon garbage collection. Thus, we should use
			# generator.send(should_stop) on this side, and
			# "should_stop = yield; if should_stop: return" on the other.
			for sleep_time in func():
				sleep(0) # yield to other thread
				if (
						sleep_time is None and self.should_stop() or
						sleep_time is not None and self.stoppable_sleep(sleep_time)):
					logger.info('%s: got stop request', thread_name)
					break
		except Exception:
			logger.exception('%s: ERROR', thread_name)
			# One of the threads has failed, restart all threads
			self.restart()
			raise
		finally:
			logger.info('%s: STOPPED', thread_name)
			if must_always_run:
				self.stop()

	def run(self):
		with self._runnable_lock:
			assert not self.is_running
			self.is_running = True

		try:
			while True:
				with self._runnable_lock:
					stop_requested = self._stop_requested.is_set()
					stop_requested_type = self._stop_requested_type
					if stop_requested and stop_requested_type is self.STOP_NORESTART:
						break
					self._stop_requested.clear()
					self._stop_requested_type = None

				self._logger.info('starting')
				# state = self._build_initial_state()
				self._pre_run()
				run_threads([
					(thread.name, partial(self._thread_main, thread.run, thread.must_always_run))
					for thread in self._get_threads_to_run()
				])

				# Wait a while before restarting
				sleep(self._sleep_time_when_restarting)
		finally:
			self.is_running = False

	def should_stop(self):
		return self._stop_requested.is_set()

	def _set_stop(self, value):
		with self._runnable_lock:
			if self._stop_requested_type is not self.STOP_NORESTART:
				# Allow escalation from restart to stop
				self._stop_requested_type = value
			if not self._stop_requested.is_set():
				self._stop_requested.set()
				self._stop_or_restart()

	def stop(self):
		self._logger.info('stop requested')
		self._set_stop(self.STOP_NORESTART)

	def restart(self):
		self._logger.info('restart requested')
		self._set_stop(self.STOP_RESTART)

	def stoppable_sleep(self, timeout):
		"""Returns True if the sleep was interrupted by a stop request, False otherwise."""
		return self._stop_requested.wait(timeout=timeout)
