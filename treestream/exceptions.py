class TreeWriterError(RuntimeError):
	pass


class RetryableTreeWriterError(TreeWriterError):
	pass


class TreeReaderError(RuntimeError):
	pass
