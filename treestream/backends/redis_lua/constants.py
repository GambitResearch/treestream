PROTO_VERSION = 1
OP_DELETE = 0
OP_UPDATE = 1

# Reduce the number of roundtrips by querying (HSCAN-ing) for the children of many nodes in a
# non-transactional pipeline
BFS_MAX_QUERIES_PER_PIPELINE = 1000

# Maximum number of arguments to HMGET when syncing a tree
SYNC_MULTIGET_BATCH_SIZE = 100

# Maximum nb of nodes to pop from redis at once from the gc queue
GC_QUEUE_POP_SIZE = 500

# Maximum DEL/UNLINK calls to make in a non-transactional pipeline when gc-ing
GC_DELETE_BATCH_SIZE = 100

# Seconds to wait for more nodes to gc after we've received our first node to gc
GC_BUFFERING_WAIT = 0.5
