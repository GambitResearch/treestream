from __future__ import absolute_import

from treestream.backends.redis_lua.constants import (
	PROTO_VERSION,
	OP_UPDATE,
	OP_DELETE
)

DEFINE_REDIS_KEYS = """
	local xid_key = tree_name .. '::xid'
	local root_ptr_key = tree_name .. '::root'
	local ptr_counter_key = tree_name .. '::ptr_counter'
	local pubsub_channel = tree_name .. '::pubsub'
	local children_prefix_key = tree_name .. '::children::'
	local values_key = tree_name .. '::values'
	local deferred_delete_queue_key = tree_name .. '::deferred_delete_queue'
"""

UPDATE = ("""
	-- ARGV = [k1, k2, .., kn, tree_name, ts, value]

	local tree_name = ARGV[#ARGV - 2]
	local ts = ARGV[#ARGV - 1]
	local value = ARGV[#ARGV]

	%(define_redis_keys)s

	if (redis.call('EXISTS', root_ptr_key) < 1) then
		redis.call('SET', root_ptr_key, redis.call('INCR', ptr_counter_key))
	end

	local tree_path = {}
	-- length of the tree path
	local n = #ARGV - 3

	local crt = redis.call('GET', root_ptr_key)
	for i=1,n do
		local has_value = redis.call('HEXISTS', values_key, crt)
		if (has_value == 1) then
			return 'Update would make leaf that holds a value into an internal node'
		end
		local children_key = children_prefix_key .. crt
		crt = redis.call('HGET', children_key, ARGV[i])
		table.insert(tree_path, ARGV[i])
		if (not crt) then
			crt = redis.call('INCR', ptr_counter_key)
			redis.call('HSET', children_key, ARGV[i], crt)
		end
	end

	local has_children = redis.call('EXISTS', children_prefix_key .. crt)
	if (has_children == 1) then
		return 'Update would set value to internal node'
	end
	redis.call('HSET', values_key, crt, value)

	local xid = redis.call('INCR', xid_key)

	redis.call(
		'PUBLISH',
		pubsub_channel,
		cmsgpack.pack({
			%(proto_version)s,
			xid,
			tonumber(ts),
			%(update_opcode)s,
			tree_path,
			value
		})
	)
	return xid
""" % dict(
	define_redis_keys=DEFINE_REDIS_KEYS,
	proto_version=PROTO_VERSION,
	update_opcode=OP_UPDATE,
)).strip()

DELETE = ("""
	-- ARGV = [k1, k2, ..., kn, tree_name, ts]

	local tree_name = ARGV[#ARGV - 1]
	local ts = ARGV[#ARGV]

	%(define_redis_keys)s

	local trace = {}
	local crt = redis.call('GET', root_ptr_key)
	table.insert(trace, crt)
	if (not crt) then
		return nil
	end

	-- length of the tree path
	local n = #ARGV - 2
	local tree_path = {}

	-- go down the tree by following edges with labels indicated in ARGV[1..n]
	-- trace[1] --ARGV[1]--> trace[2] --ARGV[2]--> ... --> trace[n] --ARGV[n]--> trace[n + 1]
	for i=1,n do
		crt = redis.call('HGET', children_prefix_key .. crt, ARGV[i])
		if (not crt) then
			return nil
		end
		table.insert(trace, crt)
		table.insert(tree_path, ARGV[i])
	end

	-- schedule a deletion of subtree rooted at trace[n + 1]
	redis.call('LPUSH', deferred_delete_queue_key, trace[n + 1])
	-- delete the value of node at level n + 1
	redis.call('HDEL', values_key, trace[n + 1])

	local deleted_prefix = n
	for i=n,1,-1 do
		-- delete the link: trace[i] --ARGV[i]--> trace[i + 1]
		local children_key = children_prefix_key .. trace[i]
		local removed = redis.call('HDEL', children_key, ARGV[i])
		if (not removed) then
			break
		end

		-- if we've deleted the last child of `trace[i]`, continue deleting upwards
		local remaining = redis.call('HLEN', children_key)
		if (remaining > 0) then
			break
		end
		deleted_prefix = i - 1
	end

	-- delete the whole tree
	if (deleted_prefix == 0) then
		redis.call('DEL', root_ptr_key)
	end

	local xid = redis.call('INCR', xid_key)

	-- O(|subscribers|)
	redis.call(
		'PUBLISH',
		pubsub_channel,
		cmsgpack.pack({
			%(proto_version)s,
			xid,
			tonumber(ts),
			%(delete_opcode)s,
			tree_path
		})
	)

	return xid
""" % dict(
	proto_version=PROTO_VERSION,
	delete_opcode=OP_DELETE,
	define_redis_keys=DEFINE_REDIS_KEYS,
)).strip()

_ENSURE_NONEMPTY_LIST = """
	-- LRANGE and LTRIM don't like working on an empty list
	local len = redis.call('LLEN', {src_list})
	if (len == 0) then
		return {{}}
	end
"""

# Like RPOPLPUSH, but pop multiple items at once, and SADD them into a set instead of LPUSH
# into a list.
# KEYS = [src_list, dest_set]
# ARGV = [num_elements]
RPOP_SADD_MANY = (_ENSURE_NONEMPTY_LIST + """
	local items = redis.call('LRANGE', {src_list}, -{num_elements}, -1)
	redis.call('LTRIM', {src_list}, 0, -{num_elements} - 1)
	redis.call('SADD', {dest_set}, unpack(items))
	return items
""").format(
	src_list='KEYS[1]',
	dest_set='KEYS[2]',
	num_elements='ARGV[1]',
)

# Move all elements from `src_list` into `dest_set`
# KEYS = [src_list, dest_set]
LIST_INTO_SET = (_ENSURE_NONEMPTY_LIST + """
	local items = redis.call('LRANGE', {src_list}, 0, -1)
	redis.call('DEL', {src_list})
	redis.call('SADD', {dest_set}, unpack(items))
""").format(
	src_list='KEYS[1]',
	dest_set='KEYS[2]',
)
