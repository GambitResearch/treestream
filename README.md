Treestream
============
Treestream is a Python library for keeping hierarchical data synchronised via a
stream of incremental changes.

The data structure used is a [tree](treestream/tree.py) with labeled edges and
where the leaf nodes contain arbitrary data.

The operations supported by a *writer* are updating the value of a leaf node in
the tree (described by the path of edge labels needed to reach it) and deleting
an entire subtree.

A *reader*, upon creation, obtains a *sync* from redis with the current state
of the tree, then proceeds to listen to the stream and apply the incremental
changes from the writers, providing callbacks to the user.

This mostly works with Python2 and Python3, but the only currently supported
backend is Redis (currently via Lua scripts, but should ideally be rewritten
as a C module in the future).

In Python3, labels and values are returned as `bytes` by default. You can
override the methods `_decode_edge_label` and `_decode_value` on your
`TreeReader` instance to change the return type, these functions should expect
a single positional argument of type `bytes`. In Python2, the default return
type is `str` instead.

Installation
============
```
$ virtualenv foo && . foo/bin/activate
$ pip install .
$ docker run --net=host redis:4 --save '' --appendonly no &
$ python test/test_treestream.py
$ python example.py
```

Usage
============
See the [basic usage example](example.py) and the docstrings of the
[writer](treestream/writer.py) and the [reader](treestream/reader.py).
