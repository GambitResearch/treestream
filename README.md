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

This currently only works with Python2, and the only currently supported
backend is Redis (currently via Lua scripts, but should ideally be rewritten
as a C module in the future).

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
