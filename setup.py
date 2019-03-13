#!/usr/bin/env python

from setuptools import setup

EXTRAS_REQUIRE = {
	# Use the C library hiredis to speed up decoding of redis data
	':platform_python_implementation=="CPython"': ['hiredis'],
	':platform_python_implementation=="PyPy"': ['msgpack-pypy'],
	':platform_python_implementation!="PyPy"': ['msgpack-python'],
}

setup(
	name='treestream',
	use_scm_version={'version_scheme': 'post-release'},
	author='Gambit Research',
	packages=[
		'treestream',
		'treestream.backends',
		'treestream.backends.redis_lua'
	],
	install_requires=['mock', 'redispy', 'six'],
	extras_require=EXTRAS_REQUIRE,
	setup_requires=['setuptools_scm'],
	author_email='devteam@gambitresearch.com',
	url='https://github.com/GambitResearch/treestream',
	license='MIT',
)
