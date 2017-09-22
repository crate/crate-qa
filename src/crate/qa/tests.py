#!/usr/bin/env python3

import shutil
import tempfile
import unittest

from cr8.run_crate import CrateNode, get_crate


class NodeProvider:

    def setUp(self):
        self._path_data = tempfile.mkdtemp()
        print(f'data path: {self._path_data}')
        self._on_stop = []

        def new_node(version, settings={}):
            s = dict({
                'path.data': self._path_data,
                'cluster.name': 'crate-bwc-tests'
            })
            s.update(settings)
            n = CrateNode(
                crate_dir=get_crate(version),
                keep_data=True,
                settings=s,
            )
            self._on_stop.append(n.stop)
            return n
        self._new_node = new_node

    def tearDown(self):
        print(f'Removing: {self._path_data}')
        shutil.rmtree(self._path_data, ignore_errors=True)
        self._process_on_stop()

    def _process_on_stop(self):
        for to_stop in self._on_stop:
            to_stop()
        self._on_stop.clear()
