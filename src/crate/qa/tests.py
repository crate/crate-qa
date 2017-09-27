import os
import shutil
import tempfile
import unittest

from cr8.run_crate import CrateNode, get_crate


class NodeProvider:

    def __init__(self, *args, **kwargs):
        self.tmpdirs = []
        super().__init__(*args, **kwargs)

    def mkdtemp(self, *args):
        tmp = tempfile.mkdtemp()
        self.tmpdirs.append(tmp)
        return os.path.join(tmp, *args)

    def setUp(self):
        self._path_data = self.mkdtemp()
        self._on_stop = []

        def new_node(version, settings={}):
            s = dict({
                'path.data': self._path_data,
                'cluster.name': 'crate-bwc-tests'
            })
            s.update(settings)
            print(f'# Running CrateDB {version} with settings: {s}')
            n = CrateNode(
                crate_dir=get_crate(version),
                keep_data=True,
                settings=s,
            )
            self._on_stop.append(n.stop)
            return n
        self._new_node = new_node

    def tearDown(self):
        for tmp in self.tmpdirs:
            print(f'# Removing temporary directory {tmp}')
            shutil.rmtree(tmp, ignore_errors=True)
        self._process_on_stop()

    def _process_on_stop(self):
        for to_stop in self._on_stop:
            to_stop()
        self._on_stop.clear()
