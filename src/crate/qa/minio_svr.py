#!/usr/bin/env python3

import os
import platform
import shutil
import tempfile
import socket
from pathlib import Path
from subprocess import Popen, PIPE, DEVNULL
from urllib.request import urlretrieve


def _is_up(host: str, port: int) -> bool:
    try:
        conn = socket.create_connection((host, port))
        conn.close()
        return True
    except (socket.gaierror, ConnectionRefusedError):
        return False


class MinioServer:

    MINIO_URLS = {
        'Linux-x86_64': 'https://dl.min.io/server/minio/release/linux-amd64/minio',
        'Darwin-x86_64': 'https://dl.min.io/server/minio/release/darwin-amd64/minio'
    }

    CACHE_ROOT = Path(os.environ.get('XDG_CACHE_HOME', os.path.join(os.path.expanduser('~'), '.cache')))
    CACHE_DIR = CACHE_ROOT / 'crate-tests'

    def __init__(self):
        self.minio_path = self._get_minio()
        self.data_dir = data_dir = Path(tempfile.mkdtemp())
        # Create base_path
        os.makedirs(data_dir / 'backups')
        self.process = None

    def _get_minio(self):
        minio_dir = MinioServer.CACHE_DIR / 'minio'
        minio_path = minio_dir / 'minio'
        if not os.path.exists(minio_path):
            os.makedirs(minio_dir, exist_ok=True)
            minio_url = MinioServer.MINIO_URLS[f'{platform.system()}-{platform.machine()}']
            urlretrieve(minio_url, minio_path)
        minio_path.chmod(0o755)
        return minio_path

    def run(self):
        cmd = [self.minio_path, 'server', str(self.data_dir)]
        env = os.environ.copy()
        env['MINIO_ACCESS_KEY'] = 'minio'
        env['MINIO_SECRET_KEY'] = 'miniostorage'
        self.process = Popen(
            cmd,
            stdin=DEVNULL,
            stdout=PIPE,
            stderr=PIPE,
            env=env,
            universal_newlines=True
        )

    def close(self):
        if self.process:
            self.process.terminate()
            self.process.communicate(timeout=10)
            self.process = None
        shutil.rmtree(self.data_dir)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
