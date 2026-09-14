#!/usr/bin/env python3

import os
import platform
import shutil
import tempfile
import socket
import tarfile
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
        'Linux-x86_64': 'https://github.com/pgsty/silo/releases/download/RELEASE.2026-09-03T13-18-01Z/silo_20260903131801.0.0_linux_amd64.tar.gz',
        'Linux-aarch64': 'https://github.com/pgsty/silo/releases/download/RELEASE.2026-09-03T13-18-01Z/silo_20260903131801.0.0_linux_arm64.tar.gz',
        'Darwin-x86_64': 'https://github.com/pgsty/silo/releases/download/RELEASE.2026-09-03T13-18-01Z/silo_20260903131801.0.0_darwin_amd64.tar.gz',
        'Darwin-arm64': 'https://github.com/pgsty/silo/releases/download/RELEASE.2026-09-03T13-18-01Z/silo_20260903131801.0.0_darwin_arm64.tar.gz'
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
        minio_path = minio_dir / 'silo'
        if not os.path.exists(minio_path):
            os.makedirs(minio_dir, exist_ok=True)
            minio_url = MinioServer.MINIO_URLS[f'{platform.system()}-{platform.machine()}']
            minio_download = minio_dir / "silo.tar.gz"
            urlretrieve(minio_url, minio_download)
            with tarfile.open(minio_download, "r:gz") as tf:
                tf.extract("silo", path=minio_dir)
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
