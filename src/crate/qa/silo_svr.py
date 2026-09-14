#!/usr/bin/env python3

import os
import platform
import shutil
import socket
import tarfile
import tempfile
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


class SiloServer:
    """Runs https://github.com/pgsty/silo, a MinIO-compatible S3 server."""

    SILO_RELEASE_TAG = 'RELEASE.2026-09-03T13-18-01Z'
    SILO_RELEASE_VERSION = '20260903131801.0.0'

    SILO_ASSET_SUFFIXES = {
        'Linux-x86_64': 'linux_amd64.tar.gz',
        'Darwin-x86_64': 'darwin_amd64.tar.gz',
    }

    CACHE_ROOT = Path(os.environ.get('XDG_CACHE_HOME', os.path.join(os.path.expanduser('~'), '.cache')))
    CACHE_DIR = CACHE_ROOT / 'crate-tests'

    def __init__(self):
        self.silo_path = self._get_silo()
        self.data_dir = data_dir = Path(tempfile.mkdtemp())
        # Create base_path
        os.makedirs(data_dir / 'backups')
        self.process = None

    def _get_silo(self):
        silo_dir = SiloServer.CACHE_DIR / 'silo'
        silo_path = silo_dir / 'silo'
        if not os.path.exists(silo_path):
            os.makedirs(silo_dir, exist_ok=True)
            suffix = SiloServer.SILO_ASSET_SUFFIXES[f'{platform.system()}-{platform.machine()}']
            asset_name = f'silo_{SiloServer.SILO_RELEASE_VERSION}_{suffix}'
            asset_url = (
                f'https://github.com/pgsty/silo/releases/download/'
                f'{SiloServer.SILO_RELEASE_TAG}/{asset_name}'
            )
            archive_path, _ = urlretrieve(asset_url)
            try:
                with tarfile.open(archive_path, 'r:gz') as tar:
                    tar.extract('silo', path=silo_dir)
            finally:
                os.remove(archive_path)
        silo_path.chmod(0o755)
        return silo_path

    def run(self):
        cmd = [self.silo_path, 'server', str(self.data_dir)]
        env = os.environ.copy()
        env['MINIO_ROOT_USER'] = 'silo'
        env['MINIO_ROOT_PASSWORD'] = 'silostorage'
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
