#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools
from setuptools import setup


def read(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return f.read()
    except IOError:
        return ''


readme = functools.partial(read, 'README.rst')


setup(
    name='crate-qa',
    author='Crate.io Team',
    author_email='office@crate.io',
    url='https://github.com/crate/crate-qa',
    description='A collection of quality ensurance tests for CrateDB',
    long_description=readme(),
    entry_points={
        'console_scripts': []
    },
    package_dir={'': 'src'},
    packages=['crate.qa'],
    namespace_packages=['crate'],
    install_requires=[
        'cr8>=0.15.0',
        'Cython',
        'asyncpg>=0.18.2',
        'pyodbc',
        'psycopg2-binary==2.7.5'
    ],
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    use_scm_version=True,
    setup_requires=['setuptools_scm']
)
