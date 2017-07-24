SQL Logic Tests
===============

This project allows to run the test files from
https://github.com/crate/sqllogictest against crate and postgresql
servers.

This package requires Python installed with at least version 3.6.

In order to run it sandboxed in the project directory do the following::

    python -m venv .venv
    python -m ./venv/bin/pip install -e .

Run it:

    ./venv/bin/sqllogigtest --help
