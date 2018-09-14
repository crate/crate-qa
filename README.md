# Quality Assurance (QA) for CrateDB

This repository contains frameworks to validate various aspects of CrateDB
accross different versions.

## Installation

This project requires **Python 3.6** or greater.

Check out repository:

```
git clone https://github.com/crate/crate-qa.git
cd crate-qa
```

Create virtualenv and install requirements:

```
python3.6 -m venv env
source env/bin/activate
# optional: pip install --upgrade pip wheel
pip install --upgrade -e .
```

### ODBC Library Installation

To successfully run the `client_tests/odbc` tests, the `postgresql odbc` (and its dependency `unixodbc`)
library must be installed first.

### MacOS

Recommended way is to use [brew] or [macports] for installation.

`brew install psqlodbc`

or

`sudo port install psqlodbc`

### Linux

Use the package manager of your distribution of choice for installation.

Examples:

Apt:

`apt-get install odbc-postgresql unixodbc-dev`

Yum:

`yum install postgresql-odbc unixODBC-devel`


## Test Suites

* `startup/`: test CrateDB startup with critical settings provided by CLI and crate.yml
* `restart/`: test that metadata/partitions/blobs are persisted across cluster restarts
* `bwc/`: backwards compatibility tests
* `client_tests/`: smoke test various clients written in Python, Go, etc.

### Usage

Tests can be run by changing into the test directory and run `python -m
unittest`. See `python -m unittest --help` for further options.

Run all test cases (tests in Python files that are prefixed with `test_`)
inside `tests/` and subfolders.

```bash
cd tests/
$ python3.6 -m unittest -v
```

Run a specific test method (e.g. `restart.test_query_partitioned_table`)

```bash
$ python3.6 -m unittest -v restart.test_partitions.PartitionTestCase.test_query_partitioned_table
```

[brew]: https://brew.sh/
[macports]: https://www.macports.org/