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

Create virtualenv:

```
python3.6 -m venv env
source env/bin/activate
```

Install requirements:

```
pip install -e .
```

## Usage

Run all Test Cases (tests that are prefixed with `test_*.py`) inside `tests/restart/`

```bash
cd tests/restart/
$ python3.6 -m unittest -v
```

Run a specific test method (e.g. `test_query_partitioned_table`)

```bash
$ python3.6 -m unittest -v test_partitions.PartitionTestCase.test_query_partitioned_table
```

