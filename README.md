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
pip install -e .
```

## Test Suites

Tests can be run by either changeing into the test directory and run `python -m
unittest` or by executing the tests.py file directly with the Python
interpreter.

```
cd tests/<dir>
python -m unittest
```

or:

```
python tests/<dir>/tests.py
```
