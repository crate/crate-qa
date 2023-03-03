import pytest
from crate.crash.command import CrateShell


@pytest.fixture(autouse=True)
def setup(cratedb_http_url):
    """
    Provision database with `setup.sql`.
    """
    with CrateShell(cratedb_http_url) as crash:
        with open("setup.sql") as setup_sql:
            return crash.process_iterable(setup_sql)


def pytest_addoption(parser):
    """
    Register custom options to support invocation by cr8.
    Use cr8 to invoke a pytest test suite, see `run.sh`.

    Example::

        pytest -vvv --http-host 127.0.0.1 --http-port 4200 --psql-host 127.0.0.1 --psql-port 5432

    https://github.com/mfussenegger/cr8
    """
    parser.addoption("--http-url", action="store", default="localhost:4200")
    parser.addoption("--http-host", action="store", default="localhost")
    parser.addoption("--http-port", action="store", default="4200")
    parser.addoption("--psql-host", action="store", default="localhost")
    parser.addoption("--psql-port", action="store", default="5432")


@pytest.fixture
def cratedb_http_url(pytestconfig):
    return pytestconfig.getoption("--http-url")


@pytest.fixture
def cratedb_http_host(pytestconfig):
    return pytestconfig.getoption("--http-host")


@pytest.fixture
def cratedb_http_port(pytestconfig):
    return pytestconfig.getoption("--http-port")


@pytest.fixture
def cratedb_psql_host(pytestconfig):
    return pytestconfig.getoption("--psql-host")


@pytest.fixture
def cratedb_psql_port(pytestconfig):
    return pytestconfig.getoption("--psql-port")
