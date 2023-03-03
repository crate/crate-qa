#!/bin/bash
#
# Use cr8 to invoke a pytest test suite.
#
# The custom options `--http-host`, `--http-port`, etc.,
# are defined within the `conftest.py` file.
#

set -ae
cr8 run-crate latest-nightly \
    -- @pytest -vvv \
        --http-host '{node.addresses.http.host}' \
        --http-port '{node.addresses.http.port}' \
        --psql-host '{node.addresses.psql.host}' \
        --psql-port '{node.addresses.psql.port}' \
    -- @echo "SUCCESS"
