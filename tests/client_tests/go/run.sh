#!/bin/bash

GO111MODULE=on cr8 run-crate latest-nightly \
    -- @crash --hosts '{node.http_url}' < $(dirname "$0")/setup.sql \
    -- @go run $(dirname "$0")/basic_queries.go \
        --hosts '{node.addresses.psql.host}' \
        --port '{node.addresses.psql.port}' \
    -- @go run $(dirname "$0")/bulk_operations.go \
        --hosts '{node.addresses.psql.host}' \
        --port '{node.addresses.psql.port}' \
    -- @echo "SUCCESS"
