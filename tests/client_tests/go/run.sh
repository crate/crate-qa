#!/bin/bash

go get "github.com/jackc/pgx"
cr8 run-crate latest-nightly \
    -- @crash --hosts '{node.http_url}' < $(dirname "$0")/pg_type_workaround.sql \
    -- @go run $(dirname "$0")/basic_queries.go \
        --hosts '{node.addresses.psql.host}' \
        --port '{node.addresses.psql.port}'
