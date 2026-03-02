#!/bin/bash

go mod init main
go mod tidy

dir=$(dirname "$0")

cr8 run-crate latest-nightly \
    -- @crash --hosts '{node.http_url}' < "$dir/setup.sql" \
    -- @go run "$dir/basic_queries.go" \
        --hosts '{node.addresses.psql.host}' \
        --port '{node.addresses.psql.port}' \
    -- @go run "$dir/bulk_operations.go" \
        --hosts '{node.addresses.psql.host}' \
        --port '{node.addresses.psql.port}' \
    -- @echo "SUCCESS"
