#!/usr/bin/env bash
set -Eeuo pipefail


dotnet build
cr8 run-crate latest-nightly \
  -- @$(pwd)/bin/Debug/net8.0/stock_npgsql {node.addresses.psql.host} {node.addresses.psql.port}
