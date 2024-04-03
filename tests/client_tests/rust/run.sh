#!/usr/bin/env bash
set -Eeuo pipefail

cr8 run-crate branch:j/graal \
  -- @cargo run {node.addresses.psql.port}
