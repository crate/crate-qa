#!/usr/bin/env bash
set -Eeuo pipefail

cr8 run-crate branch:j/jdk22 \
  -- @cargo run {node.addresses.psql.port}
