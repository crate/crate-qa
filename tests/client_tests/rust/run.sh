#!/usr/bin/env bash
set -Eeuo pipefail

cr8 run-crate latest-nightly \
  -- @cargo run
