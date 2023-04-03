#!/usr/bin/env bash

cr8 run-crate latest-nightly -s psql.port=5432 \
  -- @npm test
