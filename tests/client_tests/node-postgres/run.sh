#!/usr/bin/env bash

cr8 run-crate latest-nightly \
  -- @node app.js {node.addresses.psql.host} {node.addresses.psql.port}
