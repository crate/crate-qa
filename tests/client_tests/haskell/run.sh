#!/bin/bash

DIR=$(dirname "$0")
(cd $DIR && cr8 run-crate latest-nightly \
    -- @stack build --exec "crate-qa-hask {node.addresses.psql.host} {node.addresses.psql.port}")
