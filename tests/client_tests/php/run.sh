#!/bin/bash

cr8 run-crate latest-nightly \
    -- @php $(dirname "$0")/test_deallocate.php \
            '{node.addresses.psql.host}' \
            '{node.addresses.psql.port}' 

