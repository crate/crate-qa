#!/bin/bash

docker build -t local/cratedb-qa-python-pandas .
docker run --rm -it \
    --volume="$(pwd):/root" \
    --volume="$(pwd)/.cache:/root/.cache" \
    local/cratedb-qa-python-pandas

# TODO: Make this work by fixing cr8 to use a per-platform
#       directory when downloading CrateDB tarballs.
# --volume="${HOME}/.cache:/root/.cache"
