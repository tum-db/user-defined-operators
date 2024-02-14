#!/bin/bash

set -eu

cxxudo_deps="/opt/umbra-cxxudo-deps"

exec 'g++-12' \
    '-std=c++20' -fPIC -march=native -fno-exceptions -Wall -Wextra \
    -I "/opt/umbra/udo-runtime/cxx" \
    -DUDO_STANDALONE \
    "$@" \
    -pthread
