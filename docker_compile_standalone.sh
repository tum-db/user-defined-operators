#!/bin/bash

set -eu

cxxudo_deps="/opt/umbra-cxxudo-deps"

exec "/opt/umbra-llvm/bin/clang++" \
    '-xc++' '-std=c++20' -fPIC -march=native -fno-exceptions -Wall -Wextra \
    '-nostdinc++' \
    -isystem "$cxxudo_deps/include/c++/v1" \
    -isystem "$cxxudo_deps/include" \
    -L "$cxxudo_deps/lib" \
    -I "/opt/umbra/udo-runtime" \
    -DUDO_STANDALONE \
    "$@" \
    -pthread \
    '-lc++' \
    '-lc++abi'
