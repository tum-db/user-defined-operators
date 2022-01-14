#!/bin/bash

set -eu

if (( $# < 2 )); then
    echo "Usage: $0 <llvm source dir> <install prefix> <cmake args>..."
    exit 2
fi

script_dir="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"
build_dir="$(readlink -f .)"
llvm_src="$(readlink -f "$1")"
install_prefix="$(readlink -f "$2")"
shift 2

export MAKEFLAGS="-j$(nproc)"

cmake "$llvm_src/llvm" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$install_prefix" \
    -DLLVM_LINK_LLVM_DYLIB=ON \
    -DLLVM_INSTALL_UTILS=ON \
    -DLLVM_BUILD_LLVM_DYLIB=ON \
    -DLLVM_ENABLE_FFI=ON \
    -DLLVM_TARGETS_TO_BUILD=X86 \
    -DLLVM_ENABLE_PROJECTS=clang \
    -DLLVM_INCLUDE_TESTS=OFF \
    "$@"
make
make install
