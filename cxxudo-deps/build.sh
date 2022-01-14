#!/bin/bash

set -eu

if (( $# != 1 )); then
    echo "Usage: $0 <install prefix>"
    exit 2
fi

script_dir="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"
build_dir="$(readlink -f .)"
install_prefix="$(readlink -f "$1")"

BUILD_GLIBC=${BUILD_GLIBC:-1}
BUILD_LIBCXXABI=${BUILD_LIBCXXABI:-1}
BUILD_LIBCXX=${BUILD_LIBCXX:-1}

export MAKEFLAGS="-j$(nproc)"

####################################
# Build glibc with the umbra patch #
####################################

if (( BUILD_GLIBC )); then
    mkdir -p "$build_dir/glibc/build"
    cd "$build_dir/glibc"

    if ! [[ -d git ]]; then
        git clone --no-checkout 'git://sourceware.org/git/glibc.git' git
        cd git
        git checkout --detach 9826b03b747b841f5fc6de2054bf1ef3f5c4bdf3
        patch -p1 < "$script_dir/umbra-glibc.patch"
    fi

    cd "$build_dir/glibc/build"

    "$build_dir/glibc/git/configure" \
        --disable-static-pie \
        --disable-tunables \
        --enable-cet \
        --prefix="$install_prefix"
    make
    make install
fi


##########################################
# Build libcxxabi using the custom glibc #
##########################################

if (( BUILD_LIBCXXABI || BUILD_LIBCXX )); then
    mkdir -p "$build_dir/libcxx"
    cd "$build_dir/libcxx"

    if ! [[ -d git ]]; then
        git clone --no-checkout 'https://github.com/llvm/llvm-project.git' git
        cd git
        git checkout --detach 1fdec59bffc11ae37eb51a1b9869f0696bfd5312
    fi
fi

if (( BUILD_LIBCXXABI )); then
    mkdir -p "$build_dir/libcxx/libcxxabi-build"
    cd "$build_dir/libcxx/libcxxabi-build"

    cmake -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$install_prefix" \
        -DCMAKE_CXX_FLAGS="-iprefix '$install_prefix/include' -L '$install_prefix/lib' -DUMBRA_GLIBC" \
        -DLLVM_PATH="$build_dir/libcxx/git/llvm" \
        -DLLVM_INCLUDE_TESTS=OFF \
        -DLIBCXXABI_ENABLE_SHARED=OFF \
        "$build_dir/libcxx/git/libcxxabi"
    ninja
    ninja install
fi

if (( BUILD_LIBCXX )); then
    mkdir -p "$build_dir/libcxx/libcxx-build"
    cd "$build_dir/libcxx/libcxx-build"

    cmake -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="$install_prefix" \
        -DCMAKE_CXX_FLAGS="-iprefix '$install_prefix/include' -L '$install_prefix/lib' -DUMBRA_GLIBC" \
        -DLLVM_PATH="$build_dir/libcxx/git/llvm" \
        -DLLVM_INCLUDE_TESTS=OFF \
        -DLIBCXX_ENABLE_SHARED=OFF \
        -DLIBCXX_CXX_ABI=libcxxabi \
        "$build_dir/libcxx/git/libcxx"
    ninja
    ninja install
fi
