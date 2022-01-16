FROM ubuntu:21.04

# Install apt dependencies
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install tzdata\
    && apt-get install -y \
        binutils \
        bison \
        ccache \
        clang \
        cmake \
        gawk \
        g++-11 \
        git \
        libboost-all-dev \
        libc6-dev \
        libjemalloc-dev \
        libre2-dev \
        libssl-dev \
        libtinfo-dev \
        lld \
        make \
        ninja-build \
    && rm -rf /var/lib/apt/lists/*

ENV CC=/usr/bin/gcc-11
ENV CXX=/usr/bin/g++-11

# Build LLVM
COPY llvm /run/llvm-src
COPY build-llvm.sh /run/llvm-src/build-llvm.sh

RUN \
    mkdir -p /run/build-llvm && \
    (cd /run/build-llvm ; /run/llvm-src/build-llvm.sh /run/llvm-src /opt/umbra-llvm) && \
    rm -rf /run/build-llvm
RUN rm -rf /run/llvm-src

# Build cxxudo deps
COPY cxxudo-deps /run/umbra-cxxudo-deps-src
RUN \
    mkdir -p /run/build-cxxudo-deps && \
    (cd /run/build-cxxudo-deps ; /run/umbra-cxxudo-deps-src/build.sh /opt/umbra-cxxudo-deps) && \
    rm -rf /run/build-cxxudo-deps
RUN rm -rf /run/umbra-cxxudo-deps-src

# Install Postgres dependencies
RUN \
    apt-get update -qq && \
    apt-get install -y \
        flex \
        libreadline-dev \
    && rm -rf /var/lib/apt/lists/*
# Build Postgres
COPY postgres /run/postgres-src
RUN \
    cd /run/postgres-src && \
    ./configure --prefix=/opt/postgres && \
    make -j$(nproc) install
RUN rm -rf /run/postgres-src

# Build udo-runtime
COPY udo-runtime /run/udo-runtime-src
RUN \
    mkdir -p /run/build-udo-runtime && \
    cd /run/build-udo-runtime && \
    cmake -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DLLVM_ROOT=/opt/umbra-llvm \
        -DPostgreSQL_ROOT=/opt/postgres \
        '-DCXXUDO_DEFAULT_CLANGXX=/opt/umbra-llvm/bin/clang++' \
        -DCXXUDO_DEPS_PREFIX=/opt/umbra-cxxudo-deps \
        /run/udo-runtime-src \
    && ninja && \
    cp libudoruntime_pg.so /opt/postgres/lib && \
    cd / && \
    rm -rf /run/build-udo-runtime
RUN rm -rf /run/udo-runtime-src

# Build umbra
COPY umbra /opt/umbra

# Install dependencies for the benchmarks
RUN \
    apt-get update -qq && \
    apt-get install -y \
        python3-psycopg2 \
        python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies for spark
RUN \
    apt-get update -qq && \
    apt-get install -y \
        gnupg2 \
        openjdk-8-jdk \
        scala \
        wget && \
    echo 'deb https://repo.scala-sbt.org/scalasbt/debian all main' > /etc/apt/sources.list.d/sbt.list && \
    wget -q 'https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823' -O - | apt-key add && \
    apt-get update -qq && \
    apt-get install -y sbt && \
    rm -rf /var/lib/apt/lists/*

# Download hadoop and spark
RUN \
    mkdir -p /run/download-spark && \
    (cd /run/download-spark ; wget -q 'https://downloads.apache.org/hadoop/common/hadoop-3.3.1/hadoop-3.3.1.tar.gz') && \
    (cd /run/download-spark ; wget -q 'https://downloads.apache.org/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz') && \
    (cd /opt ; tar -xf /run/download-spark/hadoop-3.3.1.tar.gz) && \
    (cd /opt ; tar -xf /run/download-spark/spark-3.1.2-bin-hadoop3.2.tgz) && \
    rm -rf /run/download-spark

ENV LD_LIBRARY_PATH=/opt/hadoop-3.3.1/lib/native
ENV HADOOP_HOME=/opt/hadoop-3.3.1
ENV SPARK_HOME=/opt/spark-3.1.2-bin-hadoop3.2

RUN useradd --uid 1000 --user-group --create-home --home-dir /home/umbra umbra
USER 1000

# Install python dependencies
COPY --chown=1000:1000 requirements.txt /home/umbra/
RUN \
    cd /home/umbra && \
    python3 -m venv --system-site-packages ./venv && \
    . ./venv/bin/activate && \
    pip install -r requirements.txt

# Build standalone binaries
COPY --chown=1000:1000 docker_compile_standalone.sh *.cpp /home/umbra/
RUN \
    cd /home/umbra && \
    ./docker_compile_standalone.sh -o ./kmeans-standalone ./udo_kmeans.cpp && \
    ./docker_compile_standalone.sh -o ./regression-standalone ./udo_regression.cpp

# Build spark project
COPY --chown=1000:1000 spark /home/umbra/spark
RUN cd /home/umbra/spark && sbt package

WORKDIR /home/umbra

COPY --chown=1000:1000 benchmarks.py postgresql.conf docker_createdb.sh docker_run_benchmarks.sh /home/umbra/

# Install R
USER root
RUN \
    apt-get update -qq && \
    apt-get install -y r-base \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
USER 1000
COPY --chown=1000:1000 install_packages.R /home/umbra/
RUN mkdir -p /home/umbra/R/x86_64-pc-linux-gnu-library/4.0
RUN R --no-save < /home/umbra/install_packages.R

COPY --chown=1000:1000 benchmarks.R generate_plots.sh /home/umbra/
