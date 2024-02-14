FROM ubuntu:23.10 AS base

# Install base dependencies
RUN apt-get update -qq \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata \
    && apt-get install -y \
        binutils \
        bison \
        cmake \
        gawk \
        g++-12 \
        git \
        libc6-dev \
        lld \
        make \
        ninja-build \
        python3 \
        python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

ENV CC=/usr/bin/gcc-12
ENV CXX=/usr/bin/g++-12

#----------------------------------------------------------------------------
FROM base AS cxxudo-deps-builder

COPY cxxudo-deps /run/umbra-cxxudo-deps-src
RUN \
    mkdir -p /run/build-cxxudo-deps && \
    (cd /run/build-cxxudo-deps ; /run/umbra-cxxudo-deps-src/build.sh /opt/umbra-cxxudo-deps) && \
    rm -rf /run/build-cxxudo-deps

#----------------------------------------------------------------------------
FROM base AS postgres-builder

# Install Postgres dependencies
RUN \
    apt-get update -qq && \
    apt-get install -y \
        flex \
        libreadline-dev \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Build Postgres
COPY postgres /run/postgres-src
RUN \
    cd /run/postgres-src && \
    ./configure --prefix=/opt/postgres && \
    make -j$(nproc) install

#----------------------------------------------------------------------------
FROM base AS udo-runtime-builder

# Install llvm dependencies
RUN \
    apt-get update -qq && \
    apt-get install -y \
        libclang-17-dev \
        llvm-17 \
    && rm -rf /var/lib/apt/lists/*

# Install clang
RUN \
    apt-get update -qq && \
    apt-get install -y \
        clang-17 \
    && rm -rf /var/lib/apt/lists/*

# Import build dependencies
COPY --from=cxxudo-deps-builder /opt/umbra-cxxudo-deps /opt/umbra-cxxudo-deps
COPY --from=postgres-builder /opt/postgres /opt/postgres

COPY udo-runtime /run/udo-runtime-src
RUN \
    mkdir -p /run/build-udo-runtime && \
    cd /run/build-udo-runtime && \
    cmake -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DPostgreSQL_ROOT=/opt/postgres \
        -DCXXUDO_DEPS_PREFIX=/opt/umbra-cxxudo-deps \
        /run/udo-runtime-src \
    && ninja && \
    cp libudoruntime_pg.so /opt/postgres/lib && \
    cd / && \
    rm -rf /run/build-udo-runtime

#----------------------------------------------------------------------------
FROM base

# Install dependencies for the benchmarks
RUN \
    apt-get update -qq && \
    apt-get install -y \
        clang-17 \
        g++-12 \
        libboost-all-dev \
        libjemalloc2 \
        libre2-10 \
        liburing2 \
        llvm-17 \
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
    (cd /run/download-spark ; wget -q 'https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz') && \
    (cd /run/download-spark ; wget -q 'https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz') && \
    (cd /opt ; tar -xf /run/download-spark/hadoop-3.3.6.tar.gz) && \
    (cd /opt ; tar -xf /run/download-spark/spark-3.1.2-bin-hadoop3.2.tgz) && \
    rm -rf /run/download-spark

ENV LD_LIBRARY_PATH=/opt/hadoop-3.3.6/lib/native
ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV SPARK_HOME=/opt/spark-3.1.2-bin-hadoop3.2

# Import builds
COPY --from=cxxudo-deps-builder /opt/umbra-cxxudo-deps /opt/umbra-cxxudo-deps
COPY --from=postgres-builder /opt/postgres /opt/postgres
COPY --from=udo-runtime-builder /opt/postgres/lib/libudoruntime_pg.so /opt/postgres/lib/libudoruntime_pg.so
COPY umbra /opt/umbra

RUN useradd --uid 1001 --user-group --create-home --home-dir /home/umbra umbra
USER 1001

# Install python dependencies
COPY --chown=1001:1001 requirements.txt /home/umbra/
RUN \
    cd /home/umbra && \
    python3 -m venv --system-site-packages ./venv && \
    . ./venv/bin/activate && \
    pip install -r requirements.txt

# Build standalone binaries
COPY --chown=1001:1001 docker_compile_standalone.sh /home/umbra/
COPY --chown=1001:1001 udo /home/umbra/udo
RUN \
    cd /home/umbra && \
    ./docker_compile_standalone.sh -O3 -DNDEBUG -o ./kmeans-standalone ./udo/udo_kmeans.cpp && \
    ./docker_compile_standalone.sh -O3 -DNDEBUG -o ./regression-standalone ./udo/udo_regression.cpp

# Build spark project
COPY --chown=1001:1001 spark /home/umbra/spark
RUN cd /home/umbra/spark && sbt package

WORKDIR /home/umbra

COPY --chown=1001:1001 benchmarks.py postgresql.conf docker_createdb.sh docker_run_benchmarks.sh /home/umbra/

# Install R
USER root
RUN \
    apt-get update -qq && \
    apt-get install -y r-base \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
USER 1001
COPY --chown=1001:1001 install_packages.R /home/umbra/
RUN mkdir -p /home/umbra/R/x86_64-pc-linux-gnu-library/4.3
RUN R --no-save < /home/umbra/install_packages.R

COPY --chown=1001:1001 benchmarks.R generate_plots.sh /home/umbra/
