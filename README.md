# User-Defined Operators

This repository contains the code and binaries that were used to generate the
results shown in the VLDB 2022 paper "User-Defined Operators: Efficiently
Integrating Custom Algorithms into Modern Databases".

The data that was used in the plots in the paper can be found in `results/`.

## How To Run Benchmarks

To run the benchmarks yourself, you need the following:

- git and git-lfs, to clone this repository
- docker, to build and the run the benchmark container
- Ideally, a server which has hardware that is comparable to our system:
  - Two CPU sockets, with an Intel Xeon E5-2680 CPU each (14 cores, 28 hyper
    threads per socket)
  - 128 GiB RAM per socket
  - Samsung 970 EVO NVMe M.2 SSD (1 TB)

The following steps describe how to run the benchmarks.

### Clone the git repository

First, to clone the git repository, run:

```
$ git clone 'https://github.com/tum-db/user-defined-operators.git'
```

If you have set up git-lfs correctly, this should have downloaded the Umbra
binaries in `umbra/`. You can verify this by running `git lfs fetch`.

Next, fetch the submodules by running:

```
$ git submodule update --init
```

This will clone the git repositories for LLVM, Postgres, and the udo-runtime.
Downloading the LLVM and Postgres repositories may take a while.

### Build Docker Container

You can either import the docker container from us or build it yourself. To
import it from us, run the following command:

```
$ curl 'https://db.in.tum.de/~sichert/user-defined-operators.tar.gz' | docker load
```

To build the container yourself, run the following command in the root
directory of this git repository:

```
$ docker build -t user-defined-operators .
```

This will:
- Build an Ubuntu 21.04 image
- Build LLVM
- Build glibc and libc++
- Build Postgres
- Build udo-runtime
- Install all other dependencies for the benchmarks

Especially building LLVM takes a while. On our system this takes about 40 minutes.

### Run Benchmark Container

To run the benchmark container, you should mount a volume to
`/home/umbra/benchmarks`. This directory will be used to store the benchmark
databases, so it should lay on a fast SSD.

The container can be started like this:

```
$ docker run -v /path/to/your/ssd:/home/umbra/benchmarks -ti user-defined-operator
```

To run the benchmarks, you first need to create the databases inside the
container with the `docker_createdb.sh` script. Run the following command
inside the docker container:

```
(docker) $ ./docker_createdb.sh
```

On our systems this takes about 3.5 hours and generates 505 GB of data. Make
sure that the volume mounted to `/home/umbra/benchmarks` has enough free space!

Finally, you can run the actual benchmarks with the `docker_run_benchmarks.sh` script:

```
(docker) $ ./docker_run_benchmarks.sh
```

This will write all raw benchmark results to several files with a `.log`
suffix. On our system this takes almost 20 hours. Most of the time is spent in
the Postgres benchmarks which unfortunately run only single-threaded.

After the benchmarks have run, you can create the pdf files that contain the
plots by running the `generate_plots.sh` script in the container:

```
(docker) $ ./generate_plots.sh
```

This generates a few pdf files in the docker container that contain the plots.
