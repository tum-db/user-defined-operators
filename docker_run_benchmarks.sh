#!/bin/bash

set -eu

cd /home/umbra

. ./venv/bin/activate

/opt/postgres/bin/pg_ctl start -D ./benchmarks/postgres/data
function stop_postgres {
    /opt/postgres/bin/pg_ctl stop -D ./benchmarks/postgres/data
}
trap stop_postgres EXIT

./benchmarks.py \
    --umbra-sql /opt/umbra/sql \
    --umbra-dbfile ./benchmarks/umbra-db/benchmarks.db \
    --postgres-connection 'host=/tmp dbname=udo_benchmarks' \
    "$@"
