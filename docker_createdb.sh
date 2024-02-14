#!/bin/bash

set -eu

cd /home/umbra

. ./venv/bin/activate

mkdir -p ./benchmarks/{umbra-db,postgres}

/opt/postgres/bin/initdb ./benchmarks/postgres/data
cp ./postgresql.conf ./benchmarks/postgres/data

/opt/postgres/bin/pg_ctl start -D ./benchmarks/postgres/data
function stop_postgres {
    /opt/postgres/bin/pg_ctl stop -D ./benchmarks/postgres/data -t 300
}
trap stop_postgres EXIT

/opt/postgres/bin/createdb udo_benchmarks

./benchmarks.py \
    --umbra-sql /opt/umbra/sql \
    --umbra-dbfile ./benchmarks/umbra-db/benchmarks.db \
    --postgres-connection 'host=/tmp dbname=udo_benchmarks' \
    --createdb \
    none

/opt/postgres/bin/psql -c vacuum udo_benchmarks
