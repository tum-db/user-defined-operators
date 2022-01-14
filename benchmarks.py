#!/usr/bin/env python3

import io
import itertools
import math
import multiprocessing
import os
import os.path
import re
import subprocess
import sys
import tempfile
import time

import psycopg2


SEED = 42
KMEANS_SIZES = (
    list(range(10_000, 100_000, 10_000)) +
    list(range(100_000, 1_000_000, 100_000)) +
    list(range(1_000_000, 10_000_000, 1_000_000)) +
    [10_000_000]
)
KMEANS_SIZES_LARGE = (
    list(range(12_000_000, 30_000_000, 2_000_000)) +
    list(range(30_000_000, 100_000_000, 10_000_000)) +
    list(range(100_000_000, 500_000_000, 100_000_000)) +
    [500_000_000]
)
LINEAR_REGRESSION_SIZES = (
    list(range(100_000_000, 1_000_000_000, 100_000_000)) +
    [1_000_000_000]
)
WORDS_SIZES = (
    list(range(100_000, 1_000_000, 100_000)) +
    list(range(1_000_000, 10_000_000, 1_000_000)) +
    list(range(10_000_000, 100_000_000, 10_000_000)) +
    [100_000_000]
)
ARRAYS_SIZES = [s // 10 for s in WORDS_SIZES]

CREATE_POINTS_SQL = '''\
create table points_{size} (
    x double precision not null,
    y double precision not null,
    cluster_id integer not null
);
insert into points_{size}
select x, y, "clusterId" from create_points({size});
'''

CREATE_XY_SQL = '''
create table xy_{size} (
    x double precision not null,
    y double precision not null
);
insert into xy_{size}
select x, y from create_regression_points(3.0, -2.0, 1.0, {size});
'''

CREATE_WORDS_SQL = '''\
create table words_{size} (
    word text not null
);
insert into words_{size}
select word from create_words({size});
'''

CREATE_ARRAYS_SQL = '''\
create table array_values_{size} (
    name text not null,
    values text not null
);
insert into array_values_{size}
select name, values from create_arrays({size});
'''

UDO_KMEANS_SQL = '''\
with data as (
    select x, y, cast(cluster_id as bigint) as payload
    from {input_relation}
)
select "clusterId", count(*)
from udo_kmeans(table (select * from data))
group by "clusterId";
'''
KMEANS_UMBRA_SQL = '''\
with data as (
    select x, y, cast(cluster_id as bigint) as payload
    from {input_relation}
)
select cluster_id, count(*)
from umbra.kmeans(table (select * from data), 8 order by x, y)
group by cluster_id;
'''

UDO_REGRESSION_SQL = '''\
select * from udo_regression(table (select x, y from {input_relation}));
'''
REGRESSION_SQL = '''\
select regr_intercept(y, x), regr_slope(y, x) from {input_relation};
'''
REGRESSION_UMBRA_SQL = '''\
select * from umbra.linear_regression(table (select x, y from {input_relation}), 2);
'''

UDO_WORDS_SQL = '''\
select count(*)
from contains_database(table (select word from {input_relation}));
'''
WORDS_SQL = '''\
select count(*)
from {input_relation}
where word ilike '%database%';
'''

UDO_ARRAYS_SQL =  '''\
select name, count(*)
from split_arrays(table (select name, values from {input_relation}))
where value between 1000 and 2000
group by name
order by name;
'''
ARRAYS_RECURSIVE_SQL = '''\
with recursive split_arrays(name, value, tail) as (
    select c.name, NULL, c.values as tail from {input_relation} c
    union all
    select
        s.name,
        case
            when comma = 0 then s.tail
            else left(s.tail, comma - 1)
        end as value,
        case
            when comma = 0 then ''
            else right(s.tail, -comma)
        end as tail
    from (
        select s.*, position(',' in s.tail) as comma
        from split_arrays s
    ) s
    where s.tail != ''
),
split_values as (
    select name,
        case when value similar to '[0-9]+'
        then cast(value as bigint)
        else null end as value
    from split_arrays
)
select name, count(*)
from split_values
where value between 1000 and 2000
group by name
order by name;
'''
ARRAYS_POSTGRES_SQL = '''\
with unnest_values(name, value) as (
    select name, string_to_table(values, ',') as value
    from {input_relation}
),
split_values as (
    select name, cast(value as bigint) as value
    from unnest_values
    where
        value != '' and
        value similar to '[0-9]+'
)
select name, count(*)
from split_values
where value between 1000 and 2000
group by name
order by name;
'''
ARRAYS_DUCKDB_SQL = '''\
with unnest_values(name, value) as (
    select name, unnest(string_split(values, ',')) as value
    from {input_relation}
),
split_values as (
    select name, cast(value as bigint) as value
    from unnest_values
    where
        value != '' and
        value similar to '[0-9]+'
)
select name, count(*)
from split_values
where value between 1000 and 2000
group by name
order by name;
'''


# List of (funcname, args, classname)
UDO_FUNCTIONS = [
    ('count_lifestyle', 'table', 'CountLifestyle'),
    ('identity', 'table', 'Identity'),
    ('create_points', 'bigint', 'CreatePoints'),
    ('create_regression_points', 'double precision, double precision, double precision, bigint', 'CreateRegressionPoints'),
    ('create_words', 'bigint', 'CreateWords'),
    ('create_arrays', 'bigint', 'CreateArrays'),
    ('contains_database', 'table', 'ContainsDatabase'),
    ('split_arrays', 'table', 'SplitArrays'),
    ('udo_kmeans', 'table', 'KMeans'),
    ('udo_regression', 'table', 'LinearRegression'),
]


def generate_query(funcname, args, classname):
    with open(f'{funcname}.cpp') as f:
        code = f.read()

    return (
f'''create function {funcname}({args}) returns table language 'UDO-C++' as $$
{code}
$$, '{classname}';
''')


class CoresInfo:
    def __init__(self):
        # The current NUMA node id
        self._numa_node = None
        # The id of the current physical core on the NUMA node
        self._core = None
        # The id of the current unique logical core id which belongs to the current physical core
        self._thread = None
        # Maps NUMA node ids to physical core ids and physical core ids to thread ids
        self.threads = {}

        self._collect()

    def _update_threads(self):
        if self._numa_node is not None and self._core is not None and self._thread is not None:
            numa_node = self.threads.setdefault(self._numa_node, {})
            numa_node.setdefault(self._core, []).append(self._thread)

        self._numa_node = None
        self._core = None
        self._thread = None

    def _collect(self, cpuinfo_filename = '/proc/cpuinfo'):
        with open(cpuinfo_filename, 'r') as cpuinfo:
            for line in cpuinfo:
                line = line.strip()
                if not line:
                    # Found an empty line which separates threads, update map
                    self._update_threads()
                    continue

                name, _, value = line.partition(':')
                name = name.strip()
                value = value.strip()

                if name == 'physical id':
                    self._numa_node = int(value)
                elif name == 'core id':
                    self._core = int(value)
                elif name == 'processor':
                    self._thread = int(value)

    def get_num_threads(self):
        num_threads = 0

        for cores in self.threads.values():
            for threads in cores.values():
                num_threads += len(threads)

        return num_threads

    def pick_threads(self, num_threads):
        first_node_cores = next(iter(self.threads.values()))
        num_cores_per_node = len(first_node_cores)

        if num_threads <= num_cores_per_node:
            # We can fit all threads into the same NUMA node without SMT
            picked_threads = []
            for threads in itertools.islice(first_node_cores.values(), num_threads):
                picked_threads.append(threads[0])

            picked_threads.sort()
            return picked_threads

        num_nodes = len(self.threads)
        num_picked_threads_per_node = num_threads // num_nodes
        # Distribute the remainder onto the first nodes
        node_thread_counts = list(itertools.chain(
            itertools.repeat(num_picked_threads_per_node + 1, num_threads % num_nodes),
            itertools.repeat(num_picked_threads_per_node, num_nodes - (num_threads % num_nodes))
        ))

        if num_threads <= num_cores_per_node * num_nodes:
            # We need more than one NUMA node but still don't need SMT

            picked_threads = []
            for node_cores, node_num_threads in zip(self.threads.values(), node_thread_counts):
                for threads in itertools.islice(node_cores.values(), node_num_threads):
                    picked_threads.append(threads[0])

            picked_threads.sort()
            return picked_threads

        # We need to use SMT
        picked_threads = []
        for node_cores, node_num_threads in zip(self.threads.values(), node_thread_counts):
            node_cores_threads = list(node_cores.values())
            for i in range(node_num_threads):
                picked_threads.append(node_cores_threads[i % num_cores_per_node][i // num_cores_per_node])

        picked_threads.sort()
        return picked_threads


def check_umbra(proc, wait=False):
    if wait:
        if proc.wait() != 0:
            raise RuntimeError(f'Umbra process exited with error code {proc.returncode}')
    else:
        if proc.poll() is not None:
            raise RuntimeError(f'Umbra process exited unexpectedly with return code {proc.returncode}')


def _start_sql_proc(args, **kwargs):
    tmpfile = tempfile.TemporaryFile('w', encoding='utf8')
    # We pass this file via fd to the umbra process, so create a new fd that
    # the umbra process will use and make it inheritable.
    umbra_tmpfile_fd = os.dup(tmpfile.fileno())
    os.set_inheritable(umbra_tmpfile_fd, True)

    env = os.environ.copy()
    env.update({
        'CODEGENRANDOMSEED': str(SEED),
        'KMEANSFIXEDITERATIONS': '10',
    })

    popen_kwargs = {
        'stdin': subprocess.PIPE,
        'stdout': subprocess.PIPE,
        'env': env,
        'encoding': 'utf8',
        'close_fds': True,
        'pass_fds': [umbra_tmpfile_fd],
    }
    popen_kwargs.update(kwargs)

    sql_proc = subprocess.Popen(args, **popen_kwargs)

    os.close(umbra_tmpfile_fd)

    check_umbra(sql_proc)

    return sql_proc, tmpfile, umbra_tmpfile_fd


def create_umbra_db(umbra_sql, dbfile):
    sql_proc, tmpfile, umbra_tmpfile_fd = _start_sql_proc([umbra_sql, '-createdb', dbfile])

    sql_proc.stdin.write('\\o -\n')
    sql_proc.stdin.flush()

    def wait_umbra():
        sql_proc.stdin.write('select 1;\n')
        sql_proc.stdin.flush();
        sql_proc.stdout.readline();
        check_umbra(sql_proc)

    for funcname, args, classname in UDO_FUNCTIONS:
        print(f'Create function {funcname}')
        query = generate_query(funcname, args, classname)
        sql_proc.stdin.write(query)
        sql_proc.stdin.flush()
        wait_umbra()

    for size in KMEANS_SIZES + KMEANS_SIZES_LARGE:
        print(f'Create points_{size}')
        sql_proc.stdin.write(CREATE_POINTS_SQL.format(size=size))
        sql_proc.stdin.flush()
        wait_umbra()

    for size in LINEAR_REGRESSION_SIZES:
        print(f'Create xy_{size}')
        sql_proc.stdin.write(CREATE_XY_SQL.format(size=size))
        sql_proc.stdin.flush()
        wait_umbra()

    for size in WORDS_SIZES:
        print(f'Create words_{size}')
        sql_proc.stdin.write(CREATE_WORDS_SQL.format(size=size))
        sql_proc.stdin.flush()
        wait_umbra()

    for size in ARRAYS_SIZES:
        print(f'Create array_values_{size}')
        sql_proc.stdin.write(CREATE_ARRAYS_SQL.format(size=size))
        sql_proc.stdin.flush()
        wait_umbra()

    sql_proc.stdin.close()
    check_umbra(sql_proc, True)


def create_postgres_db(conn):
    cursor = conn.cursor()

    for funcname, args, classname in UDO_FUNCTIONS:
        print(f'Create function {funcname}')
        query = generate_query(funcname, args, classname)
        cursor.execute(query)
        conn.commit()

    for size in KMEANS_SIZES:
        print(f'Create points_{size}')
        cursor.execute(CREATE_POINTS_SQL.format(size=size))
        conn.commit()

    for size in LINEAR_REGRESSION_SIZES:
        print(f'Create xy_{size}')
        cursor.execute(CREATE_XY_SQL.format(size=size))
        conn.commit()

    for size in WORDS_SIZES:
        print(f'Create words_{size}')
        cursor.execute(CREATE_WORDS_SQL.format(size=size))
        conn.commit()

    for size in ARRAYS_SIZES:
        print(f'Create array_values_{size}')
        cursor.execute(CREATE_ARRAYS_SQL.format(size=size))
        conn.commit()


def run_umbra_benchmark(umbra_sql, dbfile, name, sizes, get_query, umbra_settings, **popen_kwargs):
    sql_proc, tmpfile, umbra_tmpfile_fd = _start_sql_proc([umbra_sql, dbfile], **popen_kwargs)

    sql_proc.stdin.write('\\o -\n')
    sql_proc.stdin.flush()

    for setting_name, value in umbra_settings.items():
        sql_proc.stdin.write(f'''set debug.{setting_name} = '{value}';\n''');
        sql_proc.stdin.flush()
        check_umbra(sql_proc)

    for size in sizes:
        # Run query once without measurement to warm up system
        query = get_query(size)
        sql_proc.stdin.write(f'\\record off\n')
        sql_proc.stdin.write(query)
        sql_proc.stdin.flush()
        sql_proc.stdout.readline()
        check_umbra(sql_proc)

        sql_proc.stdin.write(f'\\record benchmarks.log {name}_{size}\n')
        sql_proc.stdin.flush()

        for i in range(10):
            print(f'Run umbra_{name}_{size} iteration {i+1}')
            sql_proc.stdin.write(query)
            sql_proc.stdin.flush()
            sql_proc.stdout.readline()
            check_umbra(sql_proc)

    sql_proc.stdin.close()
    check_umbra(sql_proc, True)


def run_postgres_benchmark(conn, name, sizes, get_query):
    cursor = conn.cursor()

    if not os.path.exists('postgres-benchmarks.log'):
        with open('postgres-benchmarks.log', 'w') as log:
            log.write('query,num_tuples,planning_ms,execution_ms\n')

    for size in sizes:
        # Run query once without measurement to warm up system
        query = 'explain analyze ' + get_query(size)
        cursor.execute(query)
        cursor.fetchall()

        times = []

        for i in range(10):
            print(f'Run postgres_{name}_{size} iteration {i+1}')
            cursor.execute(query)
            planning = None
            execution = None
            for (row,) in cursor.fetchall():
                if row.startswith('Planning Time: '):
                    row = row[len('Planning Time: '):]
                    assert row[-3:] == " ms"
                    planning = row[:-3]
                elif row.startswith('Execution Time: '):
                    row = row[len('Execution Time: '):]
                    assert row[-3:] == " ms"
                    execution = row[:-3]

            assert planning is not None
            assert execution is not None

            times.append((planning, execution))

        with open('postgres-benchmarks.log', 'a') as log:
            for planning, execution in times:
                log.write(f'{name},{size},{planning},{execution}\n')


def run_standalone_benchmark(umbra_sql, dbfile, standalone_exe, name, sizes, get_relation):
    sql_proc, tmpfile, umbra_tmpfile_fd = _start_sql_proc([umbra_sql, dbfile])

    sql_proc.stdin.write('\\o -\n')
    sql_proc.stdin.flush()

    if not os.path.exists('standalone-benchmarks.log'):
        with open('standalone-benchmarks.log', 'w') as log:
            log.write('name,num_tuples,time_ns\n')

    for size in sizes:
        relation = get_relation(size)
        print(f'Run standalone_{name}_{size}')
        with tempfile.NamedTemporaryFile() as data_file:
            sql_proc.stdin.write(f'''\
copy {relation} to '{data_file.name}' csv header;
''')
            sql_proc.stdin.write('select 1;\n')
            sql_proc.stdin.flush()
            sql_proc.stdout.readline()
            check_umbra(sql_proc)

            process_kwargs = {
                'stdout': subprocess.PIPE,
                'encoding': 'utf8',
                'close_fds': True,
                'check': True,
            }
            proc = subprocess.run([standalone_exe, '--benchmark', data_file.name], **process_kwargs)

            with open('standalone-benchmarks.log', 'a') as log:
                for line in proc.stdout.splitlines():
                    log.write(f'{name},{size},{line}\n')

    sql_proc.stdin.close()
    check_umbra(sql_proc, True)


def _run_spark(spark_submit, spark_class, *args):
    process_kwargs = {
        'capture_output': True,
        'encoding': 'utf8',
        'close_fds': True,
        'cwd': './spark',
        'check': True,
    }
    proc = subprocess.run(['./run-spark.sh', spark_submit, spark_class, *args], **process_kwargs)
    return proc.stdout


SPARK_TIME_RE = re.compile('Time taken: ([0-9]+) ms')

def run_spark_benchmark(umbra_sql, dbfile, spark_submit, name, spark_class, get_relation, sizes):
    sql_proc, tmpfile, umbra_tmpfile_fd = _start_sql_proc([umbra_sql, dbfile])

    sql_proc.stdin.write('\\o -\n')
    sql_proc.stdin.flush()

    if not os.path.exists('spark-benchmarks.log'):
        with open('spark-benchmarks.log', 'w') as log:
            log.write('name,num_tuples,time_in_ms\n')

    with open('spark-benchmarks.log', 'a') as log:
        for size in sizes:
            relation = get_relation(size)
            print(f'Run spark_{name}_{size}')
            with tempfile.NamedTemporaryFile() as data_file:
                sql_proc.stdin.write(f'''\
copy {relation} to '{data_file.name}' csv header;
''')
                sql_proc.stdin.write('select 1;\n')
                sql_proc.stdin.flush()
                sql_proc.stdout.readline()
                check_umbra(sql_proc)

                output = _run_spark(spark_submit, spark_class, data_file.name)

                for line in output.splitlines():
                    match = SPARK_TIME_RE.search(line)
                    time_ms = match.group(1)
                    log.write(f'{name},{size},{time_ms}\n')

    sql_proc.stdin.close()
    check_umbra(sql_proc, True)


def _duckdb_benchmark(data_file, name, size, relation, query):
    import duckdb
    import pandas as pd

    num_threads = os.sched_getaffinity(0)
    con = duckdb.connect(config={'threads': len(num_threads)})

    if 'words' in relation:
        words = pd.read_csv(data_file, sep=',', dtype='string')
        con.register(relation, words)
    elif 'array_values' in relation:
        array_values_df = pd.read_csv(data_file, sep=',', dtype='string')
        con.register(relation, array_values_df)
    elif 'xy' in relation:
        xy_df = pd.read_csv(data_file, sep=',', dtype='float64')
        con.register(relation, xy_df)
    else:
        raise ValueError(f'unknown relation type of {relation}')

    # Run query once without measurement to warm up system
    con.execute(query)
    con.fetchall()

    times = []
    for _ in range(10):
        t_begin = time.perf_counter()
        con.execute(query)
        con.fetchall()
        t_end = time.perf_counter()

        times.append(t_end - t_begin)

    if not os.path.exists('duckdb-benchmarks.log'):
        with open('duckdb-benchmarks.log', 'w') as log:
            log.write('query,num_tuples,time_s\n')

    with open('duckdb-benchmarks.log', 'a') as log:
        for t in times:
            log.write(f'{name},{size},{t}\n')


def can_import_duckb():
    import importlib

    try:
        importlib.import_module('duckdb')
        importlib.import_module('pandas')
    except ModuleNotFoundError:
        return False

    return True


def run_duckdb_benchmark(umbra_sql, dbfile, name, get_relation, get_query, sizes):
    sql_proc, tmpfile, umbra_tmpfile_fd = _start_sql_proc([umbra_sql, dbfile])

    sql_proc.stdin.write('\\o -\n')
    sql_proc.stdin.flush()

    for size in sizes:
        relation = get_relation(size)
        print(f'Run duckdb_{name}_{size}')
        with tempfile.NamedTemporaryFile() as data_file:
            sql_proc.stdin.write(f'''\
copy {relation} to '{data_file.name}' csv header;
''')
            sql_proc.stdin.write('select 1;\n')
            sql_proc.stdin.flush()
            sql_proc.stdout.readline()
            check_umbra(sql_proc)

            query = get_query(size)
            proc = multiprocessing.Process(target=_duckdb_benchmark, args=(data_file.name, name, size, relation, query))
            proc.start()
            proc.join()
            if proc.exitcode != 0:
                raise RuntimeError(f'duckdb process returned error code {proc.exitcode}')

    sql_proc.stdin.close()
    check_umbra(sql_proc, True)


if __name__ == '__main__':
    import argparse
    import sys

    if len(sys.argv) == 2 and sys.argv[1] == '--generate-queries':
        for (funcname, args, classname) in UDO_FUNCTIONS:
            query = generate_query(funcname, args, classname)
            with open(f'{funcname}.sql', 'w') as f:
                f.write(query)

        sys.exit(0)


    ALL_BENCHMARKS = ['kmeans', 'regression', 'words', 'arrays', 'spark']
    ALL_SYSTEMS = ['Umbra', 'Postgres', 'Spark', 'DuckDB', 'Standalone']

    parser = argparse.ArgumentParser(description='Run UDO benchmarks')
    parser.add_argument('--createdb', help='Create the benchmark database', action='store_true')
    parser.add_argument('--umbra-sql', help='Path to the Umbra sql binary')
    parser.add_argument('--umbra-dbfile', help='Path to the Umbra database file')
    parser.add_argument('--postgres-connection', help='The postgres connection string')
    parser.add_argument('--spark-home', help='The path to the Spark install directory, overrides SPARK_HOME env variable')
    parser.add_argument('--systems', help='Run the benchmarks only on the specified systems (comma separated list)')
    parser.add_argument('benchmarks', help='Which benchmarks to run', nargs='*')

    args = parser.parse_args()

    if args.benchmarks:
        benchmarks = set(args.benchmarks)
    else:
        benchmarks = set(ALL_BENCHMARKS)

    systems_lower = set(s.lower() for s in ALL_SYSTEMS)
    if args.systems is None:
        selected_systems = systems_lower
    else:
        selected_systems = set()
        for system in args.systems.split(','):
            if system.lower() not in systems_lower:
                print(f'Unknown system {system}', file=sys.stderr)
                print('Possible values: ' + ', '.join(ALL_SYSTEMS), file=sys.stderr)
                sys.exit(2)
            selected_systems.add(system)

    run_umbra = False
    run_postgres = False
    run_spark = False
    run_duckdb = False
    run_standalone = False

    if bool(args.umbra_sql) != bool(args.umbra_dbfile):
        print("--umbra-sql and --umbra-dbfile need to be specified together", file=sys.stderr)
        sys.exit(2)
    if args.umbra_sql:
        run_umbra = True

    if args.postgres_connection is not None:
        postgres_conn = psycopg2.connect(args.postgres_connection)
        run_postgres = True

    if run_umbra:
        # The duckdb and spark and standalone benchmarks get their inputs from
        # the umbra process, so we need umbra.

        if can_import_duckb():
            run_duckdb = True

        if os.path.exists('./kmeans-standalone'):
            run_standalone = True

        spark_home = args.spark_home
        if not spark_home:
            spark_home = os.environ.get('SPARK_HOME')

        if spark_home:
            run_spark = True

    if 'umbra' not in selected_systems:
        run_umbra = False
    if 'postgres' not in selected_systems:
        run_postgres = False
    if 'spark' not in selected_systems:
        run_spark = False
    if 'duckdb' not in selected_systems:
        run_duckdb = False
    if 'standalone' not in selected_systems:
        run_standalone = False

    if args.createdb:
        if run_umbra:
            create_umbra_db(args.umbra_sql, args.umbra_dbfile)
        if run_postgres:
            create_postgres_db(postgres_conn)

    if run_postgres:
        postgres_conn.set_session(readonly=True)

    if 'kmeans' in benchmarks:
        if run_umbra:
            def run_kmeans(name, query, compilationmode):
                return run_umbra_benchmark(
                    args.umbra_sql,
                    args.umbra_dbfile,
                    name,
                    KMEANS_SIZES + KMEANS_SIZES_LARGE,
                    lambda s: query.format(input_relation=f'points_{s}'),
                    {'compilationmode': compilationmode}
                )
            run_kmeans('udo_kmeans', UDO_KMEANS_SQL, 'o')
            run_kmeans('kmeans', KMEANS_UMBRA_SQL, 'o')

            cores_info = CoresInfo()

            def run_kmeans_threads(name, query):
                for num_threads in range(2, cores_info.get_num_threads() + 1, 2):
                    thread_list = cores_info.pick_threads(num_threads)
                    run_umbra_benchmark(
                        args.umbra_sql,
                        args.umbra_dbfile,
                        name,
                        [KMEANS_SIZES_LARGE[-1]],
                        lambda s: query.format(input_relation=f'points_{s}'),
                        {
                            'compilationmode': 'o',
                            'parallel': str(num_threads),
                        },
                        preexec_fn = lambda: os.sched_setaffinity(0, thread_list)
                    )
            run_kmeans_threads('udo_kmeans_threads', UDO_KMEANS_SQL)
            run_kmeans_threads('kmeans_threads', KMEANS_UMBRA_SQL)

        if run_postgres:
            def run_kmeans(name, query):
                return run_postgres_benchmark(
                    postgres_conn,
                    name,
                    KMEANS_SIZES,
                    lambda s: query.format(input_relation=f'points_{s}')
                )
            run_kmeans('udo_kmeans', UDO_KMEANS_SQL)

        if run_standalone:
            run_standalone_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                './kmeans-standalone',
                'udo_kmeans',
                KMEANS_SIZES,
                lambda s: f'points_{s}'
            )

        if run_spark:
            run_spark_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                os.path.join(spark_home, 'bin', 'spark-submit'),
                'kmeans',
                'UDOKMeans',
                lambda s: f'points_{s}',
                KMEANS_SIZES
            )

    if 'regression' in benchmarks:
        if run_umbra:
            def run_regression(name, query, compilationmode):
                return run_umbra_benchmark(
                    args.umbra_sql,
                    args.umbra_dbfile,
                    name,
                    LINEAR_REGRESSION_SIZES,
                    lambda s: query.format(input_relation=f'xy_{s}'),
                    {'compilationmode': compilationmode}
                )
            run_regression('udo_regression', UDO_REGRESSION_SQL, 'o')
            run_regression('regression', REGRESSION_SQL, 'o')
            run_regression('regression_2', REGRESSION_UMBRA_SQL, 'o')

            cores_info = CoresInfo()

            def run_regression_threads(name, query):
                for num_threads in range(2, cores_info.get_num_threads() + 1, 2):
                    thread_list = cores_info.pick_threads(num_threads)
                    run_umbra_benchmark(
                        args.umbra_sql,
                        args.umbra_dbfile,
                        name,
                        [LINEAR_REGRESSION_SIZES[-1]],
                        lambda s: query.format(input_relation=f'xy_{s}'),
                        {
                            'compilationmode': 'o',
                            'parallel': str(num_threads),
                        },
                        preexec_fn = lambda: os.sched_setaffinity(0, thread_list)
                    )
            run_regression_threads('udo_regression_threads', UDO_REGRESSION_SQL)
            run_regression_threads('regression_threads', REGRESSION_SQL)
            run_regression_threads('regression_2_threads', REGRESSION_UMBRA_SQL)

        if run_postgres:
            def run_regression(name, query):
                return run_postgres_benchmark(
                    postgres_conn,
                    name,
                    LINEAR_REGRESSION_SIZES,
                    lambda s: query.format(input_relation=f'xy_{s}')
                )
            run_regression('udo_regression', UDO_REGRESSION_SQL)
            run_regression('regression', REGRESSION_SQL)

        if run_duckdb:
            run_duckdb_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                'regression',
                lambda s: f'xy_{s}',
                lambda s: REGRESSION_SQL.format(input_relation=f'xy_{s}'),
                LINEAR_REGRESSION_SIZES
            )

        if run_standalone:
            run_standalone_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                './regression-standalone',
                'udo_regression',
                LINEAR_REGRESSION_SIZES,
                lambda s: f'xy_{s}'
            )

        if run_spark:
            run_spark_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                os.path.join(spark_home, 'bin', 'spark-submit'),
                'regression_2',
                'UDOLinearRegression',
                lambda s: f'xy_{s}',
                LINEAR_REGRESSION_SIZES
            )

    if 'words' in benchmarks:
        if run_umbra:
            def run_words(name, query, compilationmode):
                return run_umbra_benchmark(
                    args.umbra_sql,
                    args.umbra_dbfile,
                    name,
                    WORDS_SIZES,
                    lambda s: query.format(input_relation=f'words_{s}'),
                    {'compilationmode': compilationmode}
                )
            run_words('udo_words', UDO_WORDS_SQL, 'o')
            run_words('words', WORDS_SQL, 'o')

        if run_duckdb:
            run_duckdb_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                'words',
                lambda s: f'words_{s}',
                lambda s: WORDS_SQL.format(input_relation=f'words_{s}'),
                WORDS_SIZES
            )

        if run_postgres:
            def run_words(name, query):
                return run_postgres_benchmark(
                    postgres_conn,
                    name,
                    WORDS_SIZES,
                    lambda s: query.format(input_relation=f'words_{s}')
                )
            run_words('udo_words', UDO_WORDS_SQL)
            run_words('words', WORDS_SQL)

    if 'arrays' in benchmarks:
        if run_umbra:
            def run_arrays(name, query, compilationmode):
                return run_umbra_benchmark(
                    args.umbra_sql,
                    args.umbra_dbfile,
                    name,
                    ARRAYS_SIZES,
                    lambda s: query.format(input_relation=f'array_values_{s}'),
                    {'compilationmode': compilationmode}
                )
            run_arrays('udo_arrays', UDO_ARRAYS_SQL, 'o')
            run_arrays('arrays_recursive', ARRAYS_RECURSIVE_SQL, 'o')

        if run_duckdb:
            run_duckdb_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                'arrays_unnest',
                lambda s: f'array_values_{s}',
                lambda s: ARRAYS_DUCKDB_SQL.format(input_relation=f'array_values_{s}'),
                ARRAYS_SIZES
            )

        if run_postgres:
            def run_arrays(name, query):
                return run_postgres_benchmark(
                    postgres_conn,
                    name,
                    ARRAYS_SIZES,
                    lambda s: query.format(input_relation=f'array_values_{s}')
                )
            run_arrays('udo_arrays', UDO_ARRAYS_SQL)
            run_arrays('arrays_recursive', ARRAYS_RECURSIVE_SQL)
            run_arrays('arrays_unnest', ARRAYS_POSTGRES_SQL)
