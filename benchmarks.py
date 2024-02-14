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
LINEAR_REGRESSION_SIZES_LARGE = (
    list(range(2_000_000_000, 40_000_000_000, 2_000_000_000)) +
    [40_000_000_000]
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


def get_kmeans_sql(mode, input_relation):
    if mode in ('cxxudo', 'wasmudo'):
        funcname = 'udo_kmeans'
        if mode == 'wasmudo':
            funcname = 'wasm_' + funcname
        return f'''\
with data as (
    select x, y, cast(cluster_id as bigint) as payload
    from {input_relation}
)
select "clusterId", count(*)
from {funcname}(table (select * from data))
group by "clusterId";
'''
    elif mode == 'umbra':
        return f'''\
with data as (
    select x, y, cast(cluster_id as bigint) as payload
    from {input_relation}
)
select cluster_id, count(*)
from umbra.kmeans(table (select * from data), 8 order by x, y)
group by cluster_id;
'''
    else:
        raise ValueError(f"invalid mode: {mode}")


def get_regression_sql(mode, input_relation, repeat = 0):
    query = ''
    if repeat > 1:
        query = 'with data(x,y) as (\n'
        select_repeat = f'select x, y from {input_relation}\n'
        query += 'union all\n'.join(select_repeat for _ in range(repeat))
        query += ')\n'
        input_relation = 'data'

    if mode in ('cxxudo', 'wasmudo'):
        funcname = 'udo_regression'
        if mode == 'wasmudo':
            funcname = 'wasm_' + funcname
        return query + f'select * from {funcname}(table (select x, y from {input_relation}));\n'
    elif mode == 'sql':
        return query + f'select regr_intercept(y, x), regr_slope(y, x) from {input_relation};\n'
    elif mode == 'umbra':
        return query + f'select * from umbra.linear_regression(table (select x, y from {input_relation}), 2);\n'
    else:
        raise ValueError(f"invalid mode: {mode}")


def get_words_sql(mode, input_relation):
    if mode in ('cxxudo', 'wasmudo'):
        funcname = 'contains_database'
        if mode == 'wasmudo':
            funcname = 'wasm_' + funcname
        return f'''\
select count(*)
from {funcname}(table (select word from {input_relation}));
'''
    elif mode == 'sql':
        return f'''\
select count(*)
from {input_relation}
where word ilike '%database%';
'''
    else:
        raise ValueError(f"invalid mode: {mode}")


def get_arrays_sql(mode, input_relation):
    if mode in ('cxxudo', 'wasmudo'):
        funcname = 'split_arrays'
        if mode == 'wasmudo':
            funcname = 'wasm_' + funcname
        return f'''\
select name, count(*)
from {funcname}(table (select name, values from {input_relation}))
where value between 1000 and 2000
group by name
order by name;
'''
    elif mode == 'recursive_sql':
        return f'''\
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
    elif mode == 'postgres':
        return f'''\
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
    elif mode == 'duckdb':
        return f'''\
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
    else:
        raise ValueError(f"invalid mode: {mode}")


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


def generate_cxxudo_create_query(funcname, args, classname):
    benchmark_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    with open(os.path.join(benchmark_dir, f'udo/{funcname}.cpp')) as f:
        code = f.read()

    return (
f'''create function {funcname}({args}) returns table language 'UDO-C++' as $$
{code}
$$, '{classname}';
''')


def generate_wasmudo_create_query(funcname, args, classname):
    benchmark_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    wasm_file = os.path.join(benchmark_dir, f'udo/wasm/{funcname}.wasm')

    return f'''create function wasm_{funcname}({args}) returns table language 'udo-wasm' security definer as '{wasm_file}', '{classname}';\n'''


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


class UmbraProcess:
    UMBRA_DEFAULT_STARTUP_SETTINGS = {
        'verbosity': 'warning',
    }

    UMBRA_DEFAULT_SETTINGS = {
        'codegenrandomseed': SEED,
        'kmeansFixedIterations': 10,
    }

    def __init__(self, cmdline, umbra_startup_settings=None, popen_kwargs=None):
        env = os.environ.copy()
        env.update({key.upper(): value for key, value in self.UMBRA_DEFAULT_STARTUP_SETTINGS.items()})
        if umbra_startup_settings:
            env.update(umbra_startup_settings)

        popen_all_kwargs = {
            'stdin': subprocess.PIPE,
            'stderr': subprocess.PIPE,
            'env': env,
            'close_fds': True,
        }
        if popen_kwargs:
            popen_all_kwargs.update(popen_kwargs)

        self.sql_proc = subprocess.Popen(cmdline, **popen_all_kwargs)

        self.check()

        for name, value in self.UMBRA_DEFAULT_SETTINGS.items():
            self.set_setting(name, value)

    def wait(self):
        if self.sql_proc.wait() != 0:
            raise RuntimeError(f'Umbra process exited with error code {self.sql_proc.returncode}')

    def check(self):
        if self.sql_proc.poll() is not None:
            raise RuntimeError(f'Umbra process exited unexpectedly with return code {self.sql_proc.returncode}')

    def close(self):
        self.sql_proc.stdin.close()
        self.wait()

    def execute_statement(self, statement):
        self.sql_proc.stdin.write(statement.encode('utf8'))
        self.sql_proc.stdin.write(b'\\warn BENCHMARKS_STATEMENT_DONE\n')
        self.sql_proc.stdin.flush()

        while True:
            line = self.sql_proc.stderr.readline()

            for error in (b'ERROR', b'FATAL', b'PANIC'):
                if line.startswith(error):
                    # Forward potentially relevant error messages to stderr
                    sys.stderr.buffer.write(line)
                    sys.stderr.flush()
                    self.check()
                    raise RuntimeError('Umbra generated error')

            if line.startswith(b'BENCHMARKS_STATEMENT_DONE'):
                break

            self.check()

    def execute_large_statement(self, statement):
        with tempfile.NamedTemporaryFile() as f:
            f.write(statement.encode('utf8'))
            f.flush()

            self.execute_statement(f'\\i {f.name}\n')

    def set_setting(self, name, value):
        self.execute_statement(f'\\set {name} {value}\n')


def create_umbra_db(umbra_sql, dbfile):
    umbra_proc = UmbraProcess([umbra_sql, '-createdb', dbfile], {'max_wal_size': '256M'})

    for funcname, args, classname in UDO_FUNCTIONS:
        print(f'Create C++ function {funcname}')
        query = generate_cxxudo_create_query(funcname, args, classname)
        umbra_proc.execute_large_statement(query)

    for funcname, args, classname in UDO_FUNCTIONS:
        print(f'Create Wasm function {funcname}')
        query = generate_wasmudo_create_query(funcname, args, classname)
        umbra_proc.execute_large_statement(query)

    for size in KMEANS_SIZES + KMEANS_SIZES_LARGE:
        print(f'Create points_{size}')
        umbra_proc.execute_large_statement(CREATE_POINTS_SQL.format(size=size))

    for size in LINEAR_REGRESSION_SIZES:
        print(f'Create xy_{size}')
        umbra_proc.execute_large_statement(CREATE_XY_SQL.format(size=size))

    for size in WORDS_SIZES:
        print(f'Create words_{size}')
        umbra_proc.execute_large_statement(CREATE_WORDS_SQL.format(size=size))

    for size in ARRAYS_SIZES:
        print(f'Create array_values_{size}')
        umbra_proc.execute_large_statement(CREATE_ARRAYS_SQL.format(size=size))

    umbra_proc.close()


def create_postgres_db(conn):
    cursor = conn.cursor()

    for funcname, args, classname in UDO_FUNCTIONS:
        print(f'Create function {funcname}')
        query = generate_cxxudo_create_query(funcname, args, classname)
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

def run_umbra_benchmark(umbra_sql, dbfile, name, sizes, get_query, umbra_settings, num_iterations = 10, **popen_kwargs):
    umbra_proc = UmbraProcess([umbra_sql, dbfile], popen_kwargs=popen_kwargs)

    umbra_proc.execute_statement('\\o -\n')

    for setting_name, value in umbra_settings.items():
        umbra_proc.set_setting(setting_name, value);

    for size in sizes:
        # Run query once without measurement to warm up system and force the
        # compilation of the function.
        query = get_query(size)
        umbra_proc.execute_statement('\\record off\n')
        umbra_proc.execute_large_statement(query)

        umbra_proc.execute_statement(f'\\record benchmarks.log {name}_{size}\n')

        for i in range(num_iterations):
            print(f'Run umbra_{name}_{size} iteration {i+1}')
            umbra_proc.execute_large_statement(query)

    umbra_proc.close()


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
    umbra_proc = UmbraProcess([umbra_sql, dbfile])
    db_dir = os.path.dirname(os.path.abspath(dbfile))

    umbra_proc.execute_statement('\\o -\n')

    if not os.path.exists('standalone-benchmarks.log'):
        with open('standalone-benchmarks.log', 'w') as log:
            log.write('name,num_tuples,csvparse_ns,execution_ns\n')

    for size in sizes:
        relation = get_relation(size)
        print(f'Run standalone_{name}_{size}')
        with tempfile.NamedTemporaryFile(dir=db_dir) as data_file:
            umbra_proc.execute_statement(f'''copy {relation} to '{data_file.name}' csv header;\n''')

            process_kwargs = {
                'stdout': subprocess.PIPE,
                'encoding': 'utf8',
                'close_fds': True,
                'check': True,
            }
            proc = subprocess.run([standalone_exe, '--benchmark', data_file.name], **process_kwargs)

            with open('standalone-benchmarks.log', 'a') as log:
                last_parse_ns = ''
                for line in proc.stdout.splitlines():
                    part, _, time_ns = line.partition(':')
                    if part == 'parse':
                        last_parse_ns = time_ns
                    elif part == 'exec':
                        log.write(f'{name},{size},{last_parse_ns},{time_ns}\n')

    umbra_proc.close()


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
    umbra_proc = UmbraProcess([umbra_sql, dbfile])
    db_dir = os.path.dirname(os.path.abspath(dbfile))

    umbra_proc.execute_statement('\\o -\n')

    if not os.path.exists('spark-benchmarks.log'):
        with open('spark-benchmarks.log', 'w') as log:
            log.write('name,num_tuples,time_in_ms\n')

    with open('spark-benchmarks.log', 'a') as log:
        for size in sizes:
            relation = get_relation(size)
            print(f'Run spark_{name}_{size}')
            with tempfile.NamedTemporaryFile(dir=db_dir) as data_file:
                umbra_proc.execute_statement(f'''copy {relation} to '{data_file.name}' csv header;\n''')

                output = _run_spark(spark_submit, spark_class, data_file.name)

                for line in output.splitlines():
                    match = SPARK_TIME_RE.search(line)
                    time_ms = match.group(1)
                    log.write(f'{name},{size},{time_ms}\n')

    umbra_proc.close()


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
    umbra_proc = UmbraProcess([umbra_sql, dbfile])

    umbra_proc.execute_statement('\\o -\n')

    for size in sizes:
        relation = get_relation(size)
        print(f'Run duckdb_{name}_{size}')
        with tempfile.NamedTemporaryFile() as data_file:
            umbra_proc.execute_statement(f'''copy {relation} to '{data_file.name}' csv header;\n''')

            query = get_query(size)
            proc = multiprocessing.Process(target=_duckdb_benchmark, args=(data_file.name, name, size, relation, query))
            proc.start()
            proc.join()
            if proc.exitcode != 0:
                raise RuntimeError(f'duckdb process returned error code {proc.exitcode}')

    umbra_proc.close()


WASM_FULL_BOUNDSCHECKS_SETTINGS = {'wasm_disableboundschecks': 0, 'wasm_loop_optimization': 0, 'wasm_offset_optimization': 0}
WASM_OPT_BOUNDSCHECKS_SETTINGS = {'wasm_disableboundschecks': 0, 'wasm_loop_optimization': 1, 'wasm_offset_optimization': 1}
WASM_NO_BOUNDSCHECKS_SETTINGS = {'wasm_disableboundschecks': 1, 'wasm_loop_optimization': 0, 'wasm_offset_optimization': 0}
WASM_SETTINGS = {
    'full_boundschecks': WASM_FULL_BOUNDSCHECKS_SETTINGS,
    'optimized_boundschecks': WASM_OPT_BOUNDSCHECKS_SETTINGS,
    'no_boundschecks': WASM_NO_BOUNDSCHECKS_SETTINGS,
}


if __name__ == '__main__':
    import argparse
    import sys

    if sys.argv[1] == '--generate-queries':
        import os
        import shlex
        import subprocess

        parser = argparse.ArgumentParser(description='Generate the SQL files for the UDOs')
        parser.add_argument('--generate-queries', action='store_true', help='Generate the SQL files for the UDOs')
        parser.add_argument('--plugin-wasmudo', help='Path to the plugin-wasmudo binary')
        parser.add_argument('--verbose', action='store_true', help='Print the commands that are used to compile the wasm functions')

        args = parser.parse_args()

        try:
            os.mkdir('udo/sql')
        except FileExistsError:
            pass

        for (funcname, func_args, classname) in UDO_FUNCTIONS:
            query = generate_cxxudo_create_query(funcname, func_args, classname)
            with open(f'udo/sql/{funcname}.sql', 'w') as f:
                f.write(query)

        if args.plugin_wasmudo:
            try:
                os.mkdir('udo/wasm')
            except FileExistsError:
                pass

            cxx_cmdline = subprocess.run([args.plugin_wasmudo, '-cxx-cmdline'], stdout=subprocess.PIPE, text=True, check=True).stdout
            cxx_cmdline = cxx_cmdline.splitlines()

            for (funcname, _, _) in UDO_FUNCTIONS:
                cpp_file = f'udo/{funcname}.cpp'
                prefix = '// plugin-wasmudo'
                wasmudo_cmds = None
                for line in open(cpp_file):
                    if line.startswith(prefix):
                        wasmudo_cmds = shlex.split(line[len(prefix):])

                if not wasmudo_cmds:
                    continue

                target_file = os.path.join('udo', 'wasm', wasmudo_cmds[-1])
                wasmudo_cmds = wasmudo_cmds[:-2]
                wasmudo_cmdline = [args.plugin_wasmudo] + wasmudo_cmds
                with open(target_file, 'w') as output_file:
                    subprocess.run(wasmudo_cmdline, stdout=output_file, check=True)

                compiler_cmdline = cxx_cmdline + ['-std=c++20', '-Wall', '-Wextra', '-Wno-unqualified-std-cast-call', '-I./udo/wasm', '-DWASMUDO', '-O3', '-DNDEBUG', '-g0', '-Wl,--strip-debug', '-o', f'udo/wasm/{funcname}.wasm', cpp_file]
                if args.verbose:
                    print(shlex.join(compiler_cmdline))
                subprocess.run(compiler_cmdline, check=True)

        sys.exit(0)


    ALL_BENCHMARKS = ['kmeans', 'regression', 'words', 'arrays', 'spark']
    ALL_SYSTEMS = ['Umbra', 'Umbra-Wasm', 'Postgres', 'Spark', 'DuckDB', 'Standalone']

    parser = argparse.ArgumentParser(description='Run UDO benchmarks')
    parser.add_argument('--generate-queries', action='store_true', help='Generate the SQL and wasm files for the UDOs')
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

    can_run_umbra = False
    can_run_postgres = False
    can_run_spark = False
    can_run_duckdb = False
    can_run_standalone = False

    if bool(args.umbra_sql) != bool(args.umbra_dbfile):
        print("--umbra-sql and --umbra-dbfile need to be specified together", file=sys.stderr)
        sys.exit(2)
    if args.umbra_sql:
        can_run_umbra = True

    if args.postgres_connection is not None:
        postgres_conn = psycopg2.connect(args.postgres_connection)
        can_run_postgres = True

    if can_run_umbra:
        # The duckdb and spark and standalone benchmarks get their inputs from
        # the umbra process, so we need umbra.

        if can_import_duckb():
            can_run_duckdb = True

        if os.path.exists('./kmeans-standalone'):
            can_run_standalone = True

        spark_home = args.spark_home
        if not spark_home:
            spark_home = os.environ.get('SPARK_HOME')

        if spark_home:
            can_run_spark = True

    run_umbra = can_run_umbra and ('umbra' in selected_systems)
    run_umbra_wasm = can_run_umbra and ('umbra-wasm' in selected_systems)
    run_postgres = can_run_postgres and ('postgres' in selected_systems)
    run_spark = can_run_spark and ('spark' in selected_systems)
    run_duckdb = can_run_duckdb and ('duckdb' in selected_systems)
    run_standalone = can_run_standalone and ('standalone' in selected_systems)

    if args.createdb:
        if run_umbra:
            create_umbra_db(args.umbra_sql, args.umbra_dbfile)
        if run_postgres:
            create_postgres_db(postgres_conn)

    if run_postgres:
        postgres_conn.set_session(readonly=True)

    cores_info = CoresInfo()

    if 'kmeans' in benchmarks:
        kmeans_sizes_all = KMEANS_SIZES + KMEANS_SIZES_LARGE
        # A wasm module only has 4 GiB of memory and it needs to
        # materialize the entire input. So, limit the size to 20M tuples.
        # Each tuple requires around 32B of memory, so for 20M tuples we
        # need ~600 MiB of memory. Because the tuples are allocated in
        # exponentially growing chunks, we can't easily use up most of the
        # 4 GiB of available memory.
        wasmudo_kmeans_max_tuples = 20_000_000
        kmeans_sizes_wasmudo = [s for s in kmeans_sizes_all if s <= wasmudo_kmeans_max_tuples]

        def run_umbra_kmeans(name, compilationmode, umbra_settings = None):
            if name.startswith('wasmudo'):
                sizes = kmeans_sizes_wasmudo
                mode = 'wasmudo'
            else:
                sizes = kmeans_sizes_all
                mode = name
            settings = {'compilationmode': 'o', 'llvmoptimizer': 3}
            if umbra_settings:
                settings.update(umbra_settings)
            return run_umbra_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                name + '_kmeans',
                sizes,
                lambda s: get_kmeans_sql(mode, f'points_{s}'),
                settings
            )

        if run_umbra:
            run_umbra_kmeans('cxxudo', 'o')
            run_umbra_kmeans('umbra', 'o')
        if run_umbra_wasm:
            for name, settings in WASM_SETTINGS.items():
                run_umbra_kmeans(f'wasmudo_{name}', 'o', settings)

        def run_umbra_kmeans_threads(name, size, umbra_settings = None):
            if name.startswith('wasmudo'):
                mode = 'wasmudo'
            else:
                mode = name
            settings = {'compilationmode': 'o', 'llvmoptimizer': 3}
            if umbra_settings:
                settings.update(umbra_settings)
            for num_threads in range(2, cores_info.get_num_threads() + 1, 2):
                settings['parallel'] = str(num_threads)
                thread_list = cores_info.pick_threads(num_threads)
                run_umbra_benchmark(
                    args.umbra_sql,
                    args.umbra_dbfile,
                    name + '_kmeans_threads',
                    [size],
                    lambda s: get_kmeans_sql(mode, f'points_{s}'),
                    settings,
                    min(10, num_threads // 2),
                    preexec_fn = lambda: os.sched_setaffinity(0, thread_list)
                )

        if run_umbra:
            run_umbra_kmeans_threads('cxxudo', KMEANS_SIZES_LARGE[-1])
            run_umbra_kmeans_threads('umbra', KMEANS_SIZES_LARGE[-1])
        if run_umbra_wasm:
            for name, settings in WASM_SETTINGS.items():
                run_umbra_kmeans_threads(f'wasmudo_{name}', wasmudo_kmeans_max_tuples, settings)

        if run_postgres:
            run_postgres_benchmark(
                postgres_conn,
                'cxxudo_kmeans',
                KMEANS_SIZES,
                lambda s: get_kmeans_sql('cxxudo', f'points_{s}')
            )

        if run_standalone:
            run_standalone_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                './kmeans-standalone',
                'cxxudo_kmeans',
                KMEANS_SIZES + KMEANS_SIZES_LARGE,
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
        def get_regression_sql_repeat(mode, size):
            relation_size = min(size, LINEAR_REGRESSION_SIZES[-1])
            repeat = size // LINEAR_REGRESSION_SIZES[-1]
            return get_regression_sql(mode, f'xy_{relation_size}', repeat=repeat)

        def run_umbra_regression(name, large, umbra_settings = None):
            if name.startswith('wasmudo'):
                mode = 'wasmudo'
            else:
                mode = name
            settings = {'compilationmode': 'o', 'llvmoptimizer': 3}
            if umbra_settings:
                settings.update(umbra_settings)

            sizes = LINEAR_REGRESSION_SIZES
            if large:
                sizes = sizes + LINEAR_REGRESSION_SIZES_LARGE
            return run_umbra_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                name + '_regression',
                sizes,
                lambda s: get_regression_sql_repeat(mode, s),
                settings
            )

        if run_umbra:
            run_umbra_regression('cxxudo', True)
            run_umbra_regression('sql', True)
            run_umbra_regression('umbra', True)
        if run_umbra_wasm:
            for name, settings in WASM_SETTINGS.items():
                run_umbra_regression(f'wasmudo_{name}', True, settings)

        def run_umbra_regression_threads(name, size, umbra_settings = None):
            if name.startswith('wasmudo'):
                mode = 'wasmudo'
                num_iterations_factor = 8
            else:
                mode = name
                num_iterations_factor = 2
            settings = {'compilationmode': 'o', 'llvmoptimizer': 3}
            if umbra_settings:
                settings.update(umbra_settings)
            for num_threads in range(2, cores_info.get_num_threads() + 1, 2):
                settings['parallel'] = str(num_threads)
                thread_list = cores_info.pick_threads(num_threads)
                run_umbra_benchmark(
                    args.umbra_sql,
                    args.umbra_dbfile,
                    name + '_regression_threads',
                    [size],
                    lambda s: get_regression_sql_repeat(mode, s),
                    settings,
                    max(1, min(10, num_threads // num_iterations_factor)),
                    preexec_fn = lambda: os.sched_setaffinity(0, thread_list)
                )

        if run_umbra:
            run_umbra_regression_threads('cxxudo', LINEAR_REGRESSION_SIZES_LARGE[-1])
            run_umbra_regression_threads('sql', LINEAR_REGRESSION_SIZES_LARGE[-1])
            run_umbra_regression_threads('umbra', LINEAR_REGRESSION_SIZES_LARGE[-1])
        if run_umbra_wasm:
            for name, settings in WASM_SETTINGS.items():
                run_umbra_regression_threads(f'wasmudo_{name}', LINEAR_REGRESSION_SIZES_LARGE[-1], settings)

        if run_postgres:
            def run_regression(mode):
                return run_postgres_benchmark(
                    postgres_conn,
                    mode + '_regression',
                    LINEAR_REGRESSION_SIZES,
                    lambda s: get_regression_sql_repeat('sql', s),
                )
            run_regression('cxxudo')
            run_regression('sql')

        if run_duckdb:
            run_duckdb_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                'regression',
                lambda s: f'xy_{min(LINEAR_REGRESSION_SIZES[-1], s)}',
                lambda s: get_regression_sql_repeat('sql', s),
                LINEAR_REGRESSION_SIZES + LINEAR_REGRESSION_SIZES_LARGE
            )

        if run_standalone:
            run_standalone_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                './regression-standalone',
                'cxxudo_regression',
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
        def run_umbra_words(name, umbra_settings = None):
            if name.startswith('wasmudo'):
                mode = 'wasmudo'
            else:
                mode = name
            settings = {'compilationmode': 'o', 'llvmoptimizer': 3}
            if umbra_settings:
                settings.update(umbra_settings)
            return run_umbra_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                name + '_words',
                WORDS_SIZES,
                lambda s: get_words_sql(mode, f'words_{s}'),
                settings
            )

        if run_umbra:
            run_umbra_words('cxxudo')
            run_umbra_words('sql')
        if run_umbra_wasm:
            for name, settings in WASM_SETTINGS.items():
                run_umbra_words(f'wasmudo_{name}', settings)

        if run_duckdb:
            run_duckdb_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                'sql_words',
                lambda s: f'words_{s}',
                lambda s: get_words_sql('sql', f'words_{s}'),
                WORDS_SIZES
            )

        if run_postgres:
            def run_words(mode):
                return run_postgres_benchmark(
                    postgres_conn,
                    mode + '_words',
                    WORDS_SIZES,
                    lambda s: get_words_sql(mode, f'words_{s}'),
                )
            run_words('cxxudo')
            run_words('sql')

    if 'arrays' in benchmarks:
        def run_umbra_arrays(name, umbra_settings = None):
            if name.startswith('wasmudo'):
                mode = 'wasmudo'
            else:
                mode = name
            settings = {'compilationmode': 'o', 'llvmoptimizer': 3}
            if umbra_settings:
                settings.update(umbra_settings)
            return run_umbra_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                name + '_arrays',
                ARRAYS_SIZES,
                lambda s: get_arrays_sql(mode, f'array_values_{s}'),
                settings
            )

        if run_umbra:
            run_umbra_arrays('cxxudo')
            run_umbra_arrays('recursive_sql')
        if run_umbra_wasm:
            for name, settings in WASM_SETTINGS.items():
                run_umbra_arrays(f'wasmudo_{name}', settings)

        if run_duckdb:
            run_duckdb_benchmark(
                args.umbra_sql,
                args.umbra_dbfile,
                'arrays_unnest',
                lambda s: f'array_values_{s}',
                lambda s: get_arrays_sql('duckdb', f'array_values_{s}'),
                ARRAYS_SIZES
            )

        if run_postgres:
            def run_arrays(mode):
                return run_postgres_benchmark(
                    postgres_conn,
                    mode + '_arrays',
                    ARRAYS_SIZES,
                    lambda s: get_arrays_sql(mode, f'array_values_{s}')
                )
            run_arrays('cxxudo')
            run_arrays('recursive_sql')
            run_arrays('postgres')
