#!/usr/bin/env python3
"""Repeat the existing durable benchmark in ABBA order over a fixed small matrix."""
import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
from types import SimpleNamespace

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
DRIVER = HERE / 'benchmark.py'


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def fixed_matrix():
    first, cells = 0, []
    for batch in (1, 32, 128):
        cells.append((first, batch, 1, 1024, 8))
        first += batch * 8
    return cells, first


def host_snapshot():
    command = ['ps', '-axo', 'pid=,ppid=,stat=,pcpu=,pmem=,comm=']
    result = subprocess.run(command, capture_output=True, text=True, timeout=15,
                            check=False)
    processes, unparsed = [], 0
    for line in result.stdout.splitlines():
        fields = line.split(None, 5)
        if len(fields) != 6:
            unparsed += 1
            continue
        pid, parent, state, cpu, memory, executable = fields
        try:
            processes.append({'pid': int(pid), 'parent_pid': int(parent), 'state': state,
                'cpu_percent': float(cpu), 'memory_percent': float(memory),
                'command_name': Path(executable).name})
        except ValueError:
            unparsed += 1
    return {'observed_utc': now(), 'load_average': list(os.getloadavg()),
        'logical_cpus': os.cpu_count(), 'command': command,
        'exit_code': result.returncode, 'unparsed_lines': unparsed,
        'processes': processes, 'stderr': result.stderr,
        'scope': 'Process command names only; no arguments. Host load is uncontrolled.'}


def binary_identity(path, source):
    actual = path.resolve(strict=True)
    if not actual.is_file() or not os.access(actual, os.X_OK):
        raise ValueError('Each binary must be an executable regular file')
    return {'path': str(actual), 'sha256': sha256(actual),
            'bytes': actual.stat().st_size, 'source_commit': source}


def run_one(driver, output, label, binary, frozen):
    if sha256(Path(__file__)) != frozen['wrapper_sha256']:
        raise RuntimeError('The focused wrapper changed after its input capture')
    if sha256(DRIVER) != frozen['benchmark_sha256']:
        raise RuntimeError('The benchmark changed after its input capture')
    if sha256(Path(binary['path'])) != binary['sha256']:
        raise RuntimeError('A benchmark binary changed after its input capture')
    row = {'variant': label, 'binary': binary, 'started_utc': now(),
           'host_before': host_snapshot(), 'complete': False}
    save(output / (label + '-start.json'), row)
    arguments = SimpleNamespace(binary=Path(binary['path']), output=output / label,
        variant=label, source_commit=binary['source_commit'], reverse=False)
    print(json.dumps({'operation': 'focused-run-start', 'variant': label,
                      'source_commit': binary['source_commit']}), flush=True)
    try:
        driver.run(arguments)
        result = json.loads((arguments.output / 'result.json').read_text())
        if result.get('complete') is not True:
            raise RuntimeError('The existing benchmark did not complete')
        row.update(complete=True, result=result,
                   result_sha256=sha256(arguments.output / 'result.json'))
    except BaseException as error:
        row['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        row.update(finished_utc=now(), host_after=host_snapshot())
        save(output / (label + '-summary.json'), row)
    return row


def run(arguments):
    before = binary_identity(arguments.before_binary, arguments.before_source)
    after = binary_identity(arguments.after_binary, arguments.after_source)
    output = arguments.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    cells, records = fixed_matrix()
    command = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:]]
    frozen = {'started_utc': now(), 'wrapper_sha256': sha256(Path(__file__)),
        'benchmark_sha256': sha256(DRIVER), 'before': before, 'after': after,
        'arguments': {key: str(value) for key, value in vars(arguments).items()},
        'command': command, 'shell_command': shlex.join(command), 'cwd': os.getcwd(),
        'order': ['before-r1', 'after-r1', 'after-r2', 'before-r2'],
        'matrix_columns': ['offset', 'batch', 'writers', 'body_bytes', 'requests'],
        'matrix': cells, 'records_per_schema': records,
        'scope': 'Fixed offsets and order in every run; only benchmark.matrix is replaced. '
                 'Both geometries keep create, update, full value checks and normal reopen. '
                 'The benchmark owns timed HTTP boundaries; host observations are outside them. '
                 'No quiet-host or hardware-cold claim.'}
    save(output / 'command.json', frozen)
    spec = importlib.util.spec_from_file_location('durable_write_benchmark', DRIVER)
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    driver.matrix = fixed_matrix
    summary = {'complete': False, 'inputs': frozen, 'runs': []}
    try:
        for label, binary in [('before-r1', before), ('after-r1', after),
                              ('after-r2', after), ('before-r2', before)]:
            summary['runs'].append(run_one(driver, output, label, binary, frozen))
            save(output / 'summary.json', summary)
        summary['complete'] = True
    except BaseException as error:
        summary['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        summary['finished_utc'] = now()
        save(output / 'summary.json', summary)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before-binary', type=Path, required=True)
    parser.add_argument('--after-binary', type=Path, required=True)
    parser.add_argument('--before-source', required=True)
    parser.add_argument('--after-source', required=True)
    parser.add_argument('--output', type=Path, required=True)
    run(parser.parse_args())
