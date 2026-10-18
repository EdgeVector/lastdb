#!/usr/bin/env python3
"""Run two matched rounds, with the second configuration order reversed."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from api import BASE, measure

HERE = Path(__file__).resolve().parent
CASES = [('before', 1), ('current-off', 1), ('current-on', 1),
         ('current-on', 2), ('current-off', 2), ('before', 2)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ('before', 'current'):
        parser.add_argument('--' + label + '-binary', type=Path, required=True)
        parser.add_argument('--' + label + '-sha256', required=True)
        parser.add_argument('--' + label + '-source', type=Path, required=True)
    args = parser.parse_args()
    programs = {}
    for label in ('before', 'current'):
        binary = getattr(args, label + '_binary').resolve(strict=True)
        source = getattr(args, label + '_source').resolve(strict=True)
        expected = getattr(args, label + '_sha256')
        if measure.profile.file_hash(binary) != expected:
            raise RuntimeError('The executable identity differs: ' + label)
        programs[label] = {'binary': str(binary), 'sha256': expected,
            'source_identity': str(source), 'source_identity_sha256': measure.profile.file_hash(source)}
    with (HERE / 'run-inputs.json').open('x') as output:
        json.dump({'programs': programs, 'cases': CASES,
            'runner_sha256': measure.profile.file_hash(Path(__file__)),
            'inherited_nonsecret_flags': {name: os.environ.get(name) for name in (
                'LASTDB_RESIDENT_MODE', 'LASTDB_DISK_PACK', 'LASTDB_MUTATION_SYNC_FLUSH',
                'LASTDB_BACKGROUND_FLUSH_MS', 'LASTDB_RESIDENT_KEY_CAP',
                'LASTDB_RESIDENT_MAX_DEFERRED_BYTES', 'LASTDB_RESIDENT_PERSIST_MS',
                'LASTDB_SCHEMA_KEY_BYTES', 'LASTDB_SCHEMA_PRELOAD_SCHEMAS')},
            'fixed_dataset': 'Two independent schemas, each256 initial records, four fields, 1KiB bodies. One HashRange hash; every bounded request specifies it.',
            'scope': 'First reads after process reopen; OS cache uncontrolled. Repeated same-key reads and separate disjoint-key reads. Writes label caller read conditions, not guaranteed internal cache state.'},
            output, sort_keys=True, indent=2)
        output.write('\n')
    outcomes = []
    for number, (label, round_number) in enumerate(CASES, 1):
        program = programs['before' if label == 'before' else 'current']
        name = f'{label}-r{round_number}'
        command = [sys.executable, '-u', str(HERE / 'driver.py'),
            '--expected-sha256', program['sha256'], '--source-identity', program['source_identity'],
            '--label', label, '--round', str(round_number), '--binary', program['binary'],
            '--variant', name, '--receipt-dir', str(HERE / name),
            '--records-per-hash', '64', '--hashes', '4', '--body-bytes', '1024',
            '--cap', '10000', '--seed-batch', '64', '--read-rounds', '5',
            '--counts', '16,64', '--concurrent-width', '16',
            '--source-commit', 'aa64f61cd3d2d01a3f8953d4a1b3cf21f9940bab' if label == 'before'
                else 'current-frozen-' + program['sha256']]
        started = time.perf_counter_ns()
        result = subprocess.run(command, cwd=BASE, capture_output=True, text=True, check=False)
        (HERE / (name + '.stdout')).write_text(result.stdout)
        (HERE / (name + '.stderr')).write_text(result.stderr)
        outcome = {'case': number, 'label': label, 'round': round_number,
            'command': command, 'exit_code': result.returncode,
            'supervisor_elapsed_seconds': (time.perf_counter_ns() - started) / 1e9}
        saved_path = HERE / name / 'result.json'
        if saved_path.is_file():
            outcome.update(json.loads(saved_path.read_text()))
        outcomes.append(outcome)
        (HERE / 'run-order.json').write_text(json.dumps(outcomes, sort_keys=True, indent=2) + '\n')
        print(json.dumps({key: value for key, value in outcome.items()
            if key not in ('command', 'http_refusals', 'query_item_failures')}), flush=True)
        if result.returncode != 0:
            return result.returncode
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
