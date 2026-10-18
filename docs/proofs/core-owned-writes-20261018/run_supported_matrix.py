#!/usr/bin/env python3
"""Run the seven frozen ordinary API workflows on separate fresh homes."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--generation', required=True, choices=('v6',))
    args = parser.parse_args()
    commands = []
    common = ['--binary', str(args.binary.resolve(strict=True)), '--sha256', args.sha256,
              '--source', str(args.source.resolve(strict=True))]
    for workflow in ('scoped', 'protein', 'prior-completion', 'logical', 'pressure',
                     'atom', 'protein-partial-moves'):
        receipt = HERE / f'candidate-{args.generation}-{workflow}'
        if receipt.exists():
            raise RuntimeError('An immutable receipt directory already exists: ' + str(receipt))
        tail = [*common, '--receipt-dir', str(receipt)]
        if workflow == 'prior-completion':
            script = HERE.parent / 'core-memory-completion-20261018/operations.py'
        else:
            tail += ['--driver-identity', str(HERE / 'driver-identity-v5.json'),
                     '--role', 'candidate']
            script = HERE / 'run_operations.py'
            if workflow == 'atom':
                script = HERE / 'atom_supplement.py'
                tail += ['--identity', str(HERE / 'atom-driver-identity-v3.json')]
            elif workflow == 'protein-partial-moves':
                script = HERE / 'protein_move_supplement.py'
                tail += ['--identity', str(HERE / 'protein-move-driver-identity-v2.json')]
            else:
                tail += ['--workflow', workflow]
        commands.append({'workflow': workflow, 'command': [sys.executable, '-u', str(script), *tail]})
    output = HERE / f'candidate-{args.generation}-matrix.json'
    if output.exists():
        raise RuntimeError('The matrix report already exists')
    result = {'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'scope': 'Ordinary API correctness workflows, not benchmark samples. Replay remains blocked by the native activation gate.',
              'commands': commands, 'outcomes': []}
    output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    for item in commands:
        run = subprocess.run(item['command'], cwd=ROOT, capture_output=True, text=True, check=False)
        prefix = HERE / f"candidate-{args.generation}-{item['workflow']}"
        prefix.with_suffix('.stdout').write_text(run.stdout)
        prefix.with_suffix('.stderr').write_text(run.stderr)
        outcome = {'workflow': item['workflow'], 'exit_code': run.returncode,
                   'receipt_dir': str(prefix)}
        result['outcomes'].append(outcome)
        output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
        print(json.dumps(outcome), flush=True)
        if run.returncode:
            return run.returncode
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
