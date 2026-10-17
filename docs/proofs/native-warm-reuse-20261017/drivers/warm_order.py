#!/usr/bin/env python3
"""Measure the same one-key range before and after one broad exact request."""
import argparse
import importlib.util
import os
from pathlib import Path


def request_driver():
    path = Path(__file__).with_name('range_profile.py').resolve(strict=True)
    specification = importlib.util.spec_from_file_location('range_order_requests', path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['before', 'after', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ['before-diagnostics', 'after-diagnostics']:
        parser.add_argument('--' + name, choices=['on', 'off'], default='off')
    parser.add_argument('--read-rounds', type=int, default=21)
    args = parser.parse_args()
    if args.read_rounds < 1:
        parser.error('Use at least one read round')
    args.records, args.body_bytes, args.start = 320, 1024, 32
    args.shapes, args.counts = ['range'], [1]
    return args


def repeat_range(nodes, phase, rounds, expected_hashes):
    for repeat in range(rounds):
        for node in nodes if repeat % 2 == 0 else reversed(nodes):
            response_hash = node.query([32], 'range', phase, repeat)
            previous = expected_hashes.setdefault(node.variant, response_hash)
            if response_hash != previous:
                raise RuntimeError('The unchanged range returns different complete metadata')
    for node in nodes:
        node.forensics(phase)
    print(f'{phase}: {rounds} one-key range calls per program complete', flush=True)


def known_keys(node, phase):
    for first in [0, 256]:
        node.query(list(range(first, min(first + 256, 320))), 'exact', phase, first)


def main():
    profile, args = request_driver(), arguments()
    run, nodes, errors, success = profile.Run(args), [], [], False
    try:
        nodes = [profile.Node(run, variant, getattr(args, variant), getattr(args, variant + '_diagnostics'))
                 for variant in ['before', 'after']]
        run.inputs(nodes)
        inputs = profile.json.loads((run.root / 'inputs.json').read_text())
        inputs.update(control_driver_sha256=profile.file_hash(Path(__file__)),
                      request_driver_sha256=profile.file_hash(Path(profile.__file__)),
                      control='same owned home and one-key range before and after broad exact read',
                      measured_phases=['before-broad-read', 'after-broad-read'],
                      broad_exact_keys={'first': 32, 'count': 256},
                      first_call_is_measured=True)
        profile.save(run.root / 'inputs.json', inputs)
        profile.save(run.root / 'ownership.json', {'run_id': run.identity, 'uid': os.getuid(),
                     'root': str(run.root), 'homes': [str(node.home) for node in nodes],
                     'origin': 'generated-empty'})
        for node in nodes:
            node.seed()
        for node in nodes:
            node.start()
        expected_hashes = {}
        repeat_range(nodes, 'before-broad-read', args.read_rounds, expected_hashes)
        for node in nodes:
            node.query(list(range(32, 288)), 'exact', 'broad-exact-read', 0)
            node.forensics('broad-exact-read')
        repeat_range(nodes, 'after-broad-read', args.read_rounds, expected_hashes)
        for node in nodes:
            known_keys(node, 'complete-known-keys')
            node.flush()
            node.stop()
            node.start()
            known_keys(node, 'normal-reopen')
        success = True
    except Exception as error:
        errors.append(f'{type(error).__name__}: {error}')
    finally:
        for node in nodes:
            try:
                node.stop()
            except Exception as error:
                errors.append(f'{type(error).__name__}: {error}')
        stops = [row for row in run.rows if row['operation'] == 'owned-node-stop']
        if success and (len(stops) != 6 or any(row['forced_kill'] or row['exit_code'] != 0 for row in stops)):
            errors.append('The control lacks six normal zero-exit owned-node stops')
        run.summarize()
        profile.save(run.root / 'completion.json', {
            'success': success and not errors, 'errors': errors, 'finished_utc': profile.now(),
            'load_end': os.getloadavg(), 'query_count': sum(row['operation'] == 'query' for row in run.rows),
            'receipt_sha256': profile.receipt_hashes(run, nodes)})
    if errors:
        raise RuntimeError('; '.join(errors))
    profile.save(run.root / 'completed.json', {'finished_utc': profile.now(), 'expected_values_match': True})
    print(str(run.root / 'summary.json'), flush=True)


if __name__ == '__main__':
    main()
