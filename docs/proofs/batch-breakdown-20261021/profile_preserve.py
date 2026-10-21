#!/usr/bin/env python3
"""Preserve two exact diagnostic runs, including the failed first read recipe."""
import argparse
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import tempfile

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
ATOMIC = HERE.parent / 'atomic-change-feed-20261021'
sys.path.insert(0, str(ATOMIC))
import preserve

RUNS = ('profile-r1', 'profile-r2')
ROOT_NAMES = {'profile-r1': 'sp.iuazx6at', 'profile-r2': 'sp.onrw6ch1'}
BINARY = '26454b6eb19a555031903be8f85e8ab1ce121b331a29420728f3398fe587da55'


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def context():
    preserve.HERE = HERE
    preserve.RUNS = [HERE / name / 'run' for name in RUNS]
    return preserve


def identity(path):
    return preserve.parts.identity(path)


def owned_root(label, result, directory):
    root = Path(result['root'])
    require(not root.is_symlink() and root.resolve(strict=True) == root
            and root.parent == Path(tempfile.gettempdir()).resolve()
            and root.name == ROOT_NAMES[label], 'The exact synthetic root differs')
    info = root.lstat()
    marker_path = root / '.synthetic-seed-owner.json'
    marker_info = marker_path.lstat()
    marker = preserve.read(marker_path)
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, 'The exact root ownership differs')
    require(stat.S_ISREG(marker_info.st_mode) and marker_info.st_uid == os.getuid()
            and marker_info.st_nlink == 1 and stat.S_IMODE(marker_info.st_mode) == 0o600
            and marker == preserve.read(directory / '.synthetic-seed-owner.json')
            and marker['owner_uid'] == os.getuid() and marker['kind'] == 'synthetic-trial'
            and marker['device'] == info.st_dev and marker['inode'] == info.st_ino,
            'The exact root marker differs')
    return root


def inspect(label):
    base = HERE / label
    directory = base / 'run'
    result = preserve.read(base / 'result.json')
    summary = preserve.read(directory / 'summary.json')
    command = preserve.read(base / 'command.json')
    expected_complete = label == 'profile-r2'
    require(result['complete'] is expected_complete
            and summary['complete'] is expected_complete
            and summary['root'] == result['root'], 'The recorded diagnostic outcome differs')
    require((result.get('error') is None) if expected_complete else bool(result.get('error')),
            'The diagnostic failure lacks its original error')
    if expected_complete:
        require(result.get('all_request_phase_snapshots_complete') is True
                and result.get('normal_reopen_values_and_notices_match') is True
                and result['accepted_mutations'] == 1932 and len(result['writes']) == 36,
                'The completed second diagnostic lacks its full matrix or normal reopen')
    root = owned_root(label, result, directory)
    source_path = Path(command['source']['path'])
    source = preserve.read(source_path)
    require(command['binary']['sha256'] == command['source']['binary_sha256'] == BINARY
            and identity(root / 'program-lastdbd')['sha256'] == BINARY
            and identity(source_path) == {key: command['source'][key] for key in ('sha256', 'bytes')}
            and source['state'] == 'release_verified'
            and source['final_verification']['passed'] is True,
            'The frozen binary or release source identity differs')
    driver = Path(command['command'][1]).resolve(strict=True)
    require(driver.parent == HERE and driver.name ==
            ('profile_run.py' if label == 'profile-r1' else 'profile_run_v2.py'),
            'The diagnostic driver path differs')
    expected_driver = [row for row in command['helpers'].values() if row['path'] == str(driver)]
    require(expected_driver and all(identity(driver) == {key: row[key] for key in ('sha256', 'bytes')}
                                    for row in expected_driver), 'The frozen driver differs')
    return base, directory, root, result, command, driver, source_path


def copy_result(base, directory):
    source, target = base / 'result.json', directory / 'result.json'
    if target.exists():
        require(identity(source) == identity(target), 'The exact result copy differs')
    else:
        with source.open('rb') as original, target.open('xb') as destination:
            shutil.copyfileobj(original, destination)
        require(identity(source) == identity(target), 'The new result copy differs')


def extra_archive(source, name):
    destination = preserve.slot(HERE / 'portable', name)
    require(destination.is_relative_to(HERE), 'A preservation destination escapes this proof')
    return preserve.archive_exact(source, destination)


def archive():
    proof = context()
    states = [inspect(label) for label in RUNS]
    archives, outcomes, sources = [], [], set()
    for label, state in zip(RUNS, states):
        base, directory, root, result, command, driver, source_path = state
        copy_result(base, directory)
        archives.extend(proof.archive_run(directory))
        for name in ('command.json', 'result.json'):
            archives.append(extra_archive(base / name, label + '/' + name))
        log = Path(command['environment']['LASTDB_SELF_METRICS_LOG'])
        require(log.is_relative_to(root), 'The diagnostic vitals path escapes its exact root')
        log_saved = False
        if log.exists():
            require(not log.is_symlink() and log.resolve(strict=True).is_relative_to(root)
                    and log.is_file(), 'The exact vitals log differs')
            archives.append(extra_archive(log, label + '/profile-vitals.jsonl'))
            log_saved = True
        archives.append(extra_archive(driver, 'recipes/' + driver.name))
        sources.add(source_path)
        outcomes.append({'label': label, 'complete': result['complete'], 'error': result['error'],
            'accepted_mutations': result['accepted_mutations'], 'successful_calls': len(result['writes']),
            'root': str(root), 'profile_vitals_path': str(log), 'profile_vitals_archived': log_saved,
            'role': 'completed diagnostic' if result['complete'] else 'failed diagnostic; no full-matrix or reopen acceptance'})
    for source_path in sorted(sources):
        require(source_path.parent == ATOMIC / 'after-main-v2', 'Unexpected release identity directory')
        for path in (source_path, source_path.parent / 'binary.json'):
            archives.append(extra_archive(path, 'release/' + path.name))
    notes = {'runs': outcomes, 'failed_recipe':
        'profile-r1 passes its earlier HTTP writes but stops when its read recipe requests 483 HashRange keys with limit256. '
        'The original failed outcome remains false. It does not establish the full matrix or normal reopen.',
        'second_recipe': 'profile-r2 bounds each HashRange read at256 keys and each Hash read batch at64 keys. '
        'It also records the CPU counter timebase. Its own recorded outcome controls acceptance.',
        'counter_scope': 'Native snapshots and fresh request phase rows remain in exact receipts.jsonl. '
        'No derived analysis replaces their original bytes.'}
    preserve.save(HERE / 'profile_recipe_notes.json', notes)
    archives.append(extra_archive(HERE / 'profile_recipe_notes.json', 'profile-recipe-notes.json'))
    preserve.save(HERE / 'archives.json', {'archives': archives, 'outcomes': outcomes,
        'part_max_bytes': preserve.parts.PART_BYTES, 'all_gzip_streams_exactly_verified': True,
        'scope': 'Only the two new profile roots. profile-r1 remains a failed diagnostic. '
                 'The archive includes exact raw receipts, native/phase observations, logs, drivers and release identities.'})
    print(json.dumps({'archives': len(archives), 'outcomes': [
        {key: row[key] for key in ('label', 'complete', 'accepted_mutations',
                                  'successful_calls', 'profile_vitals_archived')}
        for row in outcomes]}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('archive', 'seal', 'preview', 'execute'))
    action = parser.parse_args().action
    if action == 'archive':
        archive()
    elif action == 'seal':
        context().seal()
    else:
        # execute retains the shared exact-seal independent review gate and fresh preflight.
        return context().cleanup(action == 'execute')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
