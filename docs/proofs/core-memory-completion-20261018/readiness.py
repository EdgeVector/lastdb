#!/usr/bin/env python3
"""Read the dirty tree for merge size, artifact, and retired-check readiness."""
import argparse
import collections
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent


def git(*arguments, required=True):
    outcome = subprocess.run(['git', *arguments], cwd=ROOT, capture_output=True, text=True)
    if required and outcome.returncode:
        raise RuntimeError(outcome.stderr.strip())
    return outcome.stdout if outcome.returncode == 0 else None


def line_count(value):
    return len(value.splitlines())


def source_sizes(base, paths):
    rows, file_failures, function_failures = [], [], []
    try:
        spec = importlib.util.spec_from_file_location('size_lint',
            ROOT / 'scripts/lints/lint-diff-fn-size.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        parser = module.Parser(module.Language(module.ts_rust.language()))
    except ImportError as error:
        parser, module = None, None
        function_failures.append({'unavailable': str(error)})
    for path in paths:
        absolute = ROOT / path
        if not path.endswith('.rs') or not absolute.is_file():
            continue
        current = absolute.read_text()
        prior = git('show', base + ':' + path, required=False)
        before, after = 0 if prior is None else line_count(prior), line_count(current)
        failure = after > 400 and (prior is None or before <= 400 or after - before > 10)
        override = 'lint:file-size-ok' in current
        row = {'path': path, 'before': before, 'after': after,
               'file_failure': failure and not override, 'file_override': override}
        rows.append(row)
        if row['file_failure']:
            file_failures.append(row)
        if parser is not None:
            function_failures.extend(check_functions(base, path, current, prior, parser, module))
    return {'files': rows, 'file_failures': file_failures,
            'function_failures': function_failures}


def check_functions(base, path, current, prior, parser, module):
    baseline = {} if prior is None else {name: end - start + 1
        for name, start, end in module.functions(prior.encode(), parser)}
    touched = set(range(1, line_count(current) + 1)) if prior is None else set()
    if prior is not None:
        for line in git('diff', '-U0', base, '--', path).splitlines():
            match = re.match(r'^@@ -\S+ \+(\d+)(?:,(\d+))? @@', line)
            if match:
                start = int(match.group(1))
                count = 1 if match.group(2) is None else int(match.group(2))
                touched.update(range(start, start + count))
    failures, lines = [], current.splitlines()
    for name, start, end in module.functions(current.encode(), parser):
        after, before = end - start + 1, baseline.get(name)
        if after <= 100 or not touched.intersection(range(start, end + 1)):
            continue
        if any('lint:fn-size-ok' in line for line in lines[max(start - 2, 0):end]):
            continue
        if before is None or before <= 100 or after - before > 10:
            failures.append({'path': path, 'function': name, 'start': start,
                             'before': before, 'after': after})
    return failures


def artifact_sizes(paths):
    files = [{'path': path, 'bytes': (ROOT / path).stat().st_size}
             for path in paths if (ROOT / path).is_file()]
    oversized = [row for row in files if row['bytes'] > 2 * 1024 * 1024]
    totals = collections.defaultdict(lambda: {'files': 0, 'bytes': 0})
    for row in files:
        parts = Path(row['path']).parts
        bucket = '/'.join(parts[:3]) if parts[:2] == ('docs', 'proofs') else parts[0]
        totals[bucket]['files'] += 1
        totals[bucket]['bytes'] += row['bytes']
    forbidden = [row for row in files if
        any(part in ('target', 'target-dogfood', 'node_modules', '__pycache__')
            for part in Path(row['path']).parts)
        or Path(row['path']).suffix in ('.rlib', '.rmeta', '.pyc', '.o')]
    return {'total_files': len(files), 'total_bytes': sum(row['bytes'] for row in files),
            'by_directory': dict(sorted(totals.items())), 'oversized': oversized,
            'forbidden': forbidden, 'largest': sorted(files, key=lambda row: -row['bytes'])[:30]}


def retired_checks(paths):
    markers = []
    pattern = re.compile(r'#\[(?:test|tokio::test|cfg\(test\))\]')
    for path in paths:
        source = ROOT / path
        if path.endswith('.rs') and source.is_file():
            markers.extend({'path': path, 'line': number, 'text': line.strip()}
                for number, line in enumerate(source.read_text().splitlines(), 1)
                if pattern.search(line))
    return markers


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', default='origin/main')
    parser.add_argument('--output', type=Path, default=HERE / 'readiness-initial.json')
    args = parser.parse_args()
    untracked = git('ls-files', '--others', '--exclude-standard').splitlines()
    changed = git('diff', '--name-only', args.base).splitlines()
    paths = sorted(set(changed + untracked))
    result = {'scope': 'Read-only dirty tree audit; source can change after this record.',
        'base': args.base, 'base_commit': git('rev-parse', args.base).strip(),
        'head_commit': git('rev-parse', 'HEAD').strip(),
        'branch': git('branch', '--show-current').strip(),
        'required_check': 'ci-required',
        'required_jobs': ['build_test: fmt, selected clippy and product lints',
            'lastdb_app_sdk_check: npm typecheck, lint and build',
            'full_workspace_check: workspace and off-lane clippy',
            'diff_file_size: file, function and added-allow checks'],
        'untracked_artifacts': artifact_sizes(untracked),
        'dirty_source_sizes': source_sizes(args.base, paths),
        'retired_check_markers_in_changed_source': retired_checks(paths)}
    args.output.write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    print(json.dumps({'output': str(args.output),
        'oversized_artifacts': len(result['untracked_artifacts']['oversized']),
        'forbidden_artifacts': len(result['untracked_artifacts']['forbidden']),
        'file_size_failures': len(result['dirty_source_sizes']['file_failures']),
        'function_size_failures': len(result['dirty_source_sizes']['function_failures']),
        'retired_check_markers': len(result['retired_check_markers_in_changed_source'])}))


if __name__ == '__main__':
    main()
