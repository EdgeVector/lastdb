#!/usr/bin/env python3
"""Check the actual tracked and untracked Rust edits without a branch mutation."""
import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--root', type=Path, required=True)
    args = ap.parse_args()
    root = args.root.resolve(strict=True)

    def git(*arguments):
        return subprocess.run(['git', '-C', str(root), *arguments],
                              capture_output=True, text=True)

    def source_module(name):
        spec = importlib.util.spec_from_file_location(name,
            root / 'scripts/lints' / (name + '.py'))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    lint = source_module('lint-diff-fn-size')
    parser = lint.Parser(lint.Language(lint.ts_rust.language()))
    names = set(git('diff', 'HEAD', '--name-only', '--', '*.rs').stdout.splitlines())
    names.update(git('ls-files', '--others', '--exclude-standard', '--', '*.rs').stdout.splitlines())
    failures, files, functions = [], 0, 0
    additions, removals = 0, 0
    for name in sorted(names):
        path = root / name
        if not path.is_file():
            continue
        new = path.read_text()
        previous = git('show', 'HEAD:' + name)
        old = previous.stdout if previous.returncode == 0 else ''
        files += 1
        before, after = len(old.splitlines()), len(new.splitlines())
        if after > (400 if before <= 400 else before + 10):
            failures.append(f'{name}: file size {before} -> {after}')
        old_functions = {name: end - start + 1
                         for name, start, end in lint.functions(old.encode(), parser)}
        touched = set()
        diff = git('diff', '-U0', 'HEAD', '--', name).stdout
        if previous.returncode != 0:
            touched.update(range(1, after + 1))
        for match in re.finditer(r'^@@ -\S+ \+(\d+)(?:,(\d+))? @@', diff, re.M):
            start = int(match.group(1))
            touched.update(range(start, start + int(match.group(2) or 1)))
        for qualified, start, end in lint.functions(new.encode(), parser):
            if not touched.intersection(range(start, end + 1)):
                continue
            functions += 1
            length = end - start + 1
            previous_length = old_functions.get(qualified, 0)
            if length > (100 if previous_length <= 100 else previous_length + 10):
                failures.append(f'{name}:{start}: {qualified} has {length} lines')
        added = max(0, new.count('clippy::too_many_arguments') - old.count('clippy::too_many_arguments'))
        removed = max(0, old.count('clippy::too_many_arguments') - new.count('clippy::too_many_arguments'))
        additions += added
        removals += removed
    if additions > removals:
        failures.append(f'too_many_arguments allows: +{additions} -{removals}')
    print(json.dumps(dict(files=files, touched_functions=functions,
                          allow_additions=additions, allow_removals=removals,
                          failures=failures), indent=2))
    return bool(failures)


if __name__ == '__main__':
    raise SystemExit(main())
