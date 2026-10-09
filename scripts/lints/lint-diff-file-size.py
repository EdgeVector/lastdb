#!/usr/bin/env python3
"""Fail a PR that makes a source file too long. Only the changed files count.

Rules, per file the diff touches (added, modified or renamed Rust sources):
  - new file over MAX lines                         -> fail
  - changed file that was at or under MAX and ends over MAX -> fail
  - changed file that was over MAX and grew by more than --allow lines -> fail
  - changed file that was over MAX and grew by --allow lines or fewer  -> pass
  - changed file that shrank, or ends at or under MAX                   -> pass

Lines are counted in the whole file. Test files and generated files use
a higher limit (--max-test). Override one file with a line that contains
`lint:file-size-ok <reason>` anywhere in the file.
"""
import argparse
import subprocess
import sys

OVERRIDE = "lint:file-size-ok"


def git(*args):
    out = subprocess.run(["git", *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"lint-diff-file-size: git {' '.join(args)} failed: {out.stderr.strip()}")
    return out.stdout


def is_test_path(path):
    return "/tests/" in path or path.endswith("_test.rs") or path.endswith("/tests.rs")


def line_count(text):
    return text.count("\n") + (0 if text.endswith("\n") or not text else 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--base", required=True, help="base commit (merge base)")
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--max", type=int, default=400, help="limit for source files")
    ap.add_argument("--max-test", type=int, default=800, help="limit for test files")
    ap.add_argument("--allow", type=int, default=10, help="lines an already-too-long file may grow")
    ap.add_argument("--ext", action="append", default=None, help="extension, repeatable (default .rs)")
    args = ap.parse_args()
    exts = tuple(args.ext or [".rs"])

    # AMR: added, modified, renamed. A renamed file is compared with its old path.
    rows = git("diff", "-M", "--name-status", "--diff-filter=AMR", f"{args.base}...{args.head}").splitlines()
    failures = []
    for row in rows:
        status, *paths = row.split("\t")
        status, path, old_path = status[0], paths[-1], paths[0]
        if not path.endswith(exts):
            continue
        new = git("show", f"{args.head}:{path}")
        if OVERRIDE in new:
            continue
        limit = args.max_test if is_test_path(path) else args.max
        after = line_count(new)
        if after <= limit:
            continue
        if status == "A":
            failures.append(f"{path}: new file has {after} lines, limit {limit}")
            continue
        before = line_count(git("show", f"{args.base}:{old_path}"))
        if before <= limit:
            failures.append(f"{path}: grew {before} -> {after} lines, crossed limit {limit}")
        elif after - before > args.allow:
            failures.append(f"{path}: grew {before} -> {after} lines (+{after - before}, allowed +{args.allow}), limit {limit}")

    if failures:
        print("File size check failed (changed files only):")
        for f in failures:
            print(f"  - {f}")
        print("Split the file, or add a line with `lint:file-size-ok <reason>`.")
        return 1
    print(f"File size check passed ({len(rows)} changed file(s) examined).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
