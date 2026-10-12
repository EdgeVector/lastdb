#!/usr/bin/env python3
"""Fail a PR that adds more `too_many_arguments` allows than it removes.

Splitting a long function into helpers must not trade a size problem for an
argument-count problem. Group the shared values into a small context struct
instead of silencing clippy.

Rule, over the Rust files the diff touches: the number of added lines that
silence `clippy::too_many_arguments` may not exceed the number of removed ones.
Moving or splitting code that already carries the allow nets to zero and
passes; a new allow fails. Override one added line with `lint:allow-ok <reason>`
on that line or on the line above it.
"""
import argparse
import re
import subprocess
import sys

OVERRIDE = "lint:allow-ok"
LINT = "clippy::too_many_arguments"
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


def silences_lint(text):
    """A non-comment line naming the lint: the one-line attribute, or the line
    inside a multi-line `#[allow(...)]` that lists it."""
    return LINT in text and not text.lstrip().startswith("//")


def git(*args):
    out = subprocess.run(["git", *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"lint-diff-new-allows: git {' '.join(args)} failed: {out.stderr.strip()}")
    return out.stdout


def scan(diff, head):
    """Return (added, removed): added is [(path, line_no, text)], removed is a count."""
    added, removed = [], 0
    path, new_no = None, 0
    cache = {}
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = line[6:] if line.startswith("+++ b/") else None
            continue
        if line.startswith("--- "):
            continue
        m = HUNK.match(line)
        if m:
            new_no = int(m.group(1))
            continue
        if path is None or not path.endswith(".rs"):
            continue
        if line.startswith("+"):
            if silences_lint(line[1:]):
                if path not in cache:
                    cache[path] = git("show", f"{head}:{path}").splitlines()
                lines = cache[path]
                prev = lines[new_no - 2] if new_no >= 2 else ""
                if OVERRIDE not in line and OVERRIDE not in prev:
                    added.append((path, new_no, line[1:].strip()))
            new_no += 1
        elif line.startswith("-"):
            if silences_lint(line[1:]):
                removed += 1
    return added, removed


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--base", required=True, help="base commit (merge base)")
    ap.add_argument("--head", default="HEAD")
    args = ap.parse_args()

    diff = git("diff", "-U0", "-M", "--no-color", f"{args.base}...{args.head}", "--", "*.rs")
    added, removed = scan(diff, args.head)
    net = len(added) - removed
    if net <= 0:
        print(f"allow check passed: {len(added)} added, {removed} removed")
        return
    print(
        f"allow check failed: this PR adds {len(added)} `too_many_arguments` "
        f"allow(s) and removes {removed}.",
        file=sys.stderr,
    )
    for path, no, text in added:
        print(f"  {path}:{no}: {text}", file=sys.stderr)
    print(
        "Group the shared values into a small context struct (or pick a split with "
        "fewer inputs) instead of silencing clippy. Override one line with "
        f"`{OVERRIDE} <reason>`.",
        file=sys.stderr,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
