#!/usr/bin/env python3
"""Fail a PR that makes a Rust function too long. Only touched functions count.

A function is "touched" when a line the diff adds or changes lies inside it.
For each touched function that ends over the limit:
  - new function                        -> fail
  - existing function that was within the limit and crossed it -> fail
  - existing over-limit function that grew by more than --allow lines -> fail
  - existing over-limit function that grew by --allow lines or fewer  -> pass
  - existing function that shrank or stayed equal                     -> pass
Functions the PR does not touch are never checked.

Test code (a tests/ path, #[cfg(test)] module, or #[test] fn) uses --max-test.
Override one function with `lint:fn-size-ok <reason>` inside it or on the
line above it. Needs: pip install tree-sitter==0.23.2 tree-sitter-rust==0.23.2
"""
import argparse
import re
import subprocess
import sys

import tree_sitter_rust as ts_rust
from tree_sitter import Language, Parser

OVERRIDE = "lint:fn-size-ok"
HUNK = re.compile(r"^@@ -\S+ \+(\d+)(?:,(\d+))? @@")


def git(*args):
    out = subprocess.run(["git", *args], capture_output=True, text=True)
    if out.returncode != 0:
        sys.exit(f"lint-diff-fn-size: git {' '.join(args)} failed: {out.stderr.strip()}")
    return out.stdout


def changed_lines(base, head, old_path, path):
    """New-side line numbers the diff adds or changes."""
    lines = set()
    for row in git("diff", "-M", "-U0", f"{base}...{head}", "--", old_path, path).splitlines():
        m = HUNK.match(row)
        if m:
            start = int(m.group(1))
            count = int(m.group(2)) if m.group(2) is not None else 1
            lines.update(range(start, start + count))
    return lines


def attrs_before(node):
    """Text of attribute items and comments directly above a node."""
    out, prev = [], node.prev_sibling
    while prev is not None and prev.type in ("attribute_item", "line_comment", "block_comment"):
        out.append(prev.text.decode())
        prev = prev.prev_sibling
    return " ".join(out)


def functions(src, parser):
    """Return (qualified_name, start_line, end_line, in_test) for every fn."""
    found = []

    def walk(node, scope, in_test):
        if node.type == "mod_item":
            in_test = in_test or "cfg(test)" in attrs_before(node)
            name = node.child_by_field_name("name")
            scope = scope + [name.text.decode() if name else "?"]
        elif node.type == "impl_item":
            ty = node.child_by_field_name("type")
            scope = scope + [ty.text.decode() if ty else "?"]
        elif node.type == "function_item":
            name = node.child_by_field_name("name").text.decode()
            test = in_test or "#[test]" in attrs_before(node).replace(" ", "")
            found.append(("::".join(scope + [name]), node.start_point[0] + 1, node.end_point[0] + 1, test))
            scope = scope + [name]
        for child in node.children:
            walk(child, scope, in_test)

    walk(parser.parse(src).root_node, [], False)
    return found


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--base", required=True)
    ap.add_argument("--head", default="HEAD")
    ap.add_argument("--max", type=int, default=100)
    ap.add_argument("--allow", type=int, default=10, help="lines an already-too-long function may grow")
    ap.add_argument("--max-test", type=int, default=200)
    args = ap.parse_args()
    parser = Parser(Language(ts_rust.language()))

    rows = git("diff", "-M", "--name-status", "--diff-filter=AMR", f"{args.base}...{args.head}").splitlines()
    failures = []
    for row in rows:
        status, *paths = row.split("\t")
        status, path, old_path = status[0], paths[-1], paths[0]
        if not path.endswith(".rs"):
            continue
        src = git("show", f"{args.head}:{path}").encode()
        touched = changed_lines(args.base, args.head, old_path, path)
        base_len = {}
        if status in ("M", "R"):
            for name, s, e, _ in functions(git("show", f"{args.base}:{old_path}").encode(), parser):
                base_len[name] = max(base_len.get(name, 0), e - s + 1)
        text_lines = src.decode().splitlines()
        for name, start, end, in_test in functions(src, parser):
            length = end - start + 1
            limit = args.max_test if (in_test or "/tests/" in path) else args.max
            if length <= limit or not touched.intersection(range(start, end + 1)):
                continue
            if any(OVERRIDE in l for l in text_lines[max(start - 2, 0):end]):
                continue
            if name not in base_len:
                failures.append(f"{path}:{start} fn {name}: new function has {length} lines, limit {limit}")
            elif base_len[name] <= limit:
                failures.append(f"{path}:{start} fn {name}: grew {base_len[name]} -> {length} lines, crossed limit {limit}")
            elif length - base_len[name] > args.allow:
                failures.append(f"{path}:{start} fn {name}: grew {base_len[name]} -> {length} lines (+{length - base_len[name]}, allowed +{args.allow}), limit {limit}")

    if failures:
        print("Function size check failed (touched functions only):")
        for f in failures:
            print(f"  - {f}")
        print("Split the function, or add `lint:fn-size-ok <reason>` inside it.")
        return 1
    print("Function size check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
