#!/usr/bin/env python3
"""Check finite Python helper syntax and repository source-size limits."""

import ast
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main():
    paths = sorted(ROOT.glob("*.py"))
    if len(paths) > 100:
        raise ValueError("The finite helper inventory exceeds one hundred sources")
    rows = []
    for path in paths:
        raw = path.read_bytes()
        tree = ast.parse(raw, filename=str(path))
        lines = len(raw.splitlines())
        sizes = [node.end_lineno - node.lineno + 1 for node in ast.walk(tree)
                 if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
        if lines > 400 or max(sizes, default=0) > 100:
            raise ValueError("A helper exceeds four hundred lines or a function exceeds one hundred lines: " + path.name)
        rows.append({"path": path.name, "lines": lines, "largest_function_lines": max(sizes, default=0),
                     "sha256": hashlib.sha256(raw).hexdigest()})
    print(json.dumps({"source_checks_pass": True, "helpers": rows}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
