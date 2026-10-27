#!/usr/bin/env python3
"""
Diff two contract snapshots and detect breaking changes.

Usage: diff-contract-snapshots.py <base.json> <head.json>

Outputs BREAKING: or ADDITIVE: lines for each detected change.
Exit code 0 always (the CI workflow decides pass/fail based on output).
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Diff LastDB contract snapshots'
    )
    parser.add_argument(
        'base',
        type=Path,
        help='Path to base snapshot JSON'
    )
    parser.add_argument(
        'head',
        type=Path,
        help='Path to head snapshot JSON'
    )
    return parser.parse_args()


def diff_routes(
    base: list[dict], head: list[dict], surface: str
) -> list[str]:
    """Detect route changes."""
    changes = []
    base_paths = {
        (r['method'], r['path']): r for r in base
    }
    head_paths = {
        (r['method'], r['path']): r for r in head
    }
    
    # Removed routes
    for key in base_paths:
        if key not in head_paths:
            method, path = key
            changes.append(
                f"BREAKING: {surface}: removed route {method} {path}"
            )
    
    # Added routes
    for key in head_paths:
        if key not in base_paths:
            method, path = key
            changes.append(
                f"ADDITIVE: {surface}: added route {method} {path}"
            )
    
    return changes


def diff_error_kinds(
    base: list[str], head: list[str], surface: str
) -> list[str]:
    """Detect error kind changes."""
    changes = []
    base_set = set(base)
    head_set = set(head)
    
    # Removed error kinds
    for kind in base_set - head_set:
        changes.append(
            f"BREAKING: {surface}: removed error kind '{kind}'"
        )
    
    # Added error kinds
    for kind in head_set - base_set:
        changes.append(
            f"ADDITIVE: {surface}: added error kind '{kind}'"
        )
    
    return changes


def diff_sdk_exports(
    base: list[str], head: list[str], surface: str
) -> list[str]:
    """Detect SDK export changes."""
    changes = []
    base_set = set(base)
    head_set = set(head)
    
    # Removed exports
    for export in base_set - head_set:
        changes.append(
            f"BREAKING: {surface}: removed SDK export '{export}'"
        )
    
    # Added exports
    for export in head_set - base_set:
        changes.append(
            f"ADDITIVE: {surface}: added SDK export '{export}'"
        )
    
    return changes


def diff_outbox_schema(
    base: dict, head: dict, surface: str
) -> list[str]:
    """Detect outbox schema changes."""
    changes = []
    base_fields = base.get('fields', {})
    head_fields = head.get('fields', {})
    
    # Removed fields
    for field in base_fields:
        if field not in head_fields:
            changes.append(
                f"BREAKING: {surface}: removed field '{field}'"
            )
        elif base_fields[field] != head_fields.get(field):
            changes.append(
                f"BREAKING: {surface}: changed field '{field}' type "
                f"from {base_fields[field]} to {head_fields[field]}"
            )
    
    # Added fields
    for field in head_fields:
        if field not in base_fields:
            changes.append(
                f"ADDITIVE: {surface}: added field '{field}'"
            )
    
    # Inbox path change
    if base.get('inbox_path') != head.get('inbox_path'):
        changes.append(
            f"BREAKING: {surface}: changed inbox path "
            f"from {base.get('inbox_path')} "
            f"to {head.get('inbox_path')}"
        )
    
    return changes


def main() -> int:
    """Compare base and head snapshots, output diff."""
    args = parse_args()
    
    if not args.base.exists():
        print(f"Base snapshot not found: {args.base}", file=sys.stderr)
        return 1
    
    if not args.head.exists():
        print(f"Head snapshot not found: {args.head}", file=sys.stderr)
        return 1
    
    base = json.loads(args.base.read_text())
    head = json.loads(args.head.read_text())
    
    changes: list[str] = []
    
    # HTTP API surface
    http_base = base.get('http_api', {})
    http_head = head.get('http_api', {})
    
    changes.extend(
        diff_routes(
            http_base.get('routes', []),
            http_head.get('routes', []),
            'http_api'
        )
    )
    changes.extend(
        diff_error_kinds(
            http_base.get('error_kinds', []),
            http_head.get('error_kinds', []),
            'http_api'
        )
    )
    changes.extend(
        diff_sdk_exports(
            http_base.get('sdk_exports', []),
            http_head.get('sdk_exports', []),
            'http_api'
        )
    )
    
    # Outbox surface
    outbox_base = base.get('outbox', {})
    outbox_head = head.get('outbox', {})
    
    changes.extend(
        diff_outbox_schema(
            outbox_base.get('schema', {}),
            outbox_head.get('schema', {}),
            'outbox'
        )
    )
    
    # Output
    if not changes:
        print("No contract surface changes detected")
    else:
        for change in changes:
            print(change)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
