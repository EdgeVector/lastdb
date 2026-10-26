#!/usr/bin/env python3
"""
Generate a contract snapshot for LastDB's HTTP API and outbox surfaces.

Captures the request-grammar version, HTTP routes, error kinds, SDK exports,
and IndexChangeBatch format for CI contract-diff checking.
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate LastDB contract snapshot'
    )
    parser.add_argument(
        '--repo-root',
        type=Path,
        required=True,
        help='Path to repository root'
    )
    return parser.parse_args()


def parse_data_routes(path: Path) -> list[dict[str, Any]]:
    """Extract DataRoute variants from uds_router/data_route.rs."""
    content = path.read_text()
    routes = []
    
    # Find enum DataRoute block
    match = re.search(
        r'pub enum DataRoute\s*\{(.*?)\n\}',
        content,
        re.DOTALL
    )
    if not match:
        return routes
    
    enum_body = match.group(1)
    
    # Extract each variant with its doc comment
    pattern = r'///\s*`(POST|GET|PUT|DELETE)\s+([\w\-/{}]+)`[^\n]*\n(?:\s*///[^\n]*\n)*\s*(\w+)'
    for m in re.finditer(pattern, enum_body):
        method, path, variant = m.groups()
        routes.append({
            "method": method,
            "path": path,
            "route_name": variant
        })
    
    return routes


def parse_index_change_batch(path: Path) -> dict[str, Any]:
    """Extract IndexChangeBatch schema from sink.rs."""
    content = path.read_text()
    
    # Find struct IndexChangeBatch
    match = re.search(
        r'pub struct IndexChangeBatch\s*\{(.*?)\n\}',
        content,
        re.DOTALL
    )
    if not match:
        return {}
    
    struct_body = match.group(1)
    fields = {}
    
    # Extract field names and types
    for line in struct_body.split('\n'):
        field_match = re.search(r'pub\s+(\w+):\s+([^,]+)', line)
        if field_match:
            name, typ = field_match.groups()
            fields[name] = typ.strip()
    
    return {
        "fields": fields,
        "inbox_path": "{LASTDB_HOME}/apps/search/inbox/"
    }


def parse_sdk_exports(path: Path) -> list[str]:
    """Extract public exports from lastdb_app_sdk/src/index.ts."""
    content = path.read_text()
    exports = []
    
    # Named exports
    for match in re.finditer(r'export\s+\{\s*([^}]+)\s*\}', content):
        names = match.group(1)
        for name in names.split(','):
            name = name.strip().split(' as ')[0].strip()
            if name and not name.startswith('type'):
                exports.append(name)
    
    # Direct exports
    for match in re.finditer(
        r'export\s+(?:class|function|const)\s+(\w+)',
        content
    ):
        exports.append(match.group(1))
    
    return sorted(set(exports))


class SnapshotError(Exception):
    """A contract value the snapshot cannot resolve from the source."""


VERSION_CONSTANTS = (
    'CURRENT_API_VERSION',
    'MIN_SUPPORTED_API_VERSION',
    'CURRENT_OUTBOX_FORMAT_VERSION',
    'MIN_SUPPORTED_OUTBOX_FORMAT_VERSION',
)

# Rust module paths a version constant may name instead of a literal, mapped to
# the source file (relative to the repo root) that defines that module.
# `CURRENT_API_VERSION` is `lastdb_uds::uds_router::API_VERSION`: the number
# lives with the socket router, not in `lastdb_node/src/version.rs`.
CONST_MODULE_FILES = {
    'lastdb_uds::uds_router': Path('lastdb_uds') / 'src' / 'uds_router.rs',
}

# Follow at most this many path hops before giving up (guards a cycle).
MAX_CONST_HOPS = 4


def read_u32_const(repo_root: Path, rel_path: Path, name: str) -> str:
    """Return the raw right-hand side of `pub const <name>: u32 = ...;`."""
    path = repo_root / rel_path
    if not path.is_file():
        raise SnapshotError(f"{rel_path}: file not found (looking for {name})")
    # Anchored at a line start so a commented-out definition cannot match.
    match = re.search(
        rf'^\s*pub const {name}\b\s*:\s*u32\s*=\s*([^;]+);',
        path.read_text(),
        re.MULTILINE
    )
    if not match:
        raise SnapshotError(f"{rel_path}: no `pub const {name}: u32 = ...;`")
    return match.group(1).strip()


def resolve_u32_const(repo_root: Path, rel_path: Path, name: str) -> int:
    """Resolve a u32 version constant to its number.

    The value is either a decimal literal, or a path to another constant in a
    module listed in CONST_MODULE_FILES, which is read and resolved in turn.
    Anything else is an error: a guessed default would let a breaking change
    pass (or fail) the version-bump check on a number nobody wrote.
    """
    origin = f"{rel_path}:{name}"
    for _ in range(MAX_CONST_HOPS):
        value = read_u32_const(repo_root, rel_path, name)
        literal = re.fullmatch(r'(\d[\d_]*)(?:u32)?', value)
        if literal:
            return int(literal.group(1).replace('_', ''))
        path_match = re.fullmatch(r'(?:crate::|::)?((?:\w+::)+)(\w+)', value)
        if not path_match:
            raise SnapshotError(
                f"{origin}: cannot resolve value `{value}` "
                "(expected a u32 literal or a path to a known constant)"
            )
        module = path_match.group(1).rstrip(':')
        if module not in CONST_MODULE_FILES:
            raise SnapshotError(
                f"{origin}: value `{value}` names module `{module}`, which "
                "generate-contract-snapshot.py does not know; add it to "
                "CONST_MODULE_FILES"
            )
        rel_path = CONST_MODULE_FILES[module]
        name = path_match.group(2)
    raise SnapshotError(
        f"{origin}: more than {MAX_CONST_HOPS} constant hops; is there a cycle?"
    )


def get_version_constants(repo_root: Path) -> dict[str, int]:
    """Resolve the contract version constants named in lastdb_node/src/version.rs."""
    version_rs = Path('lastdb_node') / 'src' / 'version.rs'
    return {
        name: resolve_u32_const(repo_root, version_rs, name)
        for name in VERSION_CONSTANTS
    }


def extract_error_kinds(path: Path) -> list[str]:
    """Extract error kind discriminators from errors.ts."""
    content = path.read_text()
    kinds = []
    
    # Error class names
    for match in re.finditer(r'export class (\w+Error)', content):
        kinds.append(match.group(1))
    
    # Capability denial reasons constant
    match = re.search(
        r'CAPABILITY_DENIAL_REASONS\s*=\s*\[(.*?)\]',
        content,
        re.DOTALL
    )
    if match:
        reasons_text = match.group(1)
        for reason_match in re.finditer(r"'(\w+)'", reasons_text):
            kinds.append(reason_match.group(1))
    
    return sorted(set(kinds))


def main() -> int:
    """Generate contract snapshot and write to stdout."""
    args = parse_args()
    repo_root = args.repo_root
    
    # Parse components
    try:
        versions = get_version_constants(repo_root)
    except SnapshotError as error:
        print(
            f"generate-contract-snapshot: cannot resolve a contract version: "
            f"{error}",
            file=sys.stderr
        )
        return 2

    routes = parse_data_routes(
        repo_root / 'lastdb_uds' / 'src' / 'uds_router' / 'data_route.rs'
    )
    
    error_kinds = extract_error_kinds(
        repo_root / 'lastdb_app_sdk' / 'src' / 'errors.ts'
    )
    
    sdk_exports = parse_sdk_exports(
        repo_root / 'lastdb_app_sdk' / 'src' / 'index.ts'
    )
    
    outbox_schema = parse_index_change_batch(
        repo_root / 'fold_db' / 'crates' / 'core' / 'src' /
        'db_operations' / 'search_index' / 'sink.rs'
    )
    
    # Build snapshot
    snapshot = {
        "http_api": {
            "version": versions['CURRENT_API_VERSION'],
            "min_supported": versions['MIN_SUPPORTED_API_VERSION'],
            "routes": routes,
            "error_kinds": error_kinds,
            "sdk_exports": sdk_exports
        },
        "outbox": {
            "version": versions['CURRENT_OUTBOX_FORMAT_VERSION'],
            "min_supported": versions['MIN_SUPPORTED_OUTBOX_FORMAT_VERSION'],
            "schema": outbox_schema
        }
    }
    
    print(json.dumps(snapshot, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
