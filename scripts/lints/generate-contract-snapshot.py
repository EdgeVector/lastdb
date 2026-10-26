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
from typing import Any, Optional


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


class SnapshotError(Exception):
    """A contract value the snapshot cannot resolve from the source."""


def read_source(path: Path) -> str:
    """Read one parsed source file, or fail with the path that is missing."""
    if not path.is_file():
        raise SnapshotError(f"{path}: file not found")
    return path.read_text()


# Every parser below fails instead of returning an empty surface. An empty
# base surface hides every removal from the diff, and an empty head surface
# reports every entry as removed, so either way the check would be wrong.


# A route doc line names one or more methods and a path, in either form:
#   /// `GET /api/list?schema=` — ...
#   /// `POST`/`PUT` `/api/db/catalog` — ...
# A query string is not part of the route path, so it is dropped.
ROUTE_DOC_ONE = re.compile(r'`((?:GET|POST|PUT|DELETE)(?:/(?:GET|POST|PUT|DELETE))*)\s+(/[^`?\s]*)[^`]*`')
ROUTE_DOC_SPLIT = re.compile(
    r'((?:`(?:GET|POST|PUT|DELETE)`/?)+)\s+`(/[^`?\s]*)[^`]*`'
)
VARIANT_LINE = re.compile(r'^\s*([A-Z]\w*)\b')


def route_doc(doc_lines: list[str]) -> Optional[tuple[list[str], str]]:
    """Return (methods, path) from the first route doc line, if any."""
    for line in doc_lines:
        m = ROUTE_DOC_ONE.search(line)
        if m:
            return m.group(1).split('/'), m.group(2)
        m = ROUTE_DOC_SPLIT.search(line)
        if m:
            return re.findall(r'GET|POST|PUT|DELETE', m.group(1)), m.group(2)
    return None


def parse_data_routes(path: Path) -> list[dict[str, Any]]:
    """Extract DataRoute variants from uds_router/data_route.rs.

    Every variant must carry a route doc line. A variant the parser cannot
    read is an error, not a skip: a skipped variant is invisible to the diff,
    so its removal would pass the check with no BREAKING line.
    """
    content = read_source(path)

    match = re.search(
        r'pub enum DataRoute\s*\{(.*?)\n\}',
        content,
        re.DOTALL
    )
    if not match:
        raise SnapshotError(f"{path}: no `pub enum DataRoute {{ ... }}` found")

    routes = []
    unread = []
    docs: list[str] = []
    for line in match.group(1).split('\n'):
        stripped = line.strip()
        if stripped.startswith('///'):
            docs.append(stripped)
            continue
        if not stripped or stripped.startswith('#[') or stripped.startswith('//'):
            continue
        variant = VARIANT_LINE.match(line)
        if variant:
            found = route_doc(docs)
            if found is None:
                unread.append(variant.group(1))
            else:
                methods, route_path = found
                for method in methods:
                    routes.append({
                        "method": method,
                        "path": route_path,
                        "route_name": variant.group(1)
                    })
        docs = []

    if unread:
        raise SnapshotError(
            f"{path}: DataRoute variants with no readable route doc line "
            f"(/// `METHOD /path` or /// `METHOD`/`METHOD` `/path`): "
            + ", ".join(unread)
        )
    if not routes:
        raise SnapshotError(f"{path}: DataRoute has no variants")
    return routes


ROUTE_TABLE_ROW = re.compile(
    r'row\("([^"]+)",\s*Scope::\w+,\s*"(GET|POST|PUT|DELETE)",\s*DataRoute::(\w+)\)'
)


def add_route_table_rows(path: Path, routes: list[dict[str, Any]]) -> None:
    """Add the exact-path rows of uds_router/route_table.rs to `routes`.

    The table can serve one DataRoute at a second path (an alias such as
    `GET /api/db/list`). The enum docs name only one path per variant, so
    without the table a removed alias would not show in the diff.
    """
    rows = ROUTE_TABLE_ROW.findall(read_source(path))
    if not rows:
        raise SnapshotError(f"{path}: no `row(\"/path\", Scope::.., \"METHOD\", DataRoute::..)` found")
    names = {r["route_name"] for r in routes}
    have = {(r["method"], r["path"]) for r in routes}
    for route_path, method, variant in rows:
        if variant not in names:
            raise SnapshotError(
                f"{path}: row {method} {route_path} names DataRoute::{variant}, "
                "which the enum docs do not define"
            )
        if (method, route_path) not in have:
            routes.append({"method": method, "path": route_path, "route_name": variant})
            have.add((method, route_path))


def parse_index_change_batch(path: Path) -> dict[str, Any]:
    """Extract IndexChangeBatch schema from sink.rs."""
    content = read_source(path)
    
    # Find struct IndexChangeBatch
    match = re.search(
        r'pub struct IndexChangeBatch\s*\{(.*?)\n\}',
        content,
        re.DOTALL
    )
    if not match:
        raise SnapshotError(
            f"{path}: no `pub struct IndexChangeBatch {{ ... }}` found"
        )
    
    struct_body = match.group(1)
    fields = {}
    
    # Extract field names and types
    for line in struct_body.split('\n'):
        field_match = re.search(r'pub\s+(\w+):\s+([^,]+)', line)
        if field_match:
            name, typ = field_match.groups()
            fields[name] = typ.strip()
    
    if not fields:
        raise SnapshotError(f"{path}: IndexChangeBatch has no `pub` fields")
    return {
        "fields": fields,
        "inbox_path": "{LASTDB_HOME}/apps/search/inbox/"
    }


def parse_sdk_exports(path: Path) -> list[str]:
    """Extract public exports from lastdb_app_sdk/src/index.ts."""
    content = read_source(path)
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
    
    if not exports:
        raise SnapshotError(f"{path}: no SDK exports found")
    return sorted(set(exports))


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
    content = read_source(path)
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
    if not match:
        raise SnapshotError(f"{path}: no `CAPABILITY_DENIAL_REASONS = [...]` found")
    reasons = re.findall(r"'(\w+)'", match.group(1))
    if not reasons:
        raise SnapshotError(f"{path}: CAPABILITY_DENIAL_REASONS lists no reason")
    kinds.extend(reasons)
    if not any(kind.endswith('Error') for kind in kinds):
        raise SnapshotError(f"{path}: no `export class <Name>Error` found")
    
    return sorted(set(kinds))


def build_snapshot(repo_root: Path) -> dict[str, Any]:
    """Parse every contract surface. Raises SnapshotError on any gap."""
    versions = get_version_constants(repo_root)

    routes = parse_data_routes(
        repo_root / 'lastdb_uds' / 'src' / 'uds_router' / 'data_route.rs'
    )
    add_route_table_rows(
        repo_root / 'lastdb_uds' / 'src' / 'uds_router' / 'route_table.rs',
        routes
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
    
    return {
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


def main() -> int:
    """Generate contract snapshot and write to stdout."""
    args = parse_args()
    try:
        snapshot = build_snapshot(args.repo_root)
    except SnapshotError as error:
        print(
            f"generate-contract-snapshot: cannot read a contract surface: {error}",
            file=sys.stderr
        )
        return 2

    print(json.dumps(snapshot, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
