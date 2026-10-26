#!/usr/bin/env python3
"""
Generate a contract snapshot for LastDB's HTTP API and outbox surfaces.

Captures the request-grammar version, HTTP routes, error kinds, SDK exports,
and IndexChangeBatch format for CI contract-diff checking.
"""

import json
import re
import sys
from pathlib import Path
from typing import Any


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


def get_version_constants(path: Path) -> dict[str, int]:
    """Extract version constants from lastdb_node/src/version.rs."""
    content = path.read_text()
    versions = {}
    
    for const in [
        'CURRENT_API_VERSION',
        'MIN_SUPPORTED_API_VERSION',
        'CURRENT_OUTBOX_FORMAT_VERSION',
        'MIN_SUPPORTED_OUTBOX_FORMAT_VERSION'
    ]:
        match = re.search(
            rf'pub const {const}:\s*u32\s*=\s*(\d+)',
            content
        )
        if match:
            versions[const] = int(match.group(1))
    
    return versions


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
    repo_root = Path(__file__).parent.parent.parent
    
    # Parse components
    versions = get_version_constants(
        repo_root / 'lastdb_node' / 'src' / 'version.rs'
    )
    
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
            "version": versions.get('CURRENT_API_VERSION', 1),
            "min_supported": versions.get('MIN_SUPPORTED_API_VERSION', 1),
            "routes": routes,
            "error_kinds": error_kinds,
            "sdk_exports": sdk_exports
        },
        "outbox": {
            "version": versions.get(
                'CURRENT_OUTBOX_FORMAT_VERSION', 1
            ),
            "min_supported": versions.get(
                'MIN_SUPPORTED_OUTBOX_FORMAT_VERSION', 1
            ),
            "schema": outbox_schema
        }
    }
    
    print(json.dumps(snapshot, indent=2))
    return 0


if __name__ == '__main__':
    sys.exit(main())
