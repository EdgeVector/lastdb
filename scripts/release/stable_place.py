#!/usr/bin/env python3
"""Place one restricted app's manifest schemas on the owner socket.

The installer posts each manifest entry once and does not read the node first
(`lastdb_node/src/bin/lastdb/app_registry_index/place_schemas.rs`):

    POST /api/apps/declare-schema
    {"app_id": <app id>, "schema": <entry with retention removed>}

When the entry has `retention`, that value sits beside `schema`. The node
names the schema `<app id>/<name>`. The same definition already in place
succeeds.
"""

from __future__ import annotations


def place_manifest_schemas(node, manifest):
    """Post every schema of a checked-out manifest that declares LastDB access."""
    app_id = manifest.get("app_id")
    schemas = manifest.get("schemas")
    if not isinstance(app_id, str) or not app_id or "/" in app_id:
        raise RuntimeError("lastdb-app.json app_id must be a non-empty name without '/'")
    if not isinstance(schemas, list):
        raise RuntimeError("manifest schemas must be a list of schema declarations")
    for entry in schemas:
        _place_one(node, app_id, entry)


def _place_one(node, app_id, entry):
    if not isinstance(entry, dict):
        raise RuntimeError("schema declaration is not an object")
    schema = dict(entry)
    name = schema.get("name") or "<unnamed>"
    retention = schema.pop("retention", None)
    body = {"app_id": app_id, "schema": schema}
    if retention is not None:
        body["retention"] = retention
    try:
        node.call("/api/apps/declare-schema", body)
    except RuntimeError as error:
        raise RuntimeError(f"schema '{name}' was refused: {error}") from error
