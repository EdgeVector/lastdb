#!/usr/bin/env python3
"""Place one restricted app's manifest schemas on the owner socket.

The installer posts each manifest entry once and does not read the node first
(`lastdb_node/src/bin/lastdb/app_registry_index/place_schemas.rs`):

    POST /api/apps/declare-schema
    {"app_id": <app id>, "schema": <entry with retention removed>}

When the entry has `retention`, that value sits beside `schema`. The node
names the schema `<app id>/<name>`. The same definition already in place
succeeds.

A `fields` object is rewritten into the stored field list before the post.
That rewrite matches `schema_types::expand_manifest_schema`. An array
`fields` value is posted unchanged. Field names are sorted: the Rust reader
stores object keys in sorted order, and the portable definition id keeps the
`fields` array order.
"""

from __future__ import annotations


_KNOWN = (
    "type",
    "description",
    "classifications",
    "interest_category",
    "version",
    "declaration",
    "hash",
)
_MAPS = {
    "type": "field_types",
    "description": "field_descriptions",
    "classifications": "field_classifications",
    "interest_category": "field_interest_categories",
    "version": "field_versions",
    "declaration": "field_declarations",
    "hash": "field_hashes",
}
_STRINGS = ("description", "interest_category", "declaration", "hash")


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
    schema = expand_manifest_schema(schema)
    body = {"app_id": app_id, "schema": schema}
    if retention is not None:
        body["retention"] = retention
    try:
        node.call("/api/apps/declare-schema", body)
    except RuntimeError as error:
        raise RuntimeError(f"schema '{name}' was refused: {error}") from error


def expand_manifest_schema(entry):
    """Rewrite a field-object entry into the field list and `field_*` maps."""
    if not isinstance(entry, dict) or not isinstance(entry.get("fields"), dict):
        return entry
    label = _label(entry)
    mixed = sorted(key for key in entry if key.startswith("field_"))
    if mixed:
        raise RuntimeError(
            f"schema '{label}' uses the field object form and also sets '{mixed[0]}'; "
            "write each field once, or keep the older field list and field_* maps"
        )
    names = sorted(entry["fields"])
    maps = {dest: {} for dest in dict.fromkeys(_MAPS.values())}
    for name in names:
        spec = entry["fields"][name]
        if not isinstance(spec, dict):
            raise RuntimeError(f"schema '{label}' field '{name}' must be an object with a type")
        _reject_field(label, name, spec)
        for prop, dest in _MAPS.items():
            if prop in spec:
                maps[dest][name] = spec[prop]
    expanded = dict(entry)
    expanded["fields"] = names
    for dest, values in maps.items():
        if values:
            expanded[dest] = values
    return expanded


def _label(entry):
    name = entry.get("name")
    return name if isinstance(name, str) else "<unnamed>"


def _reject_field(label, name, spec):
    unknown = sorted(set(spec) - set(_KNOWN))
    if unknown:
        raise RuntimeError(f"schema '{label}' field '{name}' has unknown property '{unknown[0]}'")
    if "type" not in spec or spec["type"] is None:
        raise RuntimeError(f"schema '{label}' field '{name}' needs a type")
    for prop in _STRINGS:
        if prop in spec and not isinstance(spec[prop], str):
            raise RuntimeError(
                f"schema '{label}' field '{name}' property '{prop}' must be a string"
            )
    _reject_classifications(label, name, spec)
    _reject_version(label, name, spec)


def _reject_classifications(label, name, spec):
    if "classifications" not in spec:
        return
    value = spec["classifications"]
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise RuntimeError(
            f"schema '{label}' field '{name}' property 'classifications' must be an array of strings"
        )


def _reject_version(label, name, spec):
    if "version" not in spec:
        return
    value = spec["version"]
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 4_294_967_295:
        raise RuntimeError(
            f"schema '{label}' field '{name}' property 'version' must be an integer from 0 through 4294967295"
        )
