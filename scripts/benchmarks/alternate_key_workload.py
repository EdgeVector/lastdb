"""Accepted alternate-key benchmark input, independent of retired fixture suites."""

import hashlib
import json


ENVIRONMENT = {
    "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_TTL_SWEEP": "0",
    "LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "EXEMEM_API_URL": "http://127.0.0.1:9",
    "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def task_definition(name="AlternateTask"):
    # Preserve the accepted benchmark's declaration and hash bytes exactly.
    fields = ["record_id", "group_id", "owner_id", "payload"]
    declaration = "fd_alternate_task"
    hashes = {}
    for field in fields:
        value = {"algo": 2, "declaration": declaration, "name": field,
                 "type": "String", "version": 1}
        raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        hashes[field] = hashlib.sha256(raw).hexdigest()
    definition = {
        "source": "system_seed", "name": name, "descriptive_name": name,
        "schema_type": "Hash", "key": {"hash_field": "record_id"},
        "fields": fields, "field_types": {field: "String" for field in fields},
        "field_descriptions": {field: f"alternate task canonical property {field}" for field in fields},
        "field_versions": {field: 1 for field in fields},
        "field_declarations": {field: declaration for field in fields},
        "field_hashes": hashes,
        "alternate_keys": {
            "by_group": {"hash_field": "group_id", "range_field": "record_id", "missing": "reject"},
            "by_owner": {"hash_field": "owner_id", "range_field": "record_id", "missing": "omit"},
        },
    }
    definition["field_types"]["owner_id"] = {"OneOf": ["String", "Null"]}
    return definition


def install(node, definition):
    result = node.call("/api/schemas/seed-system",
                       {"isolated_copy": True, "schema": definition})[0]
    require(result.get("system_seed_schema") == definition["name"], "schema seed receipt differs")
    return definition["name"]
