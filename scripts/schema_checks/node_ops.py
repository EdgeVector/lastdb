"""Finite real-node requests for the canonical schema CI check."""

import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from node import Node, file_hash, save  # noqa: E402

FIELDS = ["record_id", "group_id", "payload"]
ENVIRONMENT = {
    "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_TTL_SWEEP": "0",
    "LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "EXEMEM_API_URL": "http://127.0.0.1:9",
    "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(value):
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def schema(name, family, cross=False, declared=True, fields=None):
    names = list(fields or FIELDS)
    definition = {
        "source": "system_seed", "name": name, "descriptive_name": name,
        "schema_type": "HashRange" if cross else "Hash",
        "key": {"hash_field": "group_id" if cross else "record_id"},
        "fields": names, "field_types": {field: "String" for field in names},
        "field_descriptions": {field: f"{family} canonical property {field}" for field in names},
        "field_versions": {field: 1 for field in names},
    }
    if cross:
        definition["key"]["range_field"] = "record_id"
    if declared:
        declaration = "fd_" + family.replace(" ", "_")
        definition["field_declarations"] = {field: declaration for field in names}
        definition["field_hashes"] = {
            field: digest({"algo": 2, "declaration": declaration, "name": field,
                           "type": "String", "version": 1}) for field in names}
    return definition


def install(node, definition):
    result = node.call("/api/schemas/seed-system",
                       {"isolated_copy": True, "schema": definition})[0]
    require(result.get("system_seed_schema") == definition["name"], "schema seed receipt differs")
    return definition["name"]


def metadata(node, name):
    return node.call("/api/schema/" + quote(name, safe=""))[0]["schema"]


def molecule(node, name, field="payload"):
    return metadata(node, name)["field_molecule_uuids"][field]


def protein(node, molecule_id):
    result = node.call("/api/protein/of-molecule/" + quote(molecule_id, safe=""))[0]
    require(result.get("molecule_uuid") == molecule_id, "protein query named another molecule")
    return result["protein_uuid"]


def write(node, name, key, values=None, operation="create"):
    request = {"type": "mutation", "schema": name, "mutation_type": operation,
               "durability": "durable", "convergence": "sync", "key_value": key,
               "fields_and_values": values or {}}
    result = node.call("/api/mutation", request)[0]
    require(result.get("durability") == "durable", "mutation lacks a durable acknowledgement")
    return result


def read(node, name, key, expected, names=None):
    predicate = {"HashRangeKeys": [[key["hash"], key["range"]]]} if "range" in key else {"HashKey": key["hash"]}
    result = node.call("/api/query", {"schema_name": name, "fields": names or FIELDS,
                                      "filter": predicate, "limit": 1})[0]
    records = result.get("results", [])
    if expected is None:
        require(not records, f"{name}: removed or isolated key returned values")
        return None
    require(len(records) == 1, f"{name}: expected one keyed record, got {len(records)}")
    record = records[0]
    require(record["fields"] == expected, f"{name}: keyed values differ: {record['fields']!r}")
    require(record["key"]["hash"] == key["hash"], f"{name}: wrong hash")
    if "range" in key:
        require(record["key"].get("range") == key["range"], f"{name}: wrong range")
    return record


def same_atom(*records):
    identities = [record["metadata"]["payload"]["atom_uuid"] for record in records]
    require(all(identities) and len(set(identities)) == 1, "linked records do not share one payload atom")
    return identities[0]


def restart(node, crash=False):
    if crash:
        require(node.process is not None and node.process.poll() is None, "owned daemon is not active")
        process = node.process
        process.kill()
        process.wait(timeout=10)
        node.process = None
        node.log.close()
        node.log = None
    else:
        node.stop()
    node.start()


def http_status(node, path, body=None):
    from node import UnixConnection
    connection = UnixConnection(node.socket)
    try:
        raw = json.dumps(body).encode() if body is not None else None
        connection.request("POST" if body is not None else "GET", path, raw,
                           {"Content-Type": "application/json"})
        response = connection.getresponse()
        response.read()
        return response.status
    finally:
        connection.close()
