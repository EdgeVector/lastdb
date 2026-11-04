"""Explicit benchmark-only client for equivalent historical and current record operations.

Older partial edits become client read/edit/replace operations. Reads, serialization,
conflicts and bounded retries remain inside the measured call. Full-record cases
explicitly opt in and use revisions already acknowledged to this client; no record
payload cache or server compatibility path exists.
"""

from collections import Counter, defaultdict
import copy
import json
import os
import threading
import time
import uuid


def coordinate(key):
    return key.get("hash"), key.get("range")


def historical_field_list():
    """Every schema field for a whole-record read on a daemon before API 29.

    LASTDB_BENCH_FULL_FIELDS is a comma-separated list of those names.
    An unset variable keeps the older empty-list sentinel, which also means
    every runtime field. A one-name list is not a whole-record read.
    """
    return [name for name in os.environ.get("LASTDB_BENCH_FULL_FIELDS", "").split(",") if name]


def historical_query(body, api_version):
    """Add the field list that releases before API 29 require.

    API 29 rejects the key, so a current daemon keeps the request unchanged.
    The addition is outside the RPC timer.
    """
    if body is None or not 0 < api_version < 29:
        return body
    decoded = json.loads(body) if isinstance(body, (bytes, bytearray)) else body
    if not isinstance(decoded, dict):
        return body
    queries = decoded.get("queries")
    if isinstance(queries, list):
        return {**decoded, "queries": [query_with_fields(item) for item in queries]}
    return query_with_fields(decoded)


def query_with_fields(body):
    if not isinstance(body, dict) or "fields" in body:
        return body
    return {**body, "fields": historical_field_list()}


def field_value(record, path):
    for name in path.split("."):
        if not isinstance(record, dict) or name not in record:
            return None
        record = record[name]
    if isinstance(record, (dict, list)):
        raise ValueError("benchmark key coordinate must be scalar")
    return record if record is None or isinstance(record, str) else json.dumps(record)


class RecordProtocol:
    def __init__(self):
        self.schemas, self.revisions = {}, {}
        self.lock, self.totals = threading.Lock(), Counter()

    def status(self):
        with self.lock:
            return {"client": "explicit benchmark protocol adapter", "totals": dict(self.totals),
                    "acknowledged_revision_tokens": len(self.revisions), "payload_cache": False,
                    "retry_limit": 8, "mutation_timing_includes_all_client_work": True,
                    "passthrough_timing": "same RPC timer as baseline",
                    "application_timing": "complete-flow outer timer on both labels"}

    def declaration(self, body):
        body = copy.deepcopy(body)
        schema = body["schema"]
        self.schemas[schema["name"]] = copy.deepcopy(schema)
        if "record_keys" in schema:
            return body
        primary = schema.pop("key", {}) or {}
        alternates = schema.pop("alternate_keys", {})

        def definition(key, optional=False):
            ranged = key.get("range_field")
            result = {"kind": "hash_range" if ranged else "hash"}
            if ranged:
                result["range_type"] = schema.get("field_types", {}).get(ranged, "String")
            if optional:
                result["allow_absent"] = True
            return result

        schema["record_keys"] = {"primary": definition(primary), "alternates": {
            name: definition(key, key.get("missing") == "omit") for name, key in alternates.items()}}
        return body

    def addresses(self, schema, fields):
        result = {}
        for name, key in schema.get("alternate_keys", {}).items():
            address = {"hash": field_value(fields, key["hash_field"]),
                       "range": field_value(fields, key["range_field"]) if key.get("range_field") else None}
            if address["hash"] is None or key.get("range_field") and address["range"] is None:
                if key.get("missing") != "omit":
                    raise ValueError("benchmark record omits a required alternate address")
                address = None
            result[name] = address
        return result

    def remember(self, schema, rows):
        with self.lock:
            for row in rows:
                self.revisions[(schema, *coordinate(row["key"]))] = row["revision"]

    def call(self, exchange, path, body, *, api_version, complete_records=False, transport=None, **options):
        started, physical = time.perf_counter_ns(), []

        def send(route, value, **overrides):
            metrics = {}
            settings = {**options, **overrides, "transport": metrics}
            # Distinct IDs retain every physical request in phase correlation.
            request_id = options.get("request_id")
            settings["request_id"] = f"{request_id}/rpc-{len(physical)}" if request_id else None
            try:
                return exchange(route, value, **settings)
            finally:
                physical.append({"path": route, "request_id": settings["request_id"], **metrics})

        try:
            prepares_values = path in ("/api/schemas/seed-system", "/api/mutation", "/api/mutations/batch")
            if not prepares_values and not (path == "/api/query" and complete_records):
                # The caller already applied the historical field list. This
                # timer still covers only the RPC.
                return send(path, body)
            decoded = json.loads(body) if isinstance(body, bytes) else body
            if path == "/api/schemas/seed-system":
                result = send(path, self.declaration(decoded))
            elif path in ("/api/mutation", "/api/mutations/batch"):
                operations = [decoded] if path == "/api/mutation" else decoded if isinstance(decoded, list) else decoded["mutations"]
                result = send(path, decoded) if all("record" in item for item in operations) else self.mutations(send, path, decoded, complete_records, api_version)
            else:
                result = send(path, body)
                if path == "/api/query" and isinstance(decoded, dict):
                    self.remember(decoded["schema_name"], result[0].get("results", []))
            return result[0], (time.perf_counter_ns() - started) / 1e6, result[2]
        finally:
            total = {name: sum(item.get(name, 0) for item in physical)
                     for name in ("request_body_bytes", "response_body_bytes")}
            total.update(rpc_count=len(physical), physical_requests=physical,
                         retries=sum(item.get("response_status") == 409 for item in physical))
            if transport is not None:
                transport.update(total)
            with self.lock:
                self.totals.update(logical_calls=1, physical_rpcs=len(physical), **{
                    name: total[name] for name in ("request_body_bytes", "response_body_bytes")})

    def read_current(self, send, mutations, api_version):
        grouped = defaultdict(list)
        for mutation in mutations:
            if mutation["mutation_type"] != "create":
                grouped[mutation["schema"]].append(mutation["key_value"])
        queries = [historical_query({"schema_name": schema, "filter": {"HashRangeKeys": [
            [key["hash"], key.get("range") or ""] for key in keys]}, "limit": len(keys)}, api_version)
            for schema, keys in grouped.items()]
        if not queries:
            return {}
        if len(queries) == 1:
            replies = [send("/api/query", queries[0], receipt=True)[0]]
        else:
            reply = send("/api/queries/batch", {"queries": queries}, receipt=True)[0]
            replies = []
            for item in reply["results"]:
                if item["status"] != 200:
                    raise RuntimeError("benchmark precondition batch read failed")
                replies.append(item["response"].get("data", item["response"]))
        return {(schema, *coordinate(row["key"])): row
                for schema, reply in zip(grouped, replies) for row in reply["results"]}

    def mutations(self, send, path, body, complete, api_version):
        original = [body] if path == "/api/mutation" else body if isinstance(body, list) else body["mutations"]
        for attempt in range(9):
            current = {} if complete else self.read_current(send, original, api_version)
            converted = []
            for mutation in original:
                identity = (mutation["schema"], *coordinate(mutation["key_value"]))
                prior = current.get(identity)
                with self.lock:
                    revision = self.revisions.get(identity) if complete else prior and prior["revision"]
                fields = dict(prior["fields"]) if prior else {}
                fields.update(mutation["fields_and_values"])
                expected = mutation.get("expected")
                if expected and not complete:
                    matches = (not prior if expected["type"] == "absent" else
                               prior and prior["fields"].get(expected["field"]) == expected["value"])
                    if not matches:
                        raise RuntimeError("benchmark logical field precondition failed")
                deleting = mutation["mutation_type"] in ("delete", "purge")
                if deleting and not prior:
                    raise RuntimeError("benchmark delete target is absent")
                metadata = (prior or {}).get("metadata") or {}
                converted.append({"type": "mutation",
                    "schema": mutation["schema"], "key_value": mutation["key_value"],
                    "mutation_type": mutation["mutation_type"] if deleting else "replace" if revision else "create",
                    "record": {} if deleting else fields,
                    "alternates": prior["alternates"] if deleting else self.addresses(self.schemas[mutation["schema"]], fields),
                    "expected": {"type": "revision", "revision": revision} if revision else {"type": "absent"},
                    "metadata": None if deleting else mutation.get("metadata", metadata.get("metadata")),
                    "source_file_name": None if deleting else mutation.get("source_file_name", metadata.get("source_file_name")),
                    "file": None if deleting else metadata.get("file"),
                    "durability": mutation.get("durability", "durable"),
                    "convergence": mutation.get("convergence", "async")})
            # Historical API27 requires caller IDs; current product requests never send them.
            if api_version == 27:
                for mutation in converted:
                    mutation["mutation_id"] = str(uuid.uuid4())
            try:
                reply = send(path, converted[0] if path == "/api/mutation" else {"mutations": converted})
            except RuntimeError as error:
                if getattr(error, "status", None) != 409 or complete or any(m.get("expected") for m in original) or attempt == 8:
                    raise
                with self.lock:
                    self.totals.update(conflicts=1, retries=1)
                continue
            with self.lock:
                ids = [reply[0]["mutation_id"]] if path == "/api/mutation" else reply[0]["mutation_ids"]
                if len(ids) != len(converted) or len(set(ids)) != len(ids):
                    raise RuntimeError("benchmark response has incomplete server identities")
                for mutation, mutation_id in zip(converted, ids):
                    identity = (mutation["schema"], *coordinate(mutation["key_value"]))
                    revision = reply[0]["record_revisions"][mutation_id]
                    if mutation["mutation_type"] in ("delete", "purge"):
                        self.revisions.pop(identity, None)
                    else:
                        self.revisions[identity] = revision
            return reply
