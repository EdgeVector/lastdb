"""Bounded generated data for the CI-only sustained V2 measurement."""

import base64
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import hashlib
import math
import time

SCHEMA = "SustainedRecords"
CANARY_SCHEMA = "SustainedCanary"
FIELDS = ["hash", "range", "body"]
BUCKET = 1024
BODY_BYTES = 1024
ERROR_LIMIT = 16


class IntegrityError(RuntimeError):
    """A generated read differs from the data admitted by this workload."""


def coordinates(number):
    return f"bucket-{number // BUCKET:09d}", f"{number:012d}"


def payload(number, revision, body_bytes=BODY_BYTES):
    """High-entropy printable text, with its identity and version inside it."""
    prefix = f"{number:012d}:{revision:012d}:"
    if len(prefix) >= body_bytes:
        raise ValueError("The generated body is too small for its identity")
    size = body_bytes - len(prefix)
    raw = hashlib.shake_256(f"sustained-v1:{number}:{revision}".encode()).digest(
        (size * 4 + 4) // 5)
    return prefix + base64.b85encode(raw).decode()[:size]


def schema_request(name=SCHEMA):
    return {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": name, "descriptive_name": name,
        "schema_type": "HashRange", "key": {"hash_field": "hash", "range_field": "range"},
        "fields": FIELDS,
        "field_descriptions": {field: f"generated {name} measurement {field}" for field in FIELDS}}}


def write_request(numbers, revision, create=False, body_bytes=BODY_BYTES):
    mutations = []
    for number in numbers:
        hashed, ranged = coordinates(number)
        fields = {"body": payload(number, revision, body_bytes)}
        if create:
            fields.update(hash=hashed, range=ranged)
        mutations.append({"type": "mutation", "schema": SCHEMA,
                          "mutation_type": "create" if create else "update",
                          "durability": "durable", "key_value": {"hash": hashed, "range": ranged},
                          "fields_and_values": fields})
    return {"mutations": mutations}


def keyed_request(numbers):
    numbers = list(numbers)
    return {"schema_name": SCHEMA, "fields": FIELDS, "limit": len(numbers),
            "filter": {"HashRangeKeys": [list(coordinates(number)) for number in numbers]}}


def range_request(first, count=32):
    if first // BUCKET != (first + count - 1) // BUCKET:
        raise ValueError("A generated range must stay under one hash")
    return {"schema_name": SCHEMA, "fields": FIELDS, "limit": count,
            "filter": {"HashRangeRange": {"hash": coordinates(first)[0],
                       "start": f"{first:012d}", "end": f"{first + count:012d}"}}}


def validate_read(result, expected_ids=None, body_bytes=BODY_BYTES, ordered=False):
    """Check membership and body integrity; concurrent versions may differ."""
    rows = result.get("results", [])
    if result.get("returned_count") != len(rows) or result.get("unresolved_rows", 0):
        raise IntegrityError("A generated query returned an incomplete result")
    found = {}
    for row in rows:
        key, fields = row.get("key", {}), row.get("fields", {})
        try:
            number = int(key["range"])
            body = fields["body"]
            stored_id, version, _ = body.split(":", 2)
            revision = int(version)
        except (KeyError, TypeError, ValueError, AttributeError) as error:
            raise IntegrityError("A generated record has an invalid shape") from error
        hashed, ranged = coordinates(number)
        if (number in found or key != {"hash": hashed, "range": ranged}
                or fields != {"hash": hashed, "range": ranged, "body": body}
                or stored_id != ranged or revision < 0
                or body != payload(number, revision, body_bytes)):
            raise IntegrityError("A generated record has incorrect keys or content")
        found[number] = revision
    if expected_ids is not None:
        expected = list(expected_ids)
        if set(found) != set(expected) or (ordered and list(found) != expected):
            raise IntegrityError("A generated query returned different keys or range order")
    return found


def mutation_outcome(result, count):
    """Return acknowledged successes and failures, including item errors."""
    errors = result.get("errors") or []
    details = None
    for name in ("results", "mutations", "operations"):
        items = result.get(name)
        if isinstance(items, list) and len(items) == count and all(isinstance(i, dict) for i in items):
            details = items
            break
    bad = [item for item in details or [] if item.get("error") or item.get("errors")
           or item.get("success") is False or item.get("ok") is False
           or str(item.get("status", "")).lower() in ("error", "failed")]
    if result.get("durability") != "durable":
        return 0, count, "A write lacks a durable acknowledgement"
    if errors and not bad:
        return 0, count, f"A write returned operation errors: {str(errors)[:300]}"
    good = count - len(bad)
    if result.get("count") not in (count, good):
        return 0, count, "A write returned an incomplete operation count"
    ids = result.get("mutation_ids")
    if (not isinstance(ids, list) or len(ids) != count
            or any(not isinstance(identity, str) or not identity for identity in ids)):
        return 0, count, "A write returned an incomplete mutation identity list"
    if bad:
        return good, len(bad), f"A write returned operation errors: {str(bad[0])[:300]}"
    return count, 0, None


def write_canary_request(version):
    fields = {"hash": "canary", "range": "000000000000", "body": payload(0, version)}
    return {"mutations": [{"type": "mutation", "schema": CANARY_SCHEMA,
            "mutation_type": "create" if version == 0 else "update", "durability": "durable",
            "key_value": {"hash": "canary", "range": "000000000000"},
            "fields_and_values": fields}]}


def canary_query():
    return {"schema_name": CANARY_SCHEMA, "fields": FIELDS, "limit": 1,
            "filter": {"HashRangeKeys": [["canary", "000000000000"]]}}


def validate_canary(result):
    rows = result.get("results", [])
    if result.get("returned_count") != 1 or len(rows) != 1 or result.get("unresolved_rows", 0):
        raise IntegrityError("The durable canary is missing or incomplete")
    row = rows[0]
    try:
        version = int(row["fields"]["body"].split(":", 2)[1])
    except (KeyError, TypeError, ValueError, AttributeError, IndexError) as error:
        raise IntegrityError("The durable canary body has an invalid shape") from error
    fields = {"hash": "canary", "range": "000000000000", "body": payload(0, version)}
    if row.get("key") != {"hash": "canary", "range": "000000000000"} or row.get("fields") != fields:
        raise IntegrityError("The durable canary has incorrect keys or content")
    return version


def _put_seed(node, first, count, body_bytes):
    result, _, _ = node.call("/api/mutations/batch",
                             write_request(range(first, first + count), 0, True, body_bytes))
    good, failed, error = mutation_outcome(result, count)
    if failed:
        raise RuntimeError(error)
    return good


def _seed_progress(metadata, telemetry, inflight, started):
    snapshot = {"phase": "seed", "offered": metadata["submitted"],
                "success": metadata["records"], "failed": metadata["failed"],
                "inflight_requests": inflight, "elapsed_seconds": round(time.monotonic() - started, 2)}
    telemetry.set_load(snapshot)
    return snapshot


def seed(node, args, telemetry):
    """Seed GiB of body text, with at most sixteen 64-record calls in flight."""
    body_bytes = getattr(args, "body_bytes", BODY_BYTES)
    target = math.ceil(args.gib * 1024 ** 3 / body_bytes)
    batch, workers = min(64, getattr(args, "batch", 64)), min(16, getattr(args, "workers", 16))
    if min(target, batch, workers, args.seed_seconds) <= 0:
        raise ValueError("Seed size, batch, workers, and deadline must be positive")
    metadata = {"records": 0, "target_records": target, "body_bytes": body_bytes,
                "target_payload_bytes": target * body_bytes, "submitted": 0, "failed": 0,
                "complete": False, "errors": [], "payload_recipe": "shake256/base85-v1"}
    started, next_progress = time.monotonic(), time.monotonic() + 60
    deadline, pending, stopped = started + args.seed_seconds, {}, False
    try:
        for name in (SCHEMA, CANARY_SCHEMA):
            node.call("/api/schemas/seed-system", schema_request(name))
        result, _, _ = node.call("/api/mutations/batch", write_canary_request(0))
        if mutation_outcome(result, 1)[1]:
            raise RuntimeError("The initial durable canary write failed")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            while pending or (metadata["submitted"] < target and not stopped):
                if time.monotonic() >= deadline:
                    stopped = True
                while not stopped and len(pending) < workers and metadata["submitted"] < target:
                    first = metadata["submitted"]
                    count = min(batch, target - first)
                    pending[executor.submit(_put_seed, node, first, count, body_bytes)] = count
                    metadata["submitted"] += count
                done, _ = wait(pending, timeout=.2, return_when=FIRST_COMPLETED)
                for future in done:
                    count = pending.pop(future)
                    try:
                        metadata["records"] += future.result()
                    except Exception as error:
                        metadata["failed"] += count
                        stopped = True
                        message = f"{type(error).__name__}: {str(error)[:400]}"
                        if len(metadata["errors"]) < ERROR_LIMIT:
                            metadata["errors"].append(message)
                            telemetry.note_error(message)
                snapshot = _seed_progress(metadata, telemetry, len(pending), started)
                if time.monotonic() >= next_progress:
                    print(f"sustained seed: {snapshot}", flush=True)
                    next_progress += 60
        result, _, _ = node.call("/api/db/flush", {})
        if result.get("durable") is not True:
            raise RuntimeError("The generated seed lacks a durable flush")
        metadata["complete"] = metadata["records"] == target and not metadata["failed"]
        metadata["canary_last_ack"] = validate_canary(node.call("/api/query", canary_query())[0])
    except Exception as error:
        metadata["complete"] = False
        message = f"{type(error).__name__}: {str(error)[:400]}"
        metadata["errors"].append(message)
        telemetry.note_error(message)
    metadata["measurement_failed"] = bool(metadata["errors"])
    metadata["payload_bytes"] = metadata["records"] * body_bytes
    metadata["seconds"] = time.monotonic() - started
    if not metadata["complete"]:
        metadata["reason"] = "seed API failure" if metadata["errors"] else "seed deadline reached"
    _seed_progress(metadata, telemetry, 0, started)
    print(f"sustained seed complete: records={metadata['records']}/{target}, "
          f"complete={metadata['complete']}, seconds={metadata['seconds']:.1f}", flush=True)
    return metadata
