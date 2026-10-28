"""Owner-socket blob and record operations on one fresh generated CI node."""

import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import time

from node import UnixConnection

COUNT, FILE_BYTES, SCHEMA = 256, 8192, "CasInterruptionLifecycle"


def require(value, message):
    if not value:
        raise RuntimeError(message)


def parallel(function, items):
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(function, items))


def upload(node, number):
    payload = hashlib.shake_256(f"generated CAS interruption file {number}".encode()).digest(FILE_BYTES)
    result = node.call("/api/db/put-blob-local", {"bytes_b64": base64.b64encode(payload).decode()})[0]["file_blob"]
    require(result["bytes"] == FILE_BYTES, "local blob upload size changed")
    return {"pointer": result["pointer"], "sha256": hashlib.sha256(payload).hexdigest()}


def mutate(node, pointers, operation):
    rows = []
    for number, pointer in enumerate(pointers):
        key = f"file-{number:04d}"
        fields = {"id": key}
        if operation == "create":
            fields.update(note="shared immutable value", file=pointer["pointer"])
        rows.append({"type": "mutation", "schema": SCHEMA, "mutation_type": operation,
                     "durability": "durable", "key_value": {"hash": key}, "fields_and_values": fields})
    reply = node.call("/api/mutations/batch", {"mutations": rows})[0]
    require(len(reply["mutation_ids"]) == len(rows), "real mutation batch did not acknowledge every key")


def seed(node):
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "key": {"hash_field": "id"}, "fields": ["id", "note", "file"], "field_types": {"id": "String"},
        "field_descriptions": {field: SCHEMA + " " + field for field in ("id", "note", "file")}}})
    files = parallel(lambda number: upload(node, number), range(COUNT + 1))
    # Two remaining keys share one pointer and one immutable field value.
    pointers = [*files, files[-1]]
    mutate(node, pointers, "create")
    records(node, pointers, False)
    mutate(node, pointers[:COUNT], "purge")
    records(node, pointers, True)
    return pointers


def records(node, pointers, deleted):
    queries = [{"schema_name": SCHEMA, "fields": ["id", "note", "file"],
                "filter": {"HashKey": f"file-{number:04d}"}, "limit": 2}
               for number in range(len(pointers))]
    batches = [queries[start:start + 32] for start in range(0, len(queries), 32)]
    groups = parallel(lambda batch: node.call("/api/queries/batch", {"queries": batch})[0]["results"], batches)
    replies = [reply for group in groups for reply in group]
    require(len(replies) == len(pointers), "keyed read batch was incomplete")
    for number, (reply, pointer) in enumerate(zip(replies, pointers)):
        value = reply["response"]
        require(reply["status"] == 200 and value["ok"] is True
                and value.get("unresolved_rows", 0) == 0 and not value.get("has_more", False),
                "keyed read failed or returned incomplete data")
        present = not deleted or number >= COUNT
        rows = value["results"]
        require(value["returned_count"] == int(present) and len(rows) == int(present),
                "record presence differs from the durable purge")
        if present:
            key = f"file-{number:04d}"
            require(rows[0]["key"] == {"hash": key, "range": None}
                    and rows[0]["fields"] == {"id": key, "note": "shared immutable value", "file": pointer["pointer"]},
                    "live shared record changed")


def blob(node, file):
    connection = UnixConnection(node.socket)
    try:
        raw = json.dumps({"pointer": file["pointer"]}).encode()
        connection.request("POST", "/api/db/fetch-file-blob", raw,
                           {"Content-Type": "application/json", "Accept": "application/octet-stream",
                            "X-LastDB-Client": "cas-interruption-ci"})
        response = connection.getresponse()
        body = response.read(FILE_BYTES + 1)
        if response.status == 404:
            # The ordinary owner-socket error renderer returns a plain body.
            # Require its exact message, rather than accept any unrelated 404.
            require(body == b"file blob was not found locally", "unrelated file refusal")
            return False
        require(response.status == 200 and len(body) == FILE_BYTES
                and hashlib.sha256(body).hexdigest() == file["sha256"], "live file bytes changed")
        return True
    finally:
        connection.close()


def files_after_recovery(node, pointers):
    remaining = list(pointers[:COUNT])
    deadline = time.monotonic() + 180
    while remaining:
        present = parallel(lambda file: blob(node, file), remaining)
        remaining = [file for file, exists in zip(remaining, present) if exists]
        require(time.monotonic() < deadline, "deleted file blobs remain after recovery and dwell")
        if remaining:
            time.sleep(1)
    require(blob(node, pointers[-1]), "shared live file disappeared")


def counters(node):
    cursor, identity = None, None
    for _step in range(64):
        report = node.call("/api/db/verify-refs", {"cursor": cursor, "max_pages": 64, "max_ms": 30000})[0]["verify_refs"]
        require(identity in (None, report["run_id"]), "counter verifier identity changed")
        identity = report["run_id"]
        if report["done"]:
            require(report["next_cursor"] is None and report["tally_complete"] is True
                    and report["mismatches_total"] == 0 and report["suspect_atoms"] == 0
                    and report["mismatches"]["unconfirmed_count_mismatch"] == 0,
                    "counter verification did not prove exact references")
            return {"complete": True, "faults": 0, "tally_complete": True}
        require(report["next_cursor"] and report["next_cursor"] != cursor, "counter verifier cursor did not advance")
        cursor = report["next_cursor"]
    raise RuntimeError("counter verification exceeded the operation bound")
