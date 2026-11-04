"""Owner-socket record file operations on one fresh generated CI node."""

import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json

from node import UnixConnection

COUNT, FILE_BYTES, SCHEMA, NOTE = 256, 8192, "CasInterruptionLifecycle", "generated record with one file"
# Each purge batch is one native write, so one CAS deletion window.
PURGE_BATCH = 4


def require(value, message):
    if not value:
        raise RuntimeError(message)


def parallel(function, items):
    with ThreadPoolExecutor(max_workers=4) as pool:
        return list(pool.map(function, items))


def key(number):
    return f"file-{number:04d}"


def payload(number):
    # The last two records hold equal bytes, each at its own path.
    return hashlib.shake_256(f"generated CAS interruption file {min(number, COUNT)}".encode()).digest(FILE_BYTES)


def put(node, number):
    data = payload(number)
    reply = node.call("/api/db/put-blob", {
        "schema": SCHEMA, "key": {"hash": key(number)}, "bytes_b64": base64.b64encode(data).decode(),
        "fields": {"id": key(number), "note": NOTE}})[0]
    sha256 = hashlib.sha256(data).hexdigest()
    require(reply["file"]["size_bytes"] == FILE_BYTES and reply["file"]["sha256"] == sha256,
            "put-blob marker differs from the bytes")
    return sha256


def seed(node):
    """Put COUNT + 2 records, each with one file. Return the expected state."""
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "key": {"hash_field": "id"}, "fields": ["id", "note"], "field_types": {"id": "String"},
        "field_descriptions": {field: SCHEMA + " " + field for field in ("id", "note")}}})
    expected = parallel(lambda number: put(node, number), range(COUNT + 2))
    # Ordinary field updates carry no file payload. Repeated-key groups must
    # reuse the current file marker and leave its bytes at the same path.
    updates = [{"type": "mutation", "schema": SCHEMA, "mutation_type": "update", "durability": "durable",
                "key_value": {"hash": key(COUNT)}, "fields_and_values": {"note": note}}
               for note in (NOTE + " updated", NOTE)]
    reply = node.call("/api/mutations/batch", {"mutations": updates})[0]
    require(len(reply["mutation_ids"]) == len(updates), "file-bearing record updates were not acknowledged")
    records(node, expected)
    files(node, expected)
    print("File metadata reuse: ordinary record updates kept the file contents", flush=True)
    return expected


def purge(node, numbers):
    rows = [{"type": "mutation", "schema": SCHEMA, "mutation_type": "purge", "durability": "durable",
             "key_value": {"hash": key(number)}, "fields_and_values": {"id": key(number)}}
            for number in numbers]
    reply = node.call("/api/mutations/batch", {"mutations": rows})[0]
    require(len(reply["mutation_ids"]) == len(rows), "real mutation batch did not acknowledge every key")


def purge_all(node, stop):
    """Purge the first COUNT records in small batches until done or stopped."""
    for start in range(0, COUNT, PURGE_BATCH):
        if stop.is_set():
            return
        try:
            purge(node, range(start, start + PURGE_BATCH))
        except Exception:
            # The deliberate stop breaks the request in flight.
            if stop.is_set():
                return
            raise


def read(node, count):
    queries = [{"schema_name": SCHEMA, "fields": ["id", "note"],
                "filter": {"HashKey": key(number)}, "limit": 2}
               for number in range(count)]
    batches = [queries[start:start + 32] for start in range(0, len(queries), 32)]
    groups = parallel(lambda batch: node.call("/api/queries/batch", {"queries": batch})[0]["results"], batches)
    replies = [reply for group in groups for reply in group]
    require(len(replies) == count, "keyed read batch was incomplete")
    rows = []
    for reply in replies:
        value = reply["response"]
        require(reply["status"] == 200 and value["ok"] is True
                and not value.get("has_more", False),
                "keyed read failed or returned incomplete data")
        require(value["returned_count"] == len(value["results"]) <= 1, "one key returned more than one record")
        rows.append(value["results"])
    return rows


def finish_purge(node):
    """After the cut, purge the records that the stopped run did not reach."""
    left = [number for number, rows in enumerate(read(node, COUNT)) if rows]
    for start in range(0, len(left), 64):
        purge(node, left[start:start + 64])


def records(node, expected):
    """`expected[n]` is None for an absent record, "" for a record with no
    file, or the SHA-256 of the record's file."""
    for number, (rows, want) in enumerate(zip(read(node, len(expected)), expected)):
        require(len(rows) == int(want is not None), "record presence differs from the durable writes")
        if want is not None:
            marker = (rows[0].get("metadata") or {}).get("file") or {}
            require(rows[0]["key"] == {"hash": key(number), "range": None}
                    and rows[0]["fields"] == {"id": key(number), "note": NOTE}
                    and marker.get("sha256", "") == want,
                    "live record or its file marker changed")


def fetch(node, number):
    """The file of one record, or None when the record has no file."""
    connection = UnixConnection(node.socket)
    try:
        raw = json.dumps({"schema": SCHEMA, "key": {"hash": key(number)}}).encode()
        connection.request("POST", "/api/db/fetch-blob", raw,
                           {"Content-Type": "application/json", "Accept": "application/octet-stream",
                            "X-LastDB-Client": "cas-interruption-ci"})
        response = connection.getresponse()
        body = response.read(FILE_BYTES + 1)
        if response.status == 404:
            # Require the route's own message rather than accept any 404.
            require(b"the record has no file" in body, "unrelated file refusal")
            return None
        require(response.status == 200, f"fetch-blob failed: HTTP {response.status}")
        return body
    finally:
        connection.close()


def files(node, expected):
    """Every record with a file fetches its exact bytes; no other record has one."""
    bodies = parallel(lambda number: fetch(node, number), range(len(expected)))
    for body, want in zip(bodies, expected):
        if want:
            require(body is not None and len(body) == FILE_BYTES
                    and hashlib.sha256(body).hexdigest() == want, "live file bytes changed")
        else:
            require(body is None, "a record without a file returned bytes")


def delete_file(node, number):
    reply = node.call("/api/db/delete-blob", {"schema": SCHEMA, "key": {"hash": key(number)}})[0]
    require(reply["deleted"] is True, "delete-blob did not remove the file")
