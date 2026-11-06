"""Owner-socket record file operations on one fresh generated CI node."""

import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import sys
from pathlib import Path

from node import UnixConnection
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "operations"))
from record_requests import mutation, conditioned, read_records, coordinate

COUNT, FILE_BYTES, SCHEMA, NOTE = 256, 8192, "CasInterruptionLifecycle", "generated record with one file"
FIELDS = ("id", "note", "detail")
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
        "mutation": mutation(SCHEMA, {"hash": key(number)},
            {"id": key(number), "note": NOTE, "detail": "initial"}, "create", expected={"type": "absent"}),
        "bytes_b64": base64.b64encode(data).decode()})[0]
    sha256 = hashlib.sha256(data).hexdigest()
    require(reply["file"]["size_bytes"] == FILE_BYTES and reply["file"]["sha256"] == sha256,
            "put-blob marker differs from the bytes")
    return sha256


def replace_bytes(node, number):
    fields = {"id": key(number), "note": NOTE, "detail": "initial"}
    request = replacement(node, number, fields, retain_file=False)
    data = hashlib.shake_256(b"generated replacement file").digest(FILE_BYTES)
    reply = node.call("/api/db/put-blob", {
        "mutation": request, "bytes_b64": base64.b64encode(data).decode()})[0]
    digest = hashlib.sha256(data).hexdigest()
    row = read_records(node, [request])[coordinate(SCHEMA, request["key_value"])]
    require(reply["file"]["sha256"] == digest
            and row["revision"] != request["expected"]["revision"],
            "put-blob replacement did not publish the new file version")
    return digest


def seed(node):
    """Put COUNT + 2 records, each with one file. Return the expected state."""
    prove_required_file(node)
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}}, "fields": list(FIELDS), "field_types": {"id": "String"},
        "field_descriptions": {field: SCHEMA + " " + field for field in FIELDS}}})
    expected = parallel(lambda number: put(node, number), range(COUNT + 2))
    expected[0] = replace_bytes(node, 0)
    # Each ordinary replacement supplies the complete fields and retained marker.
    # A second replacement is conditional on the first replacement's revision.
    for fields in ({"id": key(COUNT), "note": NOTE + " updated", "detail": "initial"},
                   {"id": key(COUNT), "note": NOTE + " updated", "detail": "updated"}):
        request = replacement(node, COUNT, fields, retain_file=True)
        reply = node.call("/api/mutation", request)[0]
        require(reply["record_revision"], "file-bearing replacement has no revision")
    records(node, expected)
    files(node, expected)
    print("Complete record replacements retained the selected file without new payload bytes", flush=True)
    return expected


def prove_required_file(node):
    schema, target = "RequiredRecordFile", {"hash": "required-file"}
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": schema, "descriptive_name": schema,
        "schema_type": "Hash", "file_required": True,
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": ["note"], "field_types": {"note": "String"},
        "field_descriptions": {"note": "The note that requires its original file"},
    }})
    fields = {"note": "file content is part of this record"}
    request = mutation(schema, target, fields, "create", expected={"type": "absent"})
    require_file_refusal(node, "/api/mutation", request)
    stored = node.call("/api/db/put-blob", {
        "mutation": request, "bytes_b64": base64.b64encode(payload(0)).decode(),
    })[0]
    update = conditioned(node, [mutation(schema, target, fields)])[0]
    update.pop("file", None)
    node.call("/api/mutation", update)
    clear = conditioned(node, [mutation(schema, target, fields)])[0]
    clear.pop("file", None)
    require_file_refusal(node, "/api/db/delete-blob", {"mutation": clear})
    after = node.call("/api/db/fetch-blob", {"schema": schema, "key": target})[0]
    require(after["file"]["sha256"] == stored["file"]["sha256"],
            "the required file changed or disappeared during a record replacement")
    delete = conditioned(node, [mutation(schema, target, {}, "delete")])[0]
    node.call("/api/mutation", delete)
    print("Required file admission and automatic preservation passed", flush=True)


def require_file_refusal(node, route, request):
    try:
        node.call(route, request)
    except RuntimeError as error:
        require(getattr(error, "status", None) == 400 and "this record requires a file" in str(error),
                "the required-file operation failed for an unrelated reason")
        return
    raise RuntimeError("a live required-file record was accepted without its file")


def purge(node, numbers):
    rows = conditioned(node, [mutation(SCHEMA, {"hash": key(number)}, {}, "purge")
                              for number in numbers])
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
    queries = [{"schema_name": SCHEMA,
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
            fields = {"id": key(number), "note": NOTE, "detail": "initial"}
            if number == COUNT:
                fields.update(note=NOTE + " updated", detail="updated")
            require(rows[0]["key"] == {"hash": key(number), "range": None}
                    and rows[0]["fields"] == fields
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


def replacement(node, number, fields, retain_file):
    request = mutation(SCHEMA, {"hash": key(number)}, fields)
    row = read_records(node, [request])[coordinate(SCHEMA, request["key_value"])]
    request["expected"] = {"type": "revision", "revision": row["revision"]}
    metadata = row.get("metadata") or {}
    request["metadata"] = metadata.get("metadata")
    request["source_file_name"] = metadata.get("source_file_name")
    request["file"] = metadata.get("file") if retain_file else None
    return request


def delete_file(node, number):
    request = replacement(node, number,
        {"id": key(number), "note": NOTE + " updated", "detail": "updated"}, retain_file=False)
    reply = node.call("/api/db/delete-blob", {"mutation": request})[0]
    require(reply["record_revision"], "file removal has no conditional replacement revision")
