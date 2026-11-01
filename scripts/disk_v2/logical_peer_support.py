"""Bounded owner requests and keyed observations on real generated peer nodes.

A format 7 home exports version 2 events. Each event lists the final record
effects of one request: the schema, the primary key, and each written field
with its canonical identity, value and stamp.
"""

import copy
import json

from logical_capture import UnixConnection, read, require, schema, write

PATH = "/api/sync/logical-v1"
EVENT_BYTES, HTTP_BYTES = 1024 * 1024, 1024 * 1024 + 8192
HASH_SCHEMA, RANGE_SCHEMA = "PeerHashOperation", "PeerRangeOperation"


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


def request(node, body, expected=(200,), path=PATH, declared_length=None):
    connection = UnixConnection(node.socket)
    headers = {"Content-Type": "application/json", "X-LastDB-Client": "logical-peer-ci"}
    if declared_length is not None:
        headers["Content-Length"] = str(declared_length)
    try:
        connection.request("POST", path, encoded(body) if body is not None else b"", headers)
        response = connection.getresponse()
        raw = response.read(HTTP_BYTES + 1)
        require(response.status in expected and len(raw) <= HTTP_BYTES,
                f"logical owner request returned HTTP {response.status}: {raw[:400]!r}")
        if response.status != 200:
            return {"http_status": response.status, "response_bytes": len(raw)}
        value = json.loads(raw)
        require(value.get("ok") is True, "logical owner response lacks success")
        return value
    finally:
        connection.close()


def export(node, after=0, limit=4):
    page = request(node, {"action": "export", "after": after, "limit": limit})
    events, anchor, head = page["events"], page["anchor"], page["position"]
    require(anchor["sequence"] == 0 and anchor["origin"] == head["origin"]
            and page["after"] == after and len(events) <= limit <= 4
            and len(encoded(events)) <= EVENT_BYTES, "export exceeds its declared bounds")
    for offset, event in enumerate(events, after + 1):
        require(event["sequence"] == offset and event["origin"] == head["origin"]
                and event["signature"] and event.get("version") == 2 and event.get("records")
                and not event.get("outcomes"), "original exported event is incomplete")
    return page


def position(node, origin):
    return request(node, {"action": "position", "origin": origin})["position"]


def own_head(node):
    status = node.call("/api/status")[0]["status"]["logical_capture"]
    require(status["enabled"] is True and status["position"], "peer capture is not enabled")
    require(status["encoded_backlog_bytes"] <= status["backlog_cap_bytes"], "capture backlog exceeds cap")
    return status["position"]


def ingest(node, events):
    before = own_head(node)
    receipt = request(node, {"action": "import", "events": events})
    require(own_head(node) == before, "peer import authored an outgoing echo event")
    require(position(node, events[0]["origin"]) == receipt["position"],
            "import receipt differs from durable peer position")
    return receipt


def item(source, number, ranged=True, size=96):
    coordinate = f"{source}-{number:04d}"
    key = {"hash": "peer", "range": coordinate} if ranged else {"hash": coordinate}
    fields = {"hash": "peer", "range": coordinate} if ranged else {"id": coordinate}
    return {"schema": RANGE_SCHEMA if ranged else HASH_SCHEMA, "key": key,
            "fields": {**fields, "body": coordinate + ":" + "p" * size}}


def author(node, source):
    for name, ranged in [(HASH_SCHEMA, False), (RANGE_SCHEMA, True)]:
        node.call("/api/schemas/seed-system", schema(name, ranged))
    hashed = [item(source, 0, False), item("shared", 0, False)]
    ranged = [item(source, 0), item("shared", 0)]
    for entry in [hashed[1], ranged[1]]:
        entry["fields"]["body"] = f"{source}-first-conflict"
    batches = [copy.deepcopy(hashed), copy.deepcopy(ranged)]
    write(node, hashed)
    write(node, ranged)
    updates = copy.deepcopy([hashed[1], ranged[1]])
    for entry in updates:
        entry["fields"]["body"] = f"{source}-final-conflict"
    write(node, updates, "update")
    return [*batches, updates]


def record_key(entry):
    return entry["schema"], entry["key"]["hash"], entry["key"].get("range")


def stamp_order(stamp):
    """The merge order of a field stamp: write time, logical counter, device, mutation."""
    if isinstance(stamp, dict):
        stamp = [stamp["written_at"], stamp.get("logical_counter", 0),
                 stamp.get("device_id", ""), stamp.get("mutation_id", "")]
    require(isinstance(stamp, list) and len(stamp) == 4, "event field stamp is incomplete")
    return tuple(stamp)


def add_model(model, batches, events):
    """Merge the signed fields of each event into the model by stamp, as a receiver does."""
    require(len(batches) == len(events), "authored batches and signed events differ")
    for batch, event in zip(batches, events):
        known = {record_key(entry): entry for entry in batch}
        for outcome in event["records"]:
            coordinate = (outcome["schema_name"], outcome["key"]["hash"], outcome["key"].get("range"))
            require(coordinate in known, "event changed an original authored key")
            require(not outcome.get("deleted"), "event deleted an authored record")
            for field, written in outcome["fields"].items():
                authored = known[coordinate]["fields"]
                require(field in authored, "event changed an original authored field")
                require(written.get("value") == authored[field], "event value differs from the authored value")
                order = stamp_order(written["stamp"])
                selected = model.setdefault(coordinate, {})
                if field not in selected or order > selected[field][0]:
                    selected[field] = (order, authored[field])


def records(model):
    return [{"schema": key[0], "key": {"hash": key[1], **({"range": key[2]} if key[2] is not None else {})},
             "fields": {field: value[1] for field, value in fields.items()}}
            for key, fields in model.items()]


def check_values(node, model):
    wanted = records(model)
    read(node, wanted)
    expected = sorted([entry for entry in wanted if entry["schema"] == RANGE_SCHEMA],
                      key=lambda entry: entry["key"]["range"])
    request_body = {"schema_name": RANGE_SCHEMA, "fields": ["hash", "range", "body"], "limit": 5,
                    "filter": {"HashRangePrefix": {"hash": "peer", "prefix": ""}}}
    found, previous = [], None
    for _ in range(len(expected) // 5 + 2):
        value = node.call("/api/query", request_body)[0]
        require(value.get("ok") is True and value.get("unresolved_rows", 0) == 0,
                "received range has unresolved data")
        found.extend(value["results"])
        if value.get("has_more") is False:
            break
        cursor = value.get("next_cursor")
        require(value["results"] and cursor and cursor != previous, "received range cursor did not advance")
        request_body = {**request_body, "cursor": cursor}
        previous = cursor
    require(len(found) == len(expected), "received range omitted or repeated a key")
    for row, entry in zip(found, expected):
        require(row["key"] == entry["key"] and row["fields"] == entry["fields"],
                "received range changed the signed original-order result")
    return {"keyed_records": len(wanted), "range_records": len(found)}


def refusals(node, source_page):
    events, anchor = source_page["events"], source_page["anchor"]
    before = own_head(node)
    results = {"unknown_peer": request(node, {"action": "import", "events": events[:1]}, (409,))}
    request(node, {"action": "enroll", "anchor": anchor})
    results["gap"] = request(node, {"action": "import", "events": events[1:2]}, (409,))
    changed = copy.deepcopy(events[:1])
    field = next(iter(changed[0]["records"][0]["fields"].values()))
    field["value"] = "tampered-original-event"
    results["tamper"] = request(node, {"action": "import", "events": changed}, (409,))
    signature = copy.deepcopy(events[:1])
    signature[0]["signature"] = "00" * 64
    results["signature_only"] = request(node, {"action": "import", "events": signature}, (409,))
    mismatch = {**anchor, "sha256": "00" * 32}
    results["changed_trust_anchor"] = request(node, {"action": "enroll", "anchor": mismatch}, (409,))
    local_anchor = export(node)["anchor"]
    results["self_enrollment"] = request(node, {"action": "enroll", "anchor": local_anchor}, (409,))
    results["empty"] = request(node, {"action": "import", "events": []}, (400, 409))
    results["event_count"] = request(node, {"action": "import", "events": events[:1] * 5}, (400, 409))
    results["export_limit"] = request(node, {"action": "export", "after": 0, "limit": 5}, (409,))
    results["early_byte_cap"] = request(node, None, (413,), path=PATH + "?bounded=yes#fragment",
                                         declared_length=HTTP_BYTES + 1)
    require(position(node, anchor["origin"]) == anchor and own_head(node) == before,
            "refused signed event changed the cursor or local head")
    return results
