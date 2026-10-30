"""Real public app operations for compound alternate keys and independent relations."""

from datetime import datetime, timezone

from node_ops import metadata, molecule, protein, require, write


def definition():
    fields = ["entry_id", "board_id", "lane", "position", "updated_at", "content"]
    return {"name": "OrderedEntry", "schema_type": "Hash", "key": {"hash_field": "entry_id"},
            "independent_fields": True, "fields": fields,
            "field_types": {field: "String" for field in fields},
            "field_descriptions": {field: "Compound key operational entry " + field for field in fields},
            "alternate_keys": {
                "by_board": {"hash_field": "board_id", "range_separator": "#", "range_parts": [
                    {"field": "lane"}, {"field": "position", "format": {"decimal": {"width": 8}}},
                    {"field": "entry_id"}]},
                "recent": {"hash_literal": "all", "range_separator": "#", "range_parts": [
                    {"field": "updated_at", "format": "reverse_time_millis"}, {"field": "entry_id"}]}}}


def page(node, runtime, name, predicate, limit=17, cursor=None):
    body = {"schema_name": runtime, "key_name": name, "fields": ["entry_id", "lane", "position", "updated_at"],
            "filter": predicate, "limit": limit}
    if cursor:
        body["cursor"] = cursor
    return node.call("/api/query", body)[0]


def collect(node, runtime, name, predicate):
    rows, cursor, seen = [], None, set()
    for _ in range(64):
        reply = page(node, runtime, name, predicate, cursor=cursor)
        rows.extend(reply["results"])
        if not reply["has_more"]:
            require(not reply.get("next_cursor"), "terminal alternate page retained a cursor")
            return rows
        cursor = reply["next_cursor"]
        token = repr(cursor)
        require(cursor and token not in seen, "compound alternate cursor did not advance")
        seen.add(token)
    raise RuntimeError("compound alternate exceeded its page bound")


def compound_declarations(node, publisher, port):
    body = {"app_id": "compound-app", "schema": definition()}
    publisher.publish(port, [body])
    reply = node.call("/api/apps/declare-schema", body)[0]
    runtime = reply["canonical"]
    model = {f"entry-{i:04}": {"entry_id": f"entry-{i:04}", "board_id": "board-a",
             "lane": "todo" if i % 2 else "doing", "position": str(i % 13),
             "updated_at": datetime.fromtimestamp(1700000000 + i, timezone.utc).isoformat(),
             "content": "x" * 4096} for i in range(320)}
    records = list(model.items())
    for offset in range(0, len(records), 32):
        node.call("/api/mutations/batch", [{"type": "mutation", "schema": runtime,
                  "mutation_type": "create", "key_value": {"hash": key}, "fields_and_values": fields,
                  "durability": "durable", "convergence": "sync"}
                  for key, fields in records[offset:offset + 32]])
    expected = sorted(model, key=lambda key: (model[key]["lane"], f'{int(model[key]["position"]):08}', key))
    rows = collect(node, runtime, "by_board", {"HashKey": "board-a"})
    require([row["key"]["hash"] for row in rows] == expected, "compound board order or membership differs")
    for row in rows:
        key = row["key"]["hash"]
        fields = model[key]
        require(row["matched_key"] == {"hash": "board-a", "range": f'{fields["lane"]}#{int(fields["position"]):08}#{key}'},
                "compound key lost its legacy order or primary mutation identity")
        require("content" not in row["fields"], "compound summary fetched the body")
    recent = collect(node, runtime, "recent", {"HashKey": "all"})
    require([row["key"]["hash"] for row in recent] == list(reversed(model)), "recent order differs")
    prefix = collect(node, runtime, "by_board", {"HashRangePrefix": {"hash": "board-a", "prefix": "todo#"}})
    require([row["key"]["hash"] for row in prefix] == [key for key in expected if model[key]["lane"] == "todo"],
            "compound prefix differs")
    write(node, runtime, {"hash": "entry-0001"}, {"board_id": "board-b", "position": "2"}, "update")
    write(node, runtime, {"hash": "entry-0002"}, {}, "delete")
    expected = [key for key in expected if key not in ("entry-0001", "entry-0002")]
    require([row["key"]["hash"] for row in collect(node, runtime, "by_board", {"HashKey": "board-a"})] == expected,
            "move or delete retained an old alternate entry")
    moved = collect(node, runtime, "by_board", {"HashKey": "board-b"})
    require(len(moved) == 1 and moved[0]["key"] == {"hash": "entry-0001"}, "move lost primary identity")
    try:
        write(node, runtime, {"hash": "entry-0001"}, {"lane": "bad#lane"}, "update")
    except RuntimeError as error:
        require("HTTP 400" in str(error), "reserved separator received a different refusal")
    else:
        raise RuntimeError("ambiguous compound key was accepted")
    node.stop()
    node.start()
    require([row["key"]["hash"] for row in collect(node, runtime, "by_board", {"HashKey": "board-a"})] == expected,
            "compound entries changed after reopen")
    require(protein(node, molecule(node, runtime, "content")) is None, "compound field joined a protein")
    independent_relation(node, publisher, port)
    return {"records": 320, "order": True, "membership": True, "primary_keys": True,
            "prefix": True, "move": True, "delete": True, "reopen": True, "independent_relations": True}


def independent_relation(node, publisher, port):
    source = definition()
    fields = ["board_id", "entry_id", "content"]
    relation = {"name": "RelatedEntry", "schema_type": "HashRange", "independent_fields": True,
                "key": {"hash_field": "board_id", "range_field": "entry_id"}, "fields": fields,
                "field_types": {field: source["field_types"][field] for field in fields},
                "field_descriptions": {field: source["field_descriptions"][field] for field in fields}}
    body = {"app_id": "compound-app", "schema": relation}
    publisher.publish(port, [body])
    runtime = node.call("/api/apps/declare-schema", body)[0]["canonical"]
    write(node, runtime, {"hash": "board-b", "range": "entry-0001"},
          {"board_id": "board-b", "entry_id": "entry-0001", "content": "relation-only"})
    source_runtime = "compound-app/OrderedEntry"
    require(metadata(node, runtime)["field_hashes"]["content"] == metadata(node, source_runtime)["field_hashes"]["content"],
            "relation lost canonical field reuse")
    original = node.call("/api/query", {"schema_name": source_runtime, "fields": ["content"],
                         "filter": {"HashKey": "entry-0001"}})[0]["results"]
    require(len(original) == 1 and original[0]["fields"]["content"] == "x" * 4096,
            "independent relation changed source values")
