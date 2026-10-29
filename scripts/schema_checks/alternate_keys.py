"""Public alternate-key operations on a fresh generated CI home."""

from node_ops import install, metadata, molecule, protein, read, require, restart, same_atom, schema, write


def task_definition(name="AlternateTask"):
    definition = schema(name, "alternate task", fields=["record_id", "group_id", "owner_id", "payload"])
    definition["field_types"]["owner_id"] = {"OneOf": ["String", "Null"]}
    definition["alternate_keys"] = {
        "by_group": {"hash_field": "group_id", "range_field": "record_id", "missing": "reject"},
        "by_owner": {"hash_field": "owner_id", "range_field": "record_id", "missing": "omit"},
    }
    return definition


def query(node, name, key_name, hash_value, limit=100, cursor=None):
    body = {"schema_name": name, "key_name": key_name,
            "fields": ["record_id", "group_id", "owner_id", "payload"],
            "filter": {"HashKey": hash_value}, "limit": limit}
    if cursor is not None:
        body["cursor"] = cursor
    return node.call("/api/query", body)[0]


def expect_refusal(operation, status=400):
    try:
        operation()
    except RuntimeError as error:
        require(f"HTTP {status}" in str(error), "request refused with a different status")
    else:
        raise RuntimeError("invalid alternate request succeeded")


def alternate_key_lifecycle(node):
    name = install(node, task_definition())
    require(protein(node, molecule(node, name)) is None, "alternate fields joined a protein")
    expect_refusal(lambda: write(node, name, {"hash": "missing-group"}, {"payload": "invalid"}))
    read(node, name, {"hash": "missing-group"}, None)
    for key in ("t1", "t2"):
        write(node, name, {"hash": key},
              {"record_id": key, "group_id": "g1", "owner_id": "o1", "payload": key})
    install(node, task_definition())  # An identical declaration preserves existing entries.
    page = query(node, name, "by_group", "g1", limit=1)
    first = page["results"][0]
    require(first["key"]["hash"] == "t1", "alternate response lost the primary mutation key")
    require(first["matched_key"] == {"hash": "g1", "range": "t1"}, "wrong matched key")
    require(page["has_more"] and page["next_cursor"], "alternate page lost its cursor")
    second = query(node, name, "by_group", "g1", limit=1, cursor=page["next_cursor"])
    require([row["key"]["hash"] for row in second["results"]] == ["t2"]
            and not second["has_more"], "alternate continuation differs")
    projected = node.call("/api/query", {"schema_name": name, "key_name": "by_group",
                         "fields": ["payload"], "filter": {"HashKey": "g1"}})[0]
    require([row["fields"] for row in projected["results"]] == [{"payload": "t1"}, {"payload": "t2"}],
            "alternate dependency fields escaped the requested projection")
    write(node, name, {"hash": "sparse"}, {"record_id": "sparse", "group_id": "g3", "payload": "sparse"})
    sparse = node.call("/api/query", {"schema_name": name, "key_name": "by_group",
                      "fields": ["owner_id"], "filter": {"HashKey": "g3"}})[0]
    require(len(sparse["results"]) == 1 and sparse["results"][0]["fields"] == {},
            "a missing projected field removed a valid alternate record")
    primary = read(node, name, {"hash": "t1"},
                   {"record_id": "t1", "group_id": "g1", "owner_id": "o1", "payload": "t1"},
                   names=["record_id", "group_id", "owner_id", "payload"])
    same_atom(primary, first)
    write(node, name, {"hash": "t1"}, {"payload": "changed", "group_id": "g2"}, "update")
    require([row["key"]["hash"] for row in query(node, name, "by_group", "g1")["results"]] == ["t2"],
            "old alternate key remains")
    require(query(node, name, "by_group", "g2")["results"][0]["fields"]["payload"] == "changed",
            "new alternate key missed the accepted value")
    write(node, name, {"hash": "t1"}, {"owner_id": None}, "update")
    require([row["key"]["hash"] for row in query(node, name, "by_owner", "o1")["results"]] == ["t2"],
            "sparse alternate did not remove a null key")
    expect_refusal(lambda: write(node, name, {"hash": "t1"}, {"group_id": None}, "update"))
    require(len(query(node, name, "by_group", "g2")["results"]) == 1, "refused write changed membership")
    expect_refusal(lambda: query(node, name, "unknown", "g1"))
    expect_refusal(lambda: query(node, name, "by_owner", "o1", cursor=page["next_cursor"]))
    restart(node, crash=True)
    require(query(node, name, "by_group", "g2")["results"][0]["key"]["hash"] == "t1",
            "durable alternate entry disappeared on reopen")
    write(node, name, {"hash": "t1"}, operation="delete")
    require(not query(node, name, "by_group", "g2")["results"], "delete left an alternate entry")
    write(node, name, {"hash": "t2"}, operation="purge")
    require(not query(node, name, "by_group", "g1")["results"]
            and not query(node, name, "by_owner", "o1")["results"], "purge left an alternate entry")
    restart(node)
    require(not query(node, name, "by_group", "g1")["results"], "reopen restored a removed entry")
    return {"primary_and_two_alternates": True, "same_atom": True, "page_cursor": True,
            "key_move": True, "sparse_null": True, "durable_reopen": True, "delete_and_purge": True}


def alternate_field_reuse(node):
    definition = task_definition("SeparateTask")
    name = install(node, definition)
    peer_definition = task_definition("UnrelatedTask")
    peer_definition.pop("alternate_keys")
    peer = install(node, peer_definition)
    require(metadata(node, name)["field_hashes"] == metadata(node, peer)["field_hashes"],
            "field-reuse case lacks equal canonical identities")
    write(node, name, {"hash": "same"}, {"record_id": "same", "group_id": "g",
                                       "owner_id": "o", "payload": "task"})
    write(node, peer, {"hash": "same"}, {"record_id": "same", "group_id": "other",
                                       "owner_id": "other", "payload": "unrelated"})
    require(query(node, name, "by_group", "g")["results"][0]["fields"]["payload"] == "task",
            "canonical definition reuse changed another record")
    invalid = task_definition("InvalidAlternate")
    invalid["alternate_keys"]["by_group"]["hash_field"] = "undeclared"
    expect_refusal(lambda: install(node, invalid))
    return {"equal_canonical_fields": True, "independent_values": True,
            "unknown_declared_field_refused": True}

def alternate_atomic_batch(node):
    name = install(node, task_definition("AtomicTask"))
    operations = []
    for key, fields, operation in (
            ("a", {"record_id": "a", "group_id": "g0", "owner_id": "o", "payload": "first"}, "create"),
            ("a", {"group_id": "g2", "payload": "second"}, "update"),
            ("b", {"record_id": "b", "group_id": "g0", "owner_id": "o", "payload": "b"}, "create")):
        operations.append({"type": "mutation", "schema": name, "mutation_type": operation,
                           "key_value": {"hash": key}, "fields_and_values": fields,
                           "durability": "durable", "convergence": "sync"})
    node.call("/api/mutations/batch", operations)
    require([row["key"]["hash"] for row in query(node, name, "by_group", "g0")["results"]] == ["b"],
            "repeated-key batch retained an intermediate alternate")
    require(query(node, name, "by_group", "g2")["results"][0]["fields"]["payload"] == "second",
            "repeated-key batch missed its final value")
    refused = [
        {"type": "mutation", "schema": name, "mutation_type": "create",
         "key_value": {"hash": "refused"}, "fields_and_values": {
             "record_id": "refused", "group_id": "bad", "owner_id": "o", "payload": "bad"}},
        {"type": "mutation", "schema": name, "mutation_type": "update",
         "key_value": {"hash": "a"}, "fields_and_values": {"group_id": "bad"},
         "expected": {"type": "value", "field": "payload", "value": "wrong"}},
    ]
    expect_refusal(lambda: node.call("/api/mutations/batch", refused), status=409)
    read(node, name, {"hash": "refused"}, None)
    require(not query(node, name, "by_group", "bad")["results"], "CAS failure published an alternate")
    require(query(node, name, "by_group", "g2")["results"][0]["fields"]["payload"] == "second",
            "CAS failure changed a primary record")
    return {"repeated_key_order": True, "CAS_refusal": True, "whole_batch_rollback": True}

