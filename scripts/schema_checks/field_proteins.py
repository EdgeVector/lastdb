"""Real-node field membership, shared atoms, and current-value preservation."""

from node_ops import (install, metadata, molecule, protein, read, require, restart,
                      same_atom, schema, write)


def field_identity_membership(node):
    # Only payload matches. Other fields differ, and no declaration bypass can
    # admit the pair: the old overlap is 1/5, below its 0.6 threshold.
    fields = ["record_id", "group_id", "payload", "left_only", "left_extra"]
    left = schema("FieldLeft", "left meaning", declared=False, fields=fields)
    right = schema("FieldRight", "right meaning", declared=False,
                   fields=["record_id", "group_id", "payload", "right_only", "right_extra"])
    for definition in (left, right):
        definition["field_descriptions"]["payload"] = "one shared payload field"
    left, right = install(node, left), install(node, right)
    first, second = molecule(node, left), molecule(node, right)
    require(first != second, "same-key peers unexpectedly share their molecule")
    shared = protein(node, first)
    require(shared is not None and shared == protein(node, second), "equal fields lack one protein")
    require(protein(node, molecule(node, left, "record_id"))
            != protein(node, molecule(node, right, "record_id")), "distinct fields share a protein")
    key = {"hash": "field-key"}
    values = {"record_id": "field-key", "group_id": "left-group", "payload": "shared-first"}
    write(node, left, key, values)
    first_record = read(node, left, key, values)
    peer = read(node, right, key, {"payload": values["payload"]}, names=["payload"])
    same_atom(first_record, peer)
    write(node, right, key, {"payload": "shared-second"}, "update")
    values["payload"] = "shared-second"
    same_atom(read(node, left, key, values),
              read(node, right, key, {"payload": values["payload"]}, names=["payload"]))
    restart(node)
    require(protein(node, first) == shared and protein(node, second) == shared,
            "reopen changed equal-field membership")
    read(node, left, key, values)
    read(node, right, key, {"payload": values["payload"]}, names=["payload"])
    return {"same_key_layout": True, "shared_fields": 1, "smaller_schema_fields": 5,
            "declared": False, "protein": shared, "separate_molecules": [first, second],
            "shared_atom_and_values_after_reopen": True}


def independent_field_joins(node):
    original = install(node, schema("FieldOriginal", "field joins"))
    third_definition = schema("FieldThird", "different shape", cross=True)
    original_definition = metadata(node, original)
    # This declaration reuses exactly one field, with a different local name.
    third_definition["fields"][2] = "renamed_payload"
    for mapping in ("field_types", "field_descriptions", "field_versions", "field_declarations", "field_hashes"):
        third_definition[mapping]["renamed_payload"] = third_definition[mapping].pop("payload")
    third_definition["field_hashes"]["renamed_payload"] = original_definition["field_hashes"]["payload"]
    third = install(node, third_definition)
    field_id = protein(node, molecule(node, original))
    require(protein(node, molecule(node, third, "renamed_payload")) == field_id,
            "renamed equal field did not join the existing protein")
    # Supply the peer key fields explicitly. Only the equal field propagates.
    values = {"record_id": "renamed-key", "group_id": "renamed-group", "payload": "renamed value"}
    write(node, original, {"hash": "renamed-key"}, values)
    record = read(node, third, {"hash": "renamed-group", "range": "renamed-key"},
                  {"renamed_payload": "renamed value"}, names=["renamed_payload"])
    source = read(node, original, {"hash": "renamed-key"}, values)
    require(source["metadata"]["payload"]["atom_uuid"]
            == record["metadata"]["renamed_payload"]["atom_uuid"], "renamed field copied its atom")
    return {"one_shared_declared_identity": True, "local_rename": True, "protein": field_id}
