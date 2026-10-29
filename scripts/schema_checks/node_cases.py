"""Real keyed-read assertions; setup uses only the isolated node catalog route."""

from concurrent.futures import ThreadPoolExecutor

from node_ops import (FIELDS, file_hash, http_status, install, metadata, molecule, protein,
                      read, require, restart, same_atom, schema, write)


def linked_lifecycle(node):
    by_id = install(node, schema("CanonicalById", "shared"))
    by_group = install(node, schema("CanonicalByGroup", "shared", cross=True))
    mapped = schema("CanonicalMapped", "shared")
    mapped["field_mappers"] = {name: by_id + "." + name for name in FIELDS}
    alias = install(node, mapped)
    first, second, third = (molecule(node, name) for name in (by_id, by_group, alias))
    require(first == third and first != second, "same-key mapper/cross-key molecule identity differs")
    identifier = protein(node, first)
    require(identifier is not None and protein(node, second) == identifier, "cross-key molecules lack one protein")
    membership = node.call("/api/protein/" + identifier)[0]["members"]
    require({first, second}.issubset({member["molecule_uuid"] for member in membership}),
            "protein membership lacks a molecule back-reference")
    key, alternate = {"hash": "r1"}, {"hash": "g1", "range": "r1"}
    values = {"record_id": "r1", "group_id": "g1", "payload": "first"}
    write(node, by_id, key, values)
    same_atom(read(node, by_id, key, values), read(node, by_group, alternate, values),
              read(node, alias, key, values))
    write(node, by_group, alternate, {"payload": "updated"}, "update")
    values["payload"] = "updated"
    same_atom(read(node, by_id, key, values), read(node, by_group, alternate, values))
    write(node, by_id, key, {"group_id": "g2"}, "update")
    values["group_id"] = "g2"
    read(node, by_group, alternate, None)
    alternate["hash"] = "g2"
    same_atom(read(node, by_id, key, values), read(node, by_group, alternate, values))
    write(node, alias, key, operation="delete")
    read(node, by_id, key, None)
    read(node, by_group, alternate, None)
    durable = {"record_id": "r2", "group_id": "g3", "payload": "durable-before-kill"}
    write(node, by_id, {"hash": "r2"}, durable)
    restart(node, crash=True)
    after_crash = same_atom(read(node, by_id, {"hash": "r2"}, durable),
                            read(node, by_group, {"hash": "g3", "range": "r2"}, durable))
    read(node, by_id, key, None)
    read(node, by_group, alternate, None)
    restart(node)
    require(protein(node, first) == identifier and protein(node, second) == identifier,
            "reopen changed protein back-references")
    require(same_atom(read(node, by_id, {"hash": "r2"}, durable),
                      read(node, alias, {"hash": "r2"}, durable)) == after_crash,
            "clean reopen changed the current atom")
    return {"same_key_molecule": first, "cross_key_molecule": second, "protein": identifier,
            "durable_atom_after_crash_and_clean_reopen": after_crash,
            "actual_operations": ["create", "sibling_update", "rekey", "delete", "SIGKILL", "reopen"]}


def separation(node, declared):
    label = "Declarations" if declared else "Meanings"
    left = install(node, schema(label + "Left", label + " left", declared=declared))
    right = install(node, schema(label + "Right", label + " right", cross=True, declared=declared))
    first, second = molecule(node, left), molecule(node, right)
    require(first != second, "distinct declarations or meanings share a molecule")
    require(protein(node, first) != protein(node, second),
            "same-name distinct declarations or meanings unexpectedly bind")
    values = {"record_id": "isolated", "group_id": "bucket", "payload": "left meaning"}
    write(node, left, {"hash": "isolated"}, values)
    read(node, right, {"hash": "bucket", "range": "isolated"}, None)
    other = dict(values, payload="right meaning")
    write(node, right, {"hash": "bucket", "range": "isolated"}, other)
    read(node, left, {"hash": "isolated"}, values)
    read(node, right, {"hash": "bucket", "range": "isolated"}, other)
    restart(node)
    read(node, left, {"hash": "isolated"}, values)
    read(node, right, {"hash": "bucket", "range": "isolated"}, other)
    return {"declared": declared, "molecules": [first, second], "keyed_reads_after_reopen": True}


def concurrent_members(node):
    original = install(node, schema("RaceOriginal", "node race"))
    sibling = install(node, schema("RaceSibling", "node race", cross=True))
    key, alternate = {"hash": "race"}, {"hash": "race-group", "range": "race"}
    values = {"record_id": "race", "group_id": "race-group", "payload": "initial"}
    write(node, original, key, values)
    offered = [f"writer-{number}" for number in range(16)]

    def submit(number):
        target, coordinates = (original, key) if number % 2 == 0 else (sibling, alternate)
        return write(node, target, coordinates, {"payload": offered[number]}, "update")

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(submit, range(len(offered))))
    result = node.call("/api/query", {"schema_name": original, "fields": FIELDS,
                                     "filter": {"HashKey": "race"}, "limit": 1})[0]
    require(len(result.get("results", [])) == 1, "concurrent source key disappeared")
    winner = result["results"][0]["fields"]
    require(winner.get("payload") in offered, "concurrent winner was never offered")
    identity = same_atom(read(node, original, key, winner), read(node, sibling, alternate, winner))
    restart(node, crash=True)
    require(same_atom(read(node, original, key, winner), read(node, sibling, alternate, winner)) == identity,
            "concurrent winner changed after crash/reopen")
    return {"writers": len(receipts), "maximum_parallel_requests": 8,
            "winning_payload": winner["payload"], "shared_atom": identity,
            "durable_winner_after_crash_reopen": True}


def late_member(node):
    original = install(node, schema("LateOriginal", "late member"))
    values = {"record_id": "old-record", "group_id": "old-group", "payload": "before-schema"}
    second = {"record_id": "old-second", "group_id": "old-group", "payload": "before-schema-second"}
    write(node, original, {"hash": "old-record"}, values)
    write(node, original, {"hash": "old-second"}, second)
    sibling = install(node, schema("LateSibling", "late member", cross=True))
    original_mol, sibling_mol = molecule(node, original), molecule(node, sibling)
    require(protein(node, original_mol) == protein(node, sibling_mol)
            and protein(node, original_mol) is not None, "late member lacks its protein")
    before = read(node, original, {"hash": "old-record"}, values)
    second_before = read(node, original, {"hash": "old-second"}, second)
    request = {"source_schema": original, "target_schema": sibling,
               "keys": [{"hash": "old-record"}, {"hash": "old-second"}]}
    for invalid_keys in ([], [{"hash": "old-record"}] * 65,
                         [{"hash": "old-record"}, {"hash": "absent"}]):
        require(http_status(node, "/api/db/backfill-protein-keys", dict(request, keys=invalid_keys)) == 400,
                "invalid migration batch was not rejected")
    read(node, sibling, {"hash": "old-group", "range": "old-record"}, None)
    read(node, sibling, {"hash": "old-group", "range": "old-second"}, None)
    # Schema load cannot discover arbitrary source hashes. Supply the exact
    # migration keys; this operation must preserve the existing atom and clock.
    migration = node.call("/api/db/backfill-protein-keys", request)[0]["protein_backfill"]
    require(migration["durable"] is True and migration["keys_supplied"] == 2
            and migration["tips_offered"] == 2 * len(FIELDS)
            and migration["automatic_history_backfill"] is False,
            "explicit historical migration lacks its bounded durable receipt")
    same_atom(read(node, original, {"hash": "old-record"}, values),
              read(node, sibling, {"hash": "old-group", "range": "old-record"}, values))
    require(same_atom(before, read(node, sibling, {"hash": "old-group", "range": "old-record"}, values)),
            "migration replaced the historical source atom")
    same_atom(second_before, read(node, sibling, {"hash": "old-group", "range": "old-second"}, second))
    restart(node)
    same_atom(before, read(node, original, {"hash": "old-record"}, values),
              read(node, sibling, {"hash": "old-group", "range": "old-record"}, values))
    same_atom(second_before, read(node, original, {"hash": "old-second"}, second),
              read(node, sibling, {"hash": "old-group", "range": "old-second"}, second))
    node.call("/api/db/backfill-protein-keys", request)
    same_atom(before, read(node, sibling, {"hash": "old-group", "range": "old-record"}, values))
    write(node, sibling, {"hash": "old-group", "range": "old-record"}, {"payload": "newer"}, "update")
    values["payload"] = "newer"
    newer = read(node, sibling, {"hash": "old-group", "range": "old-record"}, values)
    node.call("/api/db/backfill-protein-keys", request)
    same_atom(newer, read(node, original, {"hash": "old-record"}, values),
              read(node, sibling, {"hash": "old-group", "range": "old-record"}, values))
    write(node, sibling, {"hash": "old-group", "range": "old-record"}, operation="delete")
    require(http_status(node, "/api/db/backfill-protein-keys", request) == 400,
            "migration accepted a deleted source key")
    restart(node)
    read(node, original, {"hash": "old-record"}, None)
    read(node, sibling, {"hash": "old-group", "range": "old-record"}, None)
    same_atom(second_before, read(node, original, {"hash": "old-second"}, second),
              read(node, sibling, {"hash": "old-group", "range": "old-second"}, second))
    return {"historical_record_visible_without_rewrite": True, "migration": migration,
            "same_hash_distinct_ranges_preserved_after_reopen": True,
            "invalid_batch_has_no_partial_write": True, "repeat_creates_no_new_atom": True,
            "newer_update_and_delete_preserved": True,
            "limit": "Explicit supplied key batch only; schema load does not automatically backfill history."}


def ambiguous_backfill(node):
    original = install(node, schema("AmbiguousOriginal", "ambiguous migration"))
    first = {"record_id": "a1", "group_id": "same-target", "payload": "first-source"}
    second = {"record_id": "a2", "group_id": "same-target", "payload": "second-source"}
    write(node, original, {"hash": "a1"}, first)
    write(node, original, {"hash": "a2"}, second)
    definition = schema("AmbiguousTarget", "ambiguous migration")
    definition["key"] = {"hash_field": "group_id"}
    target = install(node, definition)
    source_before = [read(node, original, {"hash": "a1"}, first),
                     read(node, original, {"hash": "a2"}, second)]
    require(protein(node, molecule(node, original)) == protein(node, molecule(node, target))
            and protein(node, molecule(node, original)) is not None,
            "ambiguity check requires existing protein members")
    request = {"source_schema": original, "target_schema": target,
               "keys": [{"hash": "a1"}, {"hash": "a2"}]}
    require(http_status(node, "/api/db/backfill-protein-keys", request) == 400,
            "migration accepted two source records for one target slot")
    read(node, target, {"hash": "same-target"}, None)
    same_atom(source_before[0], read(node, original, {"hash": "a1"}, first))
    same_atom(source_before[1], read(node, original, {"hash": "a2"}, second))
    restart(node)
    read(node, target, {"hash": "same-target"}, None)
    same_atom(source_before[0], read(node, original, {"hash": "a1"}, first))
    same_atom(source_before[1], read(node, original, {"hash": "a2"}, second))
    return {"ambiguous_target_slots_rejected": True, "sources_unchanged_after_reopen": 2,
            "target_empty_after_reopen": True}


def schema_evolution(node):
    original = schema("EvolutionOriginal", "evolution")
    original["descriptive_name"] = "EvolutionFamily"
    install(node, original)
    values = {"record_id": "e1", "group_id": "eg", "payload": "before-expansion"}
    write(node, original["name"], {"hash": "e1"}, values)
    expanded = schema("EvolutionWide", "evolution", fields=FIELDS + ["extra"])
    expanded["descriptive_name"] = "EvolutionFamily (expanded)"
    install(node, expanded)
    require(molecule(node, original["name"]) == molecule(node, expanded["name"]),
            "same-key schema expansion did not preserve its payload molecule")
    read(node, expanded["name"], {"hash": "e1"}, values)
    write(node, expanded["name"], {"hash": "e1"}, {"extra": "new field"}, "update")
    read(node, original["name"], {"hash": "e1"}, values)
    restart(node)
    read(node, expanded["name"], {"hash": "e1"}, dict(values, extra="new field"), FIELDS + ["extra"])
    return {"existing_data_preserved": True, "new_field_after_reopen": True}


def unsupported_layouts(node):
    for layout in ("Single", "Range"):
        definition = schema("Rejected" + layout, "rejected layout")
        definition["schema_type"] = layout
        definition.pop("key")
        require(http_status(node, "/api/schemas/seed-system", {
            "isolated_copy": True, "schema": definition}) == 400, f"{layout} admission was not rejected")
        require(http_status(node, "/api/schema/" + definition["name"]) == 404,
                f"{layout} rejection published a schema")
    return {"unsupported_layouts_rejected_before_publication": ["Single", "Range"]}


def unsupported_marker(node):
    original = install(node, schema("FormatRefusal", "format refusal"))
    values = {"record_id": "preserved", "group_id": "format", "payload": "preserve-existing-data"}
    write(node, original, {"hash": "preserved"}, values)
    node.stop()
    marker = node.home / "data/laststore-pack-node-v1"
    before_marker = marker.read_bytes()
    paths = [node.home / "data" / kind / "CURRENT" for kind in ("disk-run", "disk-cas")]
    before = [file_hash(path) for path in paths]
    unsupported = before_marker.replace(b"format=4\n", b"format=3\n")
    require(unsupported != before_marker, "unexpected current node marker")
    marker.write_bytes(unsupported)
    refused = False
    try:
        try:
            node.start()
        except RuntimeError:
            refused = node.process is not None and node.process.poll() is not None
        require(refused, "unsupported marker did not refuse startup")
        require(marker.read_bytes() == unsupported and [file_hash(path) for path in paths] == before,
                "refused startup changed the existing data roots")
    finally:
        if node.process is not None:
            if node.process.poll() is None:
                node.process.kill()
                node.process.wait(timeout=10)
            node.process = None
            node.log.close()
            node.log = None
        marker.write_bytes(before_marker)
        node.start()
    read(node, original, {"hash": "preserved"}, values)
    return {"unsupported_marker_rejected": True, "current_roots_unchanged": True,
            "original_format_reopen_reads_data": True,
            "limit": "Marker refusal on generated data; no legacy-format conversion claim."}
