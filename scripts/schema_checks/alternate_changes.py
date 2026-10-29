"""Real owner requests for bounded alternate declaration backfill and retirement."""

import json
import os
import subprocess
from urllib.parse import urlencode

from alternate_keys import task_definition
from node_ops import install, require, schema, write
from alternate_key_compare import FIELDS, check_rows

PATH = "/api/db/alternate-keys"


def refused(operation, fragment):
    try:
        operation()
    except RuntimeError as error:
        require("HTTP " in str(error) and fragment.lower() in str(error).lower(),
                f"request failed without its required refusal: {str(error)[:800]}")
    else:
        raise RuntimeError("request accepted an unsupported or incomplete state")


def owned_values(number):
    return {"record_id": f"existing-{number:04}", "group_id": f"g{number % 8}",
            "owner_id": f"o{number % 4}", "payload": f"existing payload {number}"}


def primary_rows(node, name, model):
    records, keys = {}, sorted(model)
    for offset in range(0, len(keys), 64):
        chunk = keys[offset:offset + 64]
        body = {"schema_name": name, "fields": FIELDS, "filter": {"HashRangeKeys": [[key, ""] for key in chunk]},
                "limit": len(chunk)}
        reply = node.call("/api/query", body)[0]
        check_rows(reply, {key: model[key] for key in chunk})
        records.update({row["fields"]["record_id"]: row["metadata"] for row in reply["results"]})
    return records


def change(node, name, action, **fields):
    reply = node.call(PATH, {"schema": name, "action": action, **fields})[0]["alternate_key_change"]
    require(reply["durable"] is True, "alternate change lacks durable completion")
    return reply


def clean(node, name, receipt):
    for _ in range(100):
        if receipt["phase"] in ("complete", "aborted"):
            return receipt
        receipt = change(node, name, "cleanup", change_id=receipt["change_id"])
    raise RuntimeError("alternate retirement did not complete within its page bound")


def rebuild(node, name, declarations, keys):
    old = change(node, name, "status")
    started = change(node, name, "begin", expected_revision=old["active_revision"], alternate_keys=declarations)
    for offset in range(0, len(keys), 64):
        change(node, name, "backfill", change_id=started["change_id"], keys=keys[offset:offset + 64])
    return clean(node, name, change(node, name, "commit", change_id=started["change_id"]))


def alternate(node, name, key_name, hash_value, **options):
    return node.call("/api/query", {"schema_name": name, "key_name": key_name, "fields": FIELDS,
                                   "filter": {"HashKey": hash_value}, **options})[0]


def list_cli(program, node, name, expected=None):
    command = [str(program), "--data-dir", str(node.home), "list", name,
               "--key-name", "by_group", "--key-hash", "g0", "--json"]
    reply = subprocess.run(command, env={**os.environ, "LASTDB_SOCKET_PATH": str(node.socket)},
                           stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=60)
    require(len(reply.stdout) <= 128 * 1024 and len(reply.stderr) <= 128 * 1024, "key-list CLI exceeded its output bound")
    if expected is None:
        require(reply.returncode != 0 and "did not honor" in reply.stderr,
                "new CLI accepted an older node's primary-key fallback")
        return {"older_node_refused": True}
    require(reply.returncode == 0, f"named key-list CLI failed: {reply.stderr[:600]}")
    report = json.loads(reply.stdout)
    require(report["key_name"] == "by_group" and not report["has_more"]
            and sorted(key["range"] for key in report["keys"]) == sorted(expected)
            and sorted(key["hash"] for key in report["primary_keys"]) == sorted(expected),
            "named key-list CLI changed alternate or primary keys")
    return {"keys": len(expected), "alternate_and_primary_keys": True}


def converted(node, model, before, cli):
    name = "ExistingTask"
    declarations = task_definition()["alternate_keys"]
    state = change(node, name, "status")
    state = change(node, name, "begin", expected_revision=state["active_revision"], alternate_keys=declarations)
    identifier = state["change_id"]
    keys = [{"hash": key} for key in sorted(model)]
    first = change(node, name, "backfill", change_id=identifier, keys=keys[:64])
    repeated = change(node, name, "backfill", change_id=identifier, keys=keys[:64])
    require(first == repeated and repeated["processed_keys"] == 64, "repeated page changes backfill coverage")
    refused(lambda: change(node, name, "commit", change_id=identifier), "every frozen")
    refused(lambda: write(node, name, keys[0], {"payload": "forbidden"}, "update"), "blocked")
    node.stop()
    node.start()
    require(change(node, name, "status")["processed_keys"] == 64, "reopen lost backfill progress")
    for offset in (64, 128):
        change(node, name, "backfill", change_id=identifier, keys=keys[offset:offset + 64])
    completed = clean(node, name, change(node, name, "commit", change_id=identifier))
    require(primary_rows(node, name, model) == before, "backfill changed primary atoms, clocks, or metadata")
    expected = {key: fields for key, fields in model.items() if fields["group_id"] == "g0"}
    check_rows(alternate(node, name, "by_group", "g0"), expected)
    count = alternate(node, name, "by_group", "g0", response_format="count")
    require(count["exact"] is True and count["total_count"] == len(expected), "alternate count differs")
    cli_keys = list_cli(cli, node, name, expected)
    listed, cursor = [], None
    for _ in range(32):
        params = {"schema": name, "key_name": "by_group", "hash": "g0", "limit": 7}
        if cursor:
            params["cursor"] = cursor
        page = node.call("/api/list?" + urlencode(params))[0]["list"]
        listed.extend(key["range"] for key in page["keys"])
        if not page["has_more"]:
            break
        cursor = page["next_cursor"]
    require(len(listed) == len(set(listed)) and set(listed) == set(expected), "named key-list coverage differs")
    old_cursor = alternate(node, name, "by_group", "g0", limit=1)["next_cursor"]
    updated = {"by_group": {"hash_field": "owner_id", "range_field": "record_id", "missing": "reject"}}
    rebuild(node, name, updated, keys)
    refused(lambda: alternate(node, name, "by_group", "g0", cursor=old_cursor), "cursor")
    refused(lambda: alternate(node, name, "by_owner", "o0"), "not declared")
    check_rows(alternate(node, name, "by_group", "g0"), {})
    check_rows(alternate(node, name, "by_group", "o0"),
               {key: fields for key, fields in model.items() if fields["owner_id"] == "o0"})
    rebuild(node, name, {}, keys)
    definition = schema("UnrelatedAfterRemoval", "existing compatibility", fields=FIELDS)
    install(node, definition)
    write(node, "UnrelatedAfterRemoval", keys[0], {**model[keys[0]["hash"]], "payload": "unrelated"})
    write(node, name, keys[0], {"payload": "changed independently"}, "update")
    peer = node.call("/api/query", {"schema_name": "UnrelatedAfterRemoval", "fields": FIELDS,
                                   "filter": {"HashKey": keys[0]["hash"]}})[0]
    require(peer["results"][0]["fields"]["payload"] == "unrelated", "removal restored automatic protein writes")
    aborted = abort_partial(node, declarations)
    return {"records": len(model), "resumed_after_reopen": True, "duplicate_page": True,
            "primary_atoms_and_clocks_unchanged": True, "count_and_key_list": True,
            "declaration_replacement_and_removal": True, "separate_field_reuse_after_removal": True,
            "completed": completed, "abort": aborted, "cli": cli_keys}


def abort_partial(node, declarations):
    name = install(node, schema("AbortAlternate", "abort compatibility", fields=FIELDS))
    write(node, name, {"hash": "good"}, {"record_id": "good", "group_id": "g", "owner_id": "o", "payload": "good"})
    write(node, name, {"hash": "missing"}, {"record_id": "missing", "payload": "preserved"})
    model = {"good": {"record_id": "good", "group_id": "g", "owner_id": "o", "payload": "good"},
             "missing": {"record_id": "missing", "payload": "preserved"}}
    before = primary_rows(node, name, model)
    current = change(node, name, "status")
    started = change(node, name, "begin", expected_revision=current["active_revision"], alternate_keys=declarations)
    change(node, name, "backfill", change_id=started["change_id"], keys=[{"hash": "good"}])
    refused(lambda: change(node, name, "backfill", change_id=started["change_id"], keys=[{"hash": "missing"}]), "required alternate")
    receipt = clean(node, name, change(node, name, "abort", change_id=started["change_id"]))
    require(receipt["phase"] == "aborted", "abort did not restore availability")
    require(primary_rows(node, name, model) == before, "abort changed primary atoms, clocks, or metadata")
    write(node, name, {"hash": "missing"}, {"payload": "usable again"}, "update")
    return receipt
