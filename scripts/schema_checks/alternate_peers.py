"""Original signed field events rebuild alternate entries from accepted winners."""

import os
from pathlib import Path
import sys

from alternate_keys import task_definition
from alternate_changes import alternate, rebuild, refused
from node_ops import ENVIRONMENT, install, require, write, restart
from node import Node, save
from alternate_key_compare import check_rows
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "disk_v2"))
from logical_peer_support import export, ingest, own_head, position, request


def peers(args, output):
    output.mkdir(mode=0o700)
    nodes = []
    result = {}
    try:
        for label in ("source", "receiver"):
            folder = output / label
            folder.mkdir(mode=0o700)
            node = Node(args.candidate, args.candidate_revision, folder,
                        environment={**ENVIRONMENT, "LASTDB_LOGICAL_CAPTURE": "1"},
                        temporary_directory=Path(os.environ["RUNNER_TEMP"]).resolve())
            nodes.append(node)
            node.start()
            install(node, task_definition("PeerAlternate"))
        a, b = nodes
        name, key = "PeerAlternate", {"hash": "peer-1"}
        fields = {"record_id": "peer-1", "group_id": "g1", "owner_id": "o1", "payload": "first"}
        write(a, name, key, fields)
        write(a, name, key, {"payload": "second"}, "update")
        write(a, name, key, {"group_id": "g2"}, "update")
        page = export(a)
        require(len(page["events"]) == 3, "alternate capture lacks its three original requests")
        revisions = {outcome.get("alternate_key_revision") for event in page["events"] for outcome in event["outcomes"]}
        require(len(revisions) == 1 and None not in revisions, "signed outcomes lack a declaration revision")
        request(b, {"action": "enroll", "anchor": page["anchor"]})
        result["first_import"] = ingest(b, page["events"])
        expected = {**fields, "group_id": "g2", "payload": "second"}
        check_rows(alternate(b, name, "by_group", "g1"), {})
        check_rows(alternate(b, name, "by_group", "g2"), {"peer-1": expected})
        result["exact_retry"] = ingest(b, page["events"])
        require(result["exact_retry"]["imported_events"] == 0, "exact replay imported another copy")
        write(a, name, key, {"group_id": "g3"}, "update")
        older = export(a, after=3)
        write(b, name, key, {"group_id": "g4"}, "update")
        ingest(b, older["events"])
        expected["group_id"] = "g4"
        check_rows(alternate(b, name, "by_group", "g4"), {"peer-1": expected})
        check_rows(alternate(b, name, "by_group", "g3"), {})
        updated = {"by_group": {"hash_field": "owner_id", "range_field": "record_id", "missing": "reject"}}
        rebuild(a, name, updated, [key])
        write(a, name, key, {"payload": "third"}, "update")
        new_revision = export(a, after=4)
        prior = position(b, page["anchor"]["origin"])
        outgoing = own_head(b)
        refused(lambda: b.call("/api/sync/logical-v1", {"action": "import", "events": new_revision["events"]}), "revision mismatch")
        require(position(b, page["anchor"]["origin"]) == prior and own_head(b) == outgoing,
                "refused declaration mismatch changed peer progress")
        rebuild(b, name, updated, [key])
        result["retry_after_matching_backfill"] = ingest(b, new_revision["events"])
        expected["payload"] = "third"
        restart(b, crash=True)
        check_rows(alternate(b, name, "by_group", "o1"), {"peer-1": expected})
        result.update(declaration_revisions_signed=True, older_key_field_keeps_winner=True,
                      refused_revision_keeps_progress=True, crash_reopen_keeps_entries=True)
        save(output / "result.json", result)
        return result
    finally:
        for node in reversed(nodes):
            node.close()
