#!/usr/bin/env python3
"""Transfer signed logical events between two real generated V2 homes in hosted CI."""

from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path

from logical_capture import Node, file_hash, observed_resources, read, require, save, write
from logical_peer_support import (add_model, author, check_values, export, ingest, item,
                                  own_head, position, record_key, records, refusals, request)
import logical_peer_crash

CRASH_PAGES = 8
ENVIRONMENT = {
    "LASTDB_LOGICAL_CAPTURE": "1", "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_TTL_SWEEP": "0",
    "LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "EXEMEM_API_URL": "http://127.0.0.1:9",
    "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
}


def run(candidate, output):
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "logical peer operation requires hosted CI")
    output.mkdir(mode=0o700)
    result = {"source_revision": candidate[1], "binary_sha256": file_hash(candidate[0]),
              "driver_sources": driver_sources(), "complete": False,
              "primary_access": False, "cloud_access": False, "fresh_nodes": 2,
              "max_events_per_page": 4, "event_array_max_bytes": 1024 * 1024,
              "http_max_bytes": 1024 * 1024 + 8192, "max_crash_pages": CRASH_PAGES}
    nodes = []
    try:
        for label in ("a", "b"):
            folder = output / label
            folder.mkdir(mode=0o700)
            nodes.append(Node(candidate[0], candidate[1], folder, environment=dict(ENVIRONMENT)))
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda node: node.start(), nodes))
        require(all(node.disk_format() == 2 for node in nodes), "fresh peers are not V2")
        operate(nodes, output, result)
        result["complete"] = True
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"[:1200]
    finally:
        result["owned_cleanup"] = []
        for node in reversed(nodes):
            try:
                node.close()
                result["owned_cleanup"].append(json.loads((node.output / "cleanup.json").read_text()))
            except Exception as error:
                result.setdefault("cleanup_errors", []).append(str(error)[:1200])
                result["complete"] = False
        save(output / "operation.json", result)
    return result


def driver_sources():
    root = Path(__file__).resolve().parent
    paths = [root / name for name in ["logical_peer.py", "logical_peer_support.py", "logical_peer_crash.py",
                                      "logical_capture.py", "node_proof.py"]]
    paths.extend(root.parent / "benchmarks" / name for name in ["node.py", "disk_usage.py"])
    return {str(path.relative_to(root.parent)): file_hash(path) for path in paths}


def operate(nodes, output, result):
    a, b = nodes
    batches = [author(a, "a"), author(b, "b")]
    pages = [export(a), export(b)]
    require(all(len(page["events"]) == 3 for page in pages), "authored requests lack original events")
    require(pages[0]["anchor"]["origin"] != pages[1]["anchor"]["origin"], "fresh peer identities are equal")
    for label, page in zip(("a", "b"), pages):
        save(output / f"{label}-original-events.json", page)
    identities = []
    for page in pages:
        identities.append({(outcome["schema_name"], outcome["field_name"]):
                           (outcome["schema_identity"], outcome["field_identity"], outcome["molecule_uuid"])
                           for event in page["events"] for outcome in event["outcomes"]})
    require(identities[0] == identities[1], "identical canonical seed schemas have different field or molecule identities")
    result["matching_canonical_identities"] = len(identities[0])
    result["refusals"] = refusals(b, pages[0])
    refusal_values(b, batches, pages, result)
    request(a, {"action": "enroll", "anchor": pages[1]["anchor"]})
    heads = [own_head(node) for node in nodes]
    with ThreadPoolExecutor(max_workers=2) as pool:
        result["first_transfer"] = list(pool.map(lambda pair: ingest(*pair),
            [(a, pages[1]["events"]), (b, pages[0]["events"])]))
    model = {}
    for authored, page in zip(batches, pages):
        add_model(model, authored, page["events"])
    result["converged_values"] = [check_values(node, model) for node in nodes]
    result["duplicate_transfer"] = []
    for node, page in [(a, pages[1]), (b, pages[0])]:
        receipt = ingest(node, page["events"])
        require(receipt["imported_events"] == 0 and receipt["duplicate_events"] == 3
                and receipt["position"] == page["position"], "exact replay changed durable peer progress")
        result["duplicate_transfer"].append(receipt)
    require([own_head(node) for node in nodes] == heads, "two-way imports changed outgoing heads")
    result["resources_before_crash"] = [observed_resources(node) for node in nodes]
    verify_rosters(nodes, pages, result)
    crash_phase(a, b, model, output, result)


def refusal_values(receiver, batches, pages, result):
    local, remote = {}, {}
    add_model(local, batches[1], pages[1]["events"])
    add_model(remote, batches[0], pages[0]["events"])
    result["values_after_refusals"] = check_values(receiver, local)
    absent = [entry for entry in records(remote) if record_key(entry) not in local]
    read(receiver, [], absent)
    result["refused_remote_keys_absent"] = len(absent)


def verify_rosters(nodes, pages, result):
    result["peer_rosters"] = []
    for node, page in [(nodes[0], pages[1]), (nodes[1], pages[0])]:
        peers = request(node, {"action": "position"})["peers"]
        require(peers == [{"anchor": page["anchor"], "position": page["position"]}],
                "owner roster differs from the explicit enrolled peer and durable position")
        result["peer_rosters"].append(peers)


def crash_phase(source, receiver, model, output, result):
    initial = position(receiver, own_head(source)["origin"])
    receiver_head = own_head(receiver)
    batches = [[item("crash", number * 2 + offset, size=32 * 1024) for offset in range(2)]
               for number in range(CRASH_PAGES)]
    for batch in batches:
        write(source, batch)
    pages = [export(source, initial["sequence"] + offset, 1) for offset in range(CRASH_PAGES)]
    save(output / "crash-original-events.json", pages)
    result["interruption"] = logical_peer_crash.interrupt(receiver, pages)
    count = logical_peer_crash.recover(receiver, initial, receiver_head, pages, batches, model, result)
    result["catch_up"] = [ingest(receiver, page["events"]) for page in pages[count:]]
    add_model(model, batches, [page["events"][0] for page in pages])
    result["final_values"] = [check_values(node, model) for node in [source, receiver]]
    require(position(receiver, initial["origin"]) == own_head(source), "catch-up did not reach original source head")
    require(own_head(receiver) == receiver_head, "crash recovery or catch-up authored an echo")
    result["resources_final"] = [observed_resources(node) for node in [source, receiver]]
    for node in [source, receiver]:
        node.stop()
        node.start()
    result["final_reopen_values"] = [check_values(node, model) for node in [source, receiver]]
    require(position(receiver, initial["origin"]) == own_head(source) and own_head(receiver) == receiver_head,
            "final clean reopens changed source or import positions")


def summary(result):
    return "\n".join([
        "## Two production logical peers and receiver SIGKILL", "",
        f"Result: {'PASS' if result['complete'] else 'FAIL'}. Source: `{result['source_revision']}`.",
        "Two fresh V2 homes exchange original signed events through the owner API.",
        "The operation checks explicit trust, signatures, exact retries, gaps, tamper refusal, keyed values, and range pages.",
        "Conflicts use the original signed tip order. Imports must leave each local outgoing head unchanged.",
        "A SIGKILL interrupts the receiver with an import request pending. Two reopens check the durable cursor and exact data prefix.",
        "Checkpoint disk and memory samples have finite safety caps. They do not establish universal peaks or power-loss behavior.",
        "The driver closes both owned nodes and removes only their generated homes. The primary and cloud stay unchanged.",
        f"Detail: {result.get('error', 'complete')}", ""])
