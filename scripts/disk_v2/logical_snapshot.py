"""Inspect the real local byte snapshot and its encrypted inclusion map in CI."""

import copy
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid

from logical_capture import Node, file_hash, observed_resources, read, require, save, schema, write
from logical_peer_support import add_model, check_values, export, ingest, item, own_head, request

INTENT = "data/cloud-byte-backup-v2/logical-snapshot-local-v1.json"
SOURCE = "data/cloud-byte-source-v2"


def snapshot(node, action, request_id, expected=(200,)):
    return request(node, {"action": f"snapshot_{action}", "request_id": request_id}, expected)


def expected_positions(pages):
    return sorted([{"origin": page["anchor"]["origin"], "anchor_sha256": page["anchor"]["sha256"],
                    "sequence": str(page["position"]["sequence"]), "sha256": page["position"]["sha256"]}
                   for page in pages], key=lambda position: position["origin"])


def verify(report, request_id, pages, producer):
    require(report["request_id"] == request_id and report["released"] is False
            and report["remote_submitted"] is False, "local capture changed identity or claimed cloud submission")
    inclusion = report["logical_inclusion"]
    require(inclusion["state"] == "enabled", "enrolled source lacks an explicit enabled inclusion map")
    coverage = inclusion["map"]
    require(coverage["version"] == 1 and coverage["producer"] == producer
            and coverage["positions"] == expected_positions(pages)
            and coverage["snapshot"] == report["native_root"]
            and coverage["sha256"] and len(coverage["signature"]) == 128,
            "snapshot inclusion differs from its original signed device positions or exact captured root")
    require(0 < report["root_ciphertext_bytes"] <= 256 * 1024 + 29
            and len(report["root_ciphertext_sha256"]) == 64
            and report["run_profile"]["main"]["home_id"] == report["native_root"]["home_id"],
            "encrypted client metadata lost its captured native profile")


def zero_positions(nodes, output):
    output.mkdir(mode=0o700)
    pages = [export(node) for node in nodes]
    require(all(page["position"]["sequence"] == 0 and not page["events"] for page in pages),
            "fresh source has an unexpected outgoing event")
    node, identity = nodes[0], str(uuid.uuid4())
    request(node, {"action": "enroll", "anchor": pages[1]["anchor"]})
    result = {"own_and_peer_sequence_zero": True}
    try:
        result["capture"] = snapshot(node, "prepare", identity)
        verify(result["capture"], identity, pages, pages[0]["anchor"]["origin"])
        result["inspect"] = snapshot(node, "inspect", identity)
        require(result["inspect"] == result["capture"], "empty positions changed during local inspection")
    finally:
        if (node.home / INTENT).exists():
            result["release"] = snapshot(node, "release", identity)
            require(result["release"]["released"] is True, "empty source capture did not release")
        save(output / "operation.json", result)
    return result


def operate(nodes, pages, model, output):
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "snapshot inclusion operation requires hosted CI")
    output.mkdir(mode=0o700)
    request_ids = [str(uuid.uuid4()) for _ in nodes]
    captures = []
    failure = None
    result = {"complete": False, "remote_submitted": False, "frozen_positions": expected_positions(pages),
              "driver_sha256": file_hash(Path(__file__)), "fresh_existing_peers": 2}
    try:
        for node, page, request_id in zip(nodes, pages, request_ids):
            capture = snapshot(node, "prepare", request_id)
            verify(capture, request_id, pages, page["anchor"]["origin"])
            captures.append(capture)
        result["captures"] = captures
        result["duplicate_prepare"] = [snapshot(node, "prepare", identity)
                                        for node, identity in zip(nodes, request_ids)]
        require(result["duplicate_prepare"] == captures, "same request selected a different local snapshot")
        write_after_capture(nodes, pages, model, result)
        result["after_write_inspection"] = [snapshot(node, "inspect", identity)
                                             for node, identity in zip(nodes, request_ids)]
        require(result["after_write_inspection"] == captures, "later writes or imports changed the frozen snapshot map")
        result["refusals"] = corruption(nodes[1], request_ids[1], captures[0])
        for node in nodes:
            node.stop()
            node.start()
        result["resumed"] = [snapshot(node, "resume", identity)
                              for node, identity in zip(nodes, request_ids)]
        require(result["resumed"] == captures, "reopen selected a different snapshot map or encrypted root")
        result["values_after_reopen"] = [check_values(node, model) for node in nodes]
        result["resources"] = [observed_resources(node) for node in nodes]
        result["capture_disabled"] = disabled_source(nodes[0], output / "capture-disabled")
        result["complete"] = True
    except Exception as error:
        failure = error
        result["error"] = f"{type(error).__name__}: {error}"[:1200]
        raise
    finally:
        result["released"] = []
        for node, identity in zip(nodes, request_ids):
            try:
                if not (node.home / INTENT).exists():
                    continue
                released = snapshot(node, "release", identity)
                require(released["released"] is True and released["remote_submitted"] is False,
                        "owned local snapshot did not release without cloud authority")
                require(snapshot(node, "release", identity) == released, "local release is not idempotent")
                result["released"].append(released)
            except Exception as error:
                result.setdefault("release_errors", []).append(str(error)[:1200])
                result["complete"] = False
        save(output / "operation.json", result)
        if result.get("release_errors") and failure is None:
            raise RuntimeError("owned local snapshot release failed; inspect operation.json")
    return result


def disabled_source(candidate_node, output):
    output.mkdir(mode=0o700)
    environment = {name: value for name, value in candidate_node.environment.items()
                   if name != "LASTDB_LOGICAL_CAPTURE"}
    node = Node(candidate_node.binary, candidate_node.revision, output, environment=environment)
    identity = str(uuid.uuid4())
    result = {"fresh_generated_home": True}
    try:
        node.start()
        value = {"schema": "SnapshotDisabled", "key": {"hash": "disabled"},
                 "fields": {"id": "disabled", "body": "ordinary-default-backup"}}
        node.call("/api/schemas/seed-system", schema(value["schema"], False))
        write(node, [value])
        result["capture"] = snapshot(node, "prepare", identity)
        require(result["capture"]["logical_inclusion"] == {"state": "disabled"}
                and result["capture"]["remote_submitted"] is False,
                "default backup treats disabled capture as unknown or enabled coverage")
        node.stop()
        node.start()
        result["resume"] = snapshot(node, "resume", identity)
        require(result["resume"] == result["capture"], "default backup resume changed explicit disabled coverage")
        read(node, [value])
        result["released"] = snapshot(node, "release", identity)
        require(result["released"]["released"] is True, "default local capture did not release")
    finally:
        node.close()
        result["owned_cleanup"] = json.loads((node.output / "cleanup.json").read_text())
    return result


def write_after_capture(nodes, pages, model, result):
    batches, exported = [], []
    for label, node, page in zip(("a", "b"), nodes, pages):
        batch = [item(f"snapshot-after-{label}", 0)]
        write(node, batch)
        batches.append(batch)
        exported.append(export(node, page["position"]["sequence"], 1))
    result["intervening_imports"] = [ingest(nodes[0], exported[1]["events"]),
                                     ingest(nodes[1], exported[0]["events"])]
    for batch, page in zip(batches, exported):
        add_model(model, [batch], page["events"])
    result["live_positions_after_capture"] = [own_head(node) for node in nodes]
    require(all(own_head(node)["sequence"] > page["position"]["sequence"]
                for node, page in zip(nodes, pages)), "intervening local writes did not advance live heads")
    result["values_after_capture"] = [check_values(node, model) for node in nodes]


def private_file(node, relative):
    base = node.home.resolve(strict=True)
    path = base / relative
    info = path.lstat()
    require(path.resolve(strict=True).is_relative_to(base) and stat.S_ISREG(info.st_mode)
            and info.st_uid == os.getuid() and info.st_nlink == 1 and info.st_mode & 0o077 == 0
            and info.st_size <= 256 * 1024 + 29, "refuse a non-private or unexpected snapshot source file")
    return path


def replace_private(path, data):
    with path.open("r+b") as stream:
        stream.seek(0)
        stream.write(data)
        stream.truncate()
        stream.flush()
        os.fsync(stream.fileno())


def saved_state(node):
    path = private_file(node, SOURCE + "/job.json")
    original = path.read_bytes()
    record = json.loads(original)
    require(record["sha256"] == hashlib.sha256(canonical(record["value"])).hexdigest(),
            "unexpected saved source checksum before deliberate corruption")
    return path, original, record


def canonical(value):
    # The generated values use ASCII keys and exact small JSON integers.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def corrupt_map(node, identity, change):
    path, original, record = saved_state(node)
    change(record["value"])
    record["sha256"] = hashlib.sha256(canonical(record["value"])).hexdigest()
    try:
        replace_private(path, canonical(record))
        return snapshot(node, "resume", identity, (409,))
    finally:
        replace_private(path, original)


def corruption(node, identity, other_capture):
    def changed_position(state):
        position = state["logical_inclusion"]["map"]["positions"][0]
        position["sequence"] = str(int(position["sequence"]) + 1)

    def other_cut(state):
        state["logical_inclusion"] = copy.deepcopy(other_capture["logical_inclusion"])

    def bad_signature(state):
        state["logical_inclusion"]["map"]["signature"] = "00" * 64

    result = {"changed_position": corrupt_map(node, identity, changed_position),
              "signature_only": corrupt_map(node, identity, bad_signature),
              "other_snapshot_certificate": corrupt_map(node, identity, other_cut)}
    capture = snapshot(node, "inspect", identity)
    name = f"{SOURCE}/payload-{capture['root_object_index']:08d}.enc"
    path = private_file(node, name)
    original = path.read_bytes()
    require(hashlib.sha256(original).hexdigest() == capture["root_ciphertext_sha256"],
            "root ciphertext differs before deliberate corruption")
    changed = bytearray(original)
    changed[-1] ^= 1
    try:
        replace_private(path, changed)
        result["changed_encrypted_root"] = snapshot(node, "inspect", identity, (409,))
    finally:
        replace_private(path, original)
    require(snapshot(node, "inspect", identity) == capture, "owned corruption repair changed the frozen source")
    return result
