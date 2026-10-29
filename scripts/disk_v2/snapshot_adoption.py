"""Request read-only adoption plans for real held snapshots in hosted CI."""

import copy
import json
import os
from pathlib import Path
import uuid

from logical_capture import Node, UnixConnection, file_hash, observed_resources, read, require, save, schema, write
from logical_peer_support import HTTP_BYTES, add_model, check_values, encoded, export, ingest, item, own_head, request
from logical_snapshot import INTENT, SOURCE, private_file, snapshot

FIXED = {"shared_base_proof", "candidate_native_verification", "local_cut_revalidation",
         "local_identity_and_anchor_preservation", "receipt_base_and_control_rewrite", "atomic_publication"}


def candidate(capture):
    return {key: copy.deepcopy(capture[key]) for key in ("native_root", "run_profile", "logical_inclusion")}


def plan(node, identity, value, expected=(200,)):
    return request(node, {"action": "snapshot_plan", "request_id": identity, "candidate": value}, expected)


def metadata(node):
    names = (INTENT, SOURCE + "/job.json", "data/logical-device-capture-v1")
    return {name: file_hash(private_file(node, name)) for name in names}


def verify(report, capture, value):
    require({key: item for key, item in report.items() if key != "adoption_plan"} == capture,
            "plan changed the original capture response")
    detail = report["adoption_plan"]
    require(detail["version"] == 1 and detail["replacement_allowed"] is False
            and detail["local_root"] == capture["native_root"]
            and detail["candidate_root"] == value["native_root"]
            and detail["local_map_sha256"] == capture["logical_inclusion"]["map"]["sha256"]
            and detail["candidate_map_sha256"] == value["logical_inclusion"]["map"]["sha256"]
            and detail["local_origin"] == capture["logical_inclusion"]["map"]["producer"],
            "plan lost its frozen local cut or candidate context")
    requirements = detail["requirements"]
    require(FIXED <= set(requirements) and len(set(requirements)) == len(requirements) <= 9
            and 1 <= len(detail["origins"]) <= 33,
            "plan lacks explicit replacement requirements or exceeds its bounds")
    return detail


def release(node, identity):
    report = snapshot(node, "release", identity)
    require(report["released"] is True and report["remote_submitted"] is False
            and snapshot(node, "release", identity) == report, "owned snapshot release is not exact and idempotent")
    return report


def zero_plans(nodes, pages, capture, identity, output):
    other_id = str(uuid.uuid4())
    result = {"complete": False, "stage": "prepare_other"}
    try:
        other = snapshot(nodes[1], "prepare", other_id)
        result["other_capture"] = other
        other_before = metadata(nodes[1])
        result["stage"] = "untrusted_producer"
        result["untrusted_producer"] = plan(nodes[1], other_id, candidate(capture), (409,))
        result["stage"] = "check_other_unchanged"
        require(request(nodes[1], {"action": "position"})["peers"] == []
                and metadata(nodes[1]) == other_before and snapshot(nodes[1], "inspect", other_id) == other,
                "plan enrolled the unknown producer or changed the local source")
        before = metadata(nodes[0])
        result["stage"] = "zero_plan"
        result["zero_plan"] = plan(nodes[0], identity, candidate(other))
        detail = verify(result["zero_plan"], capture, candidate(other))
        require(all(origin["local_position"]["sequence"] == "0" for origin in detail["origins"])
                and "shared_base_proof" in detail["requirements"] and metadata(nodes[0]) == before,
                "zero positions became proof of a shared or empty snapshot")
        result["stage"] = "data_before_capture"
        result["data_before_capture"] = existing_data_zero(nodes[0], pages, capture, output)
        result["stage"] = "complete"
        result["complete"] = True
    except Exception as error:
        result["error"] = f"{type(error).__name__}: {error}"[:1200]
        raise
    finally:
        try:
            if (nodes[1].home / INTENT).exists():
                result["other_release"] = release(nodes[1], other_id)
        except Exception as error:
            result["release_error"] = f"{type(error).__name__}: {error}"[:1200]
            result["complete"] = False
            raise
        finally:
            save(output.parent / "adoption-zero.json", result)
    return result


def existing_data_zero(source, pages, original, output):
    output.mkdir(mode=0o700)
    environment = {name: value for name, value in source.environment.items() if name != "LASTDB_LOGICAL_CAPTURE"}
    node = Node(source.binary, source.revision, output, environment=environment)
    identity = str(uuid.uuid4())
    result = {"fresh_generated_home": True, "data_before_capture": True}
    value = {"schema": "BeforeCapture", "key": {"hash": "old"}, "fields": {"id": "old", "body": "preexisting"}}
    try:
        node.start()
        node.call("/api/schemas/seed-system", schema(value["schema"], False))
        write(node, [value])
        node.stop()
        node.environment["LASTDB_LOGICAL_CAPTURE"] = "1"
        node.start()
        require(own_head(node)["sequence"] == 0, "old data received a fabricated outgoing history")
        for page in pages:
            request(node, {"action": "enroll", "anchor": page["anchor"]})
        result["no_capture_refusal"] = plan(node, identity, candidate(original), (409,))
        require(not (node.home / INTENT).exists(), "plan created a missing capture intent")
        capture = snapshot(node, "prepare", identity)
        before = metadata(node)
        result["plan"] = plan(node, identity, candidate(original))
        detail = verify(result["plan"], capture, candidate(original))
        require(all(origin["local_position"]["sequence"] == "0" for origin in detail["origins"])
                and "shared_base_proof" in detail["requirements"] and metadata(node) == before,
                "existing data at sequence zero became an empty-base certificate")
        read(node, [value])
        node.stop()
        node.start()
        require(plan(node, identity, candidate(original)) == result["plan"], "zero-data plan changed after restart")
        read(node, [value])
        result["old_data_unchanged"] = True
    finally:
        try:
            if (node.home / INTENT).exists():
                result["release"] = release(node, identity)
        finally:
            node.close()
            result["owned_cleanup"] = json.loads((node.output / "cleanup.json").read_text())
            save(output / "operation.json", result)
    return result


def next_events(nodes, model):
    pages = []
    for label, node in zip(("a", "b"), nodes):
        before = own_head(node)["sequence"]
        batch = [item(f"adoption-{label}", 0)]
        write(node, batch)
        page = export(node, before, 1)
        require(len(page["events"]) == 1 and page["position"]["sequence"] == before + 1,
                "adoption write did not produce one original event")
        add_model(model, [batch], page["events"])
        pages.append(page)
    return pages


def peer_ahead(report, capture, value, ahead_origin):
    detail = verify(report, capture, value)
    origins = {entry["origin"]: entry for entry in detail["origins"]}
    entry = origins[ahead_origin]
    require(entry["prefix"]["state"] == "candidate_prefix_proof_required"
            and entry["prefix"]["at"] == entry["local_position"]
            and entry["replay"] is None and "candidate_prefix_proof" in detail["requirements"],
            "peer-ahead candidate received an unproved ancestry claim")


def tails(report, capture, value):
    detail = verify(report, capture, value)
    require({"own_tail_replay", "original_source_tails"} <= set(detail["requirements"]),
            "plan omitted required local or original-device tails")
    local = {entry["origin"]: entry for entry in capture["logical_inclusion"]["map"]["positions"]}
    remote = {entry["origin"]: entry for entry in value["logical_inclusion"]["map"]["positions"]}
    for entry in detail["origins"]:
        origin, replay = entry["origin"], entry["replay"]
        after = {key: remote[origin][key] for key in ("sequence", "sha256")}
        through = {key: local[origin][key] for key in ("sequence", "sha256")}
        source = "local_authored_stream" if origin == detail["local_origin"] else "original_source_device"
        require(entry["prefix"]["state"] == "local_prefix_verified"
                and replay == {"after": after, "through": through, "source": source}
                and after["sequence"] == "4" and through["sequence"] == "5",
                "tail plan changed an original boundary hash or source")


def refused_candidates(node, identity, capture):
    result = {}
    for name in ("legacy_unknown", "disabled", "signature", "native_root", "run_profile"):
        value = candidate(capture)
        if name == "legacy_unknown":
            value["logical_inclusion"] = None
        elif name == "disabled":
            value["logical_inclusion"] = {"state": "disabled"}
        elif name == "signature":
            value["logical_inclusion"]["map"]["signature"] = "00" * 64
        elif name == "native_root":
            value["native_root"]["current_sha256"] = "00" * 32
        else:
            value["run_profile"]["main"]["current_sha256"] = "00" * 32
        result[name] = plan(node, identity, value, (409,))
    return result


def padded_request(node, body, length, status):
    payload = encoded(body)
    require(len(payload) <= length, "planned padding is shorter than the real request")
    connection = UnixConnection(node.socket)
    try:
        connection.request("POST", "/api/sync/logical-v1", payload + b" " * (length - len(payload)),
                           {"Content-Type": "application/json", "X-LastDB-Client": "snapshot-adoption-ci"})
        response = connection.getresponse()
        raw = response.read(HTTP_BYTES + 1)
        require(response.status == status and len(raw) <= HTTP_BYTES, "snapshot action byte limit changed")
        return json.loads(raw) if status == 200 else {"http_status": status, "response_bytes": len(raw)}
    finally:
        connection.close()


def byte_limits(node, identity, capture, expected):
    body = {"action": "snapshot_plan", "request_id": identity, "candidate": candidate(capture)}
    require(padded_request(node, body, 8193, 200) == expected, "plan did not receive its separate byte allowance")
    return {"plan_40k_limit": padded_request(node, body, 40 * 1024 + 1, 413),
            "ordinary_8k_limit": padded_request(node, {"action": "snapshot_inspect", "request_id": identity},
                                                 8193, 413)}


def operate(nodes, model, output):
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "snapshot adoption operation requires hosted CI")
    output.mkdir(mode=0o700)
    a, b = nodes
    active, result = {}, {"complete": False, "replacement_executed": False, "cloud_access": False,
                          "stage": "capture_a4",
                          "driver_sha256": file_hash(Path(__file__))}
    failure = None
    try:
        a4_id, b4_id = str(uuid.uuid4()), str(uuid.uuid4())
        a4 = snapshot(a, "prepare", a4_id)
        active[a] = a4_id
        release(a, a4_id)
        active.pop(a)
        result["stage"] = "capture_b4"
        b4 = snapshot(b, "prepare", b4_id)
        active[b] = b4_id
        result["stage"] = "author_fifth_events"
        pages = next_events(nodes, model)
        result["stage"] = "import_b5_into_a"
        result["import_b5_into_a"] = ingest(a, pages[1]["events"])
        a5_id = str(uuid.uuid4())
        result["stage"] = "capture_a5"
        a5 = snapshot(a, "prepare", a5_id)
        active[a] = a5_id
        result["stage"] = "own_future_refusal"
        result["own_future_refusal"] = plan(b, b4_id, candidate(a5), (409,))
        release(b, b4_id)
        active.pop(b)
        b5_id = str(uuid.uuid4())
        result["stage"] = "capture_b5_a4"
        b5 = snapshot(b, "prepare", b5_id)
        active[b] = b5_id
        before = metadata(b)
        result["stage"] = "peer_ahead_plan"
        result["peer_ahead"] = plan(b, b5_id, candidate(a5))
        peer_ahead(result["peer_ahead"], b5, candidate(a5), own_head(a)["origin"])
        result["stage"] = "live_import_frozen_plan"
        result["import_a5_into_b"] = ingest(b, pages[0]["events"])
        require(plan(b, b5_id, candidate(a5)) == result["peer_ahead"] and metadata(b) == before,
                "live import changed the held local cut or its plan")
        result["stage"] = "restart_frozen_plan"
        b.stop()
        b.start()
        require(plan(b, b5_id, candidate(a5)) == result["peer_ahead"] and metadata(b) == before,
                "restart changed the held local cut or its plan")
        require(snapshot(b, "inspect", b5_id) == b5, "planning altered the original capture")
        release(b, b5_id)
        active.pop(b)
        final_id = str(uuid.uuid4())
        result["stage"] = "capture_b5_a5"
        final = snapshot(b, "prepare", final_id)
        active[b] = final_id
        before = metadata(b)
        result["stage"] = "tail_plan"
        result["tails"] = plan(b, final_id, candidate(a4))
        tails(result["tails"], final, candidate(a4))
        result["stage"] = "candidate_refusals"
        result["candidate_refusals"] = refused_candidates(b, final_id, a4)
        result["stage"] = "byte_limits"
        result["byte_limits"] = byte_limits(b, final_id, a4, result["tails"])
        result["stage"] = "wrong_request_refusal"
        result["wrong_request"] = plan(b, str(uuid.uuid4()), candidate(a4), (409,))
        require(plan(b, final_id, candidate(a4)) == result["tails"] and metadata(b) == before
                and snapshot(b, "inspect", final_id) == final, "plan or refusal changed source metadata")
        result["stage"] = "converged_values"
        result["converged_values"] = [check_values(node, model) for node in nodes]
        result["resources"] = [observed_resources(node) for node in nodes]
        result["complete"] = True
        result["stage"] = "complete"
    except Exception as error:
        failure = error
        result["error"] = f"{type(error).__name__}: {error}"[:1200]
        raise
    finally:
        result["released"] = []
        for node, identity in active.items():
            try:
                result["released"].append(release(node, identity))
            except Exception as error:
                result.setdefault("release_errors", []).append(str(error)[:1200])
                result["complete"] = False
        save(output / "operation.json", result)
        if result.get("release_errors") and failure is None:
            raise RuntimeError("owned adoption snapshot release failed")
    return result
