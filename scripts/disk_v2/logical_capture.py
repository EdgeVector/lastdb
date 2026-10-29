#!/usr/bin/env python3
"""Operate production logical capture across an owned daemon SIGKILL and reopen."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import re
import signal
import sys
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from node import Node, UnixConnection, file_hash, save


HASH_SCHEMA, RANGE_SCHEMA = "CaptureHashOperation", "CaptureRangeOperation"
MAX_CRASH_BATCHES, MEMORY_LIMIT, DISK_LIMIT = 8, 1024 ** 3, 256 * 1024 ** 2


def require(value, message):
    if not value:
        raise RuntimeError(message)


def capture(node, anchor=None):
    status = node.call("/api/status")[0]["status"]["logical_capture"]
    require(status["enabled"] is True and status["position"], "capture is not durably enabled")
    head, recent = status["position"], status["recent"]
    require(head["sequence"] <= 14 and len(recent) == head["sequence"],
            "the bounded operation lost its complete metadata window")
    require(0 <= status["encoded_backlog_bytes"] <= status["backlog_cap_bytes"],
            "encoded capture backlog exceeds its reported cap")
    previous = anchor or head["sha256"]
    for sequence, event in enumerate(recent, 1):
        require(event["sequence"] == sequence and event["origin"] == head["origin"]
                and event["previous_sha256"] == previous
                and re.fullmatch(r"[0-9a-f]{64}", event["sha256"])
                and event["encoded_event_bytes"] > 0 and event["outcome_count"] > 0,
                "captured metadata has a gap, changed origin, hash link, or empty outcome")
        require(event["mutation_ids"] and len(set(event["mutation_ids"])) == len(event["mutation_ids"]),
                "captured mutation identities are absent or repeated")
        previous = event["sha256"]
    if recent:
        require(previous == head["sha256"], "capture head differs from its final metadata entry")
    return status


def schema(name, ranged):
    fields = ["hash", "range", "body"] if ranged else ["id", "body"]
    return {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": name, "descriptive_name": name,
        "schema_type": "HashRange" if ranged else "Hash",
        "key": {"hash_field": "hash", "range_field": "range"} if ranged else {"hash_field": "id"},
        "fields": fields, "field_types": {field: "String" for field in fields},
        "field_descriptions": {field: f"{name} {field}" for field in fields}}}


def records(batch, ranged=True):
    result = []
    for offset in range(2):
        identity = batch * 2 + offset
        body = f"durable-capture-{batch}-{offset}:" + "v" * 1024
        key = {"hash": "capture", "range": f"{identity:012d}"} if ranged else {"hash": f"key-{identity}"}
        fields = {"hash": key["hash"], "range": key["range"], "body": body} if ranged else {
            "id": key["hash"], "body": body}
        result.append({"schema": RANGE_SCHEMA if ranged else HASH_SCHEMA,
                       "key": key, "fields": fields})
    return result


def mutations(items, verb="create", expected=None):
    return [{"type": "mutation", "schema": item["schema"], "mutation_type": verb,
             "durability": "durable", "key_value": item["key"],
             "fields_and_values": item["fields"], **({"expected": expected} if expected else {})}
            for item in items]


def write(node, items, verb="create"):
    reply = node.call("/api/mutations/batch", {"mutations": mutations(items, verb)})[0]
    identities = reply.get("mutation_ids")
    require(reply.get("durability") == "durable" and reply.get("count") == len(items)
            and isinstance(identities, list) and len(identities) == len(items)
            and all(isinstance(value, str) and value for value in identities)
            and not reply.get("errors"), "production batch lacks complete durable acknowledgements")
    return identities


def read(node, items, absent=()):
    queries = [{"schema_name": item["schema"], "fields": list(item["fields"]), "limit": 2,
                "filter": {"HashRangeKeys": [[item["key"]["hash"], item["key"]["range"]]]}
                if "range" in item["key"] else {"HashKey": item["key"]["hash"]}}
               for item in [*items, *absent]]
    replies = node.call("/api/queries/batch", {"queries": queries})[0]["results"]
    require(len(replies) == len(queries), "keyed read batch is incomplete")
    for index, (item, reply) in enumerate(zip([*items, *absent], replies)):
        value = reply["response"]
        require(reply["status"] == 200 and value["ok"] is True and not value.get("has_more")
                and value.get("unresolved_rows", 0) == 0, "keyed read returned incomplete data")
        rows = value["results"]
        if index >= len(items):
            require(value["returned_count"] == 0 and not rows, "refused prefix became visible")
        else:
            key = {"hash": item["key"]["hash"], "range": item["key"].get("range")}
            require(value["returned_count"] == 1 and len(rows) == 1
                    and rows[0]["key"] == key and rows[0]["fields"] == item["fields"],
                    "an acknowledged production value changed or disappeared")


def refusal(node, batch, token):
    connection = UnixConnection(node.socket)
    try:
        connection.request("POST", "/api/mutations/batch", json.dumps({"mutations": batch}),
                           {"Content-Type": "application/json", "X-LastDB-Client": "logical-capture-ci"})
        response = connection.getresponse()
        raw = response.read(1024 * 1024 + 1)
        require(len(raw) <= 1024 * 1024 and response.status in (400, 409)
                and token.lower() in raw.decode(errors="replace").lower(),
                "the requested production refusal returned an unrelated result")
        return {"http_status": response.status, "reason_token": token}
    finally:
        connection.close()


def observed_resources(node):
    memory = {}
    path = Path(f"/proc/{node.process.pid}/status")
    if path.exists():
        for line in path.read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:")):
                name, amount, _ = line.split()
                memory[name.rstrip(":")] = int(amount) * 1024
        require(memory and max(memory.values()) <= MEMORY_LIMIT,
                "the observed process memory exceeds the operation safety bound")
    disk = node.disk_usage()
    require(disk["allocated_bytes"] <= DISK_LIMIT,
            "the observed owned-home disk size exceeds the operation safety bound")
    return {"memory": memory or {"available": False}, "disk": disk,
            "memory_safety_limit_bytes": MEMORY_LIMIT, "disk_safety_limit_bytes": DISK_LIMIT,
            "scope": "checkpoint samples; not a universal peak or process memory guarantee"}


def require_ids(status, acknowledged):
    events = [set(event["mutation_ids"]) for event in status["recent"]]
    captured = [identity for event in status["recent"] for identity in event["mutation_ids"]]
    require(len(captured) == len(set(captured)), "a mutation identity appears in two capture events")
    returned = [identity for batch in acknowledged for identity in batch["mutation_ids"]]
    require(len(returned) == len(set(returned)), "durable acknowledgements repeat a mutation identity")
    for batch in acknowledged:
        require(sum(set(batch["mutation_ids"]) == event for event in events) == 1,
                "an acknowledged batch lacks one capture event with exactly its mutation identities")


def prepare(node, result):
    initial = capture(node)
    require(initial["position"]["sequence"] == 0 and not initial["recent"],
            "a fresh operation starts with captured user mutations")
    anchor = initial["position"]["sha256"]
    result["initial_capture"] = initial
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda value: node.call("/api/schemas/seed-system", value),
                      [schema(HASH_SCHEMA, False), schema(RANGE_SCHEMA, True)]))
    hashed, ranged = records(0, False), records(0)
    batches = [hashed, ranged]
    with ThreadPoolExecutor(max_workers=2) as pool:
        identifiers = list(pool.map(lambda items: write(node, items), batches))
    acknowledged = [{"mutation_ids": ids} for ids in identifiers]
    updated = [{**item, "fields": {**item["fields"], "body": "updated-durable-value"}}
               for item in ranged]
    ids = write(node, updated, "update")
    acknowledged.append({"mutation_ids": ids})
    before = capture(node, anchor)
    require(before["position"]["sequence"] == 3, "the three production batches lack three capture events")
    repeated = write(node, updated, "update")
    require(set(repeated).isdisjoint(ids), "a new HTTP write reused an original mutation identity")
    acknowledged.append({"mutation_ids": repeated})
    after = capture(node, anchor)
    require(after["position"]["sequence"] == before["position"]["sequence"] + 1
            and set(after["recent"][-1]["mutation_ids"]) == set(repeated),
            "a repeated HTTP write lacks its newly authored capture event")
    result["repeated_http_content"] = {
        "prior_mutation_ids": ids, "new_mutation_ids": repeated,
        "new_event": True, "note": "This endpoint authors a new clock and identity; it exposes no idempotent retry token."}
    before = after
    refused = [{**item, "fields": {**item["fields"], "body": "refused-precondition-value"}}
               for item in updated]
    result["failed_precondition"] = refusal(node, mutations(refused, "update", {
        "type": "value", "field": "body", "value": "not-the-current-value"}), "conflict")
    require(capture(node, anchor) == before, "failed precondition changed the capture head")
    read(node, [*hashed, *updated])
    require_ids(before, acknowledged)
    result["before_crash"] = before
    return anchor, acknowledged, [*hashed, *updated]


def kill_window(node, acknowledged, live, result):
    pending, stop, lock = threading.Event(), threading.Event(), threading.Lock()
    state = {"acknowledged": [], "live": [], "errors": [], "attempted": []}

    def writer():
        for number in range(1, MAX_CRASH_BATCHES + 1):
            if stop.is_set():
                return
            items = records(number)
            with lock:
                state["attempted"] = items
            pending.set()
            try:
                identities = write(node, items)
                with lock:
                    state["acknowledged"].append({"mutation_ids": identities})
                    state["live"].extend(items)
            except Exception as error:
                with lock:
                    state["errors"].append({"kind": type(error).__name__, "message": str(error)[:300],
                                            "transport": isinstance(error, (OSError, http.client.HTTPException))})
                return
            finally:
                pending.clear()

    thread = threading.Thread(target=writer, name="logical-capture-writer", daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with lock:
                ready = len(state["acknowledged"]) >= 2
                errors = list(state["errors"])
            require(not errors, f"writer failed before the deliberate kill: {errors}")
            if ready and pending.is_set() and thread.is_alive():
                break
            require(thread.is_alive(), "bounded writer finished without an active interruption window")
            time.sleep(0.0005)
        else:
            raise RuntimeError("no active production-write interruption window")
        process = node.process
        require(process.poll() is None and pending.is_set(), "the owned writer is not active")
        process.kill()
        process.wait(timeout=10)
        require(process.returncode == -signal.SIGKILL, "the owned daemon did not stop through SIGKILL")
        node.process = None
        node.log.close()
        node.log = None
        result["interruption"] = {"signal": "SIGKILL", "exit_code": process.returncode,
                                  "pending_client_write_observed": True, "pre_crash_flush": False}
    finally:
        stop.set()
        thread.join(timeout=65)
        require(not thread.is_alive(), "the owned socket writer did not stop")
    with lock:
        require(all(error["transport"] for error in state["errors"]),
                "writer returned a non-transport failure around the deliberate kill")
        acknowledged.extend(state["acknowledged"])
        live.extend(state["live"])
        result["transport_errors_after_kill"] = state["errors"]
        known = {(item["schema"], tuple(item["key"].items())) for item in live}
        return [item for item in state["attempted"]
                if (item["schema"], tuple(item["key"].items())) not in known]


def reopen_and_refuse(node, anchor, acknowledged, live, uncertain, result):
    node.environment.pop("LASTDB_LOGICAL_CAPTURE")
    node.start()
    first = capture(node, anchor)
    require(first["position"]["origin"] == result["initial_capture"]["position"]["origin"],
            "reopen changed the persisted device identity")
    extra = first["position"]["sequence"] - len(acknowledged)
    require(extra in (0, 1) and (not extra or len(uncertain) == 2),
            "recovered capture contains more than one unacknowledged batch")
    require_ids(first, acknowledged)
    read(node, live)
    if uncertain:
        read(node, uncertain if extra else [], [] if extra else uncertain)
    result["unacknowledged_batch_committed"] = bool(extra)
    result["first_reopen"] = first
    result["resources_after_recovery"] = observed_resources(node)
    node.stop()
    result["clean_shutdowns"] = [json.loads((node.output / "shutdown.json").read_text())]
    node.start()
    second = capture(node, anchor)
    require(second == first, "a second reopen changed the capture position or entries")
    read(node, live)
    if uncertain:
        read(node, uncertain if extra else [], [] if extra else uncertain)
    prefix = records(900)
    mixed = [mutations(prefix)[0], mutations([live[0]], "delete")[0]]
    result["unsupported_mixed_request"] = refusal(node, mixed, "logical capture v1")
    require(capture(node, anchor) == second, "unsupported mixed request changed the capture position")
    read(node, live, [prefix[0]])
    result["second_reopen"] = second
    result["acknowledged_mutation_ids"] = [identity for batch in acknowledged for identity in batch["mutation_ids"]]
    result["acknowledged_records_read"] = len(live)
    result["resources_final"] = observed_resources(node)
    node.stop()
    result["clean_shutdowns"].append(json.loads((node.output / "shutdown.json").read_text()))


def run(candidate, output):
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "logical capture operation requires hosted CI")
    output.mkdir(mode=0o700)
    result = {"source_revision": candidate[1], "binary_sha256": file_hash(candidate[0]),
              "driver_sha256": file_hash(Path(__file__)), "complete": False,
              "primary_access": False, "cloud_access": False,
              "verification_scope": "capture metadata and keyed values",
              "original_event_signature_export_operated": False,
              "max_crash_batches": MAX_CRASH_BATCHES, "records_per_batch": 2}
    node = Node(candidate[0], candidate[1], output, environment={
        "LASTDB_LOGICAL_CAPTURE": "1", "LASTDB_SELF_METRICS_TO_DB": "false",
        "LASTDB_TTL_SWEEP": "0", "LASTDB_BYTE_BACKUP_V2_ENABLED": "false",
        "EXEMEM_API_URL": "http://127.0.0.1:9", "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9"})
    try:
        node.start()
        require(node.disk_format() == 2, "the fresh production home is not V2")
        anchor, acknowledged, live = prepare(node, result)
        result["resources_before_crash"] = observed_resources(node)
        uncertain = kill_window(node, acknowledged, live, result)
        reopen_and_refuse(node, anchor, acknowledged, live, uncertain, result)
        result["complete"] = True
    except Exception as error:
        result["error"] = str(error)[:1200]
    finally:
        try:
            node.close()
            result["owned_cleanup"] = json.loads((output / "cleanup.json").read_text())
        except Exception as error:
            result["cleanup_error"] = str(error)[:1200]
            result["complete"] = False
        save(output / "operation.json", result)
    return result


def summary(result):
    return "\n".join([
        "## Production logical capture and SIGKILL", "",
        f"Result: {'PASS' if result['complete'] else 'FAIL'}. Source: `{result['source_revision']}`.",
        "Real production mutations on a fresh V2 home; no primary or cloud access.",
        "The operation checks durable mutation IDs, keyed values, sequence/hash links, repeated content, and refused writes.",
        "Verification covers capture metadata and keyed values; the operation does not export original events or verify their signatures.",
        "Repeated HTTP content receives a new author clock and event; this endpoint supplies no idempotent retry guarantee.",
        "The operation requires a deliberate kill with a client write pending and no pre-crash flush.",
        "Two normal reopens must preserve acknowledged values and captured mutation IDs.",
        "Checkpoint memory and disk samples do not establish universal peaks or power-loss behavior.",
        f"Detail: {result.get('error', 'complete')}", ""])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run((args.candidate.resolve(strict=True), args.candidate_revision), args.output)
    print(summary(result), flush=True)
    return 0 if result["complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
