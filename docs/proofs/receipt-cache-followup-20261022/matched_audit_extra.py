"""Independent focused and erase arithmetic for matched-capture evidence."""

import hashlib
import json

from matched_audit_metrics import (COUNTERS, ERASE_SPECS, ack_sequence, compact_interval, dimensions,
                                   equal, expected_request, overlaps, phase_base, require, subset, timing)


def state_at(raw, cutoff):
    state = {}
    for row in sorted(raw, key=lambda item: item["started_monotonic_ns"]):
        if row["path"] != "/api/mutations/batch" or row["ended_monotonic_ns"] > cutoff:
            continue
        for mutation in row["request"]["mutations"]:
            key, operation = mutation["key_value"]["range"], mutation["mutation_type"]
            if operation == "delete":
                require(key in state, "An erase request targets an absent generated record")
                del state[key]
            elif operation == "create":
                require(key not in state, "A create request repeats a generated record")
                state[key] = dict(mutation["fields_and_values"])
            else:
                require(operation == "update" and key in state, "An update lacks an existing generated record")
                state[key].update(mutation["fields_and_values"])
    return state


def state_digest(state):
    return hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()


def focus_role(role, value, raw, compact, dispatched, known_value):
    rows = value["requests_timeline"]
    equal(value["requests"], 64)
    equal([row["ordinal"] for row in rows], list(range(64)))
    indexed = {row["request_id"]: row for row in raw}
    elapsed, lateness, overlapping = [], [], []
    previous = 0
    for ordinal, row in enumerate(rows):
        receipt = indexed[row["request_id"]]
        equal(receipt["phase"], "focused-compaction")
        for name in ("started_monotonic_ns", "ended_monotonic_ns", "elapsed_seconds"):
            equal(row[name], receipt[name], "focused raw timing")
        equal(row["role"], role)
        nominal = dispatched + int((.02 + .05 * ordinal) * 1e9)
        equal(row["nominal_start_monotonic_ns"], nominal)
        require(previous <= row["started_monotonic_ns"] and row["started_monotonic_ns"] >= nominal,
                "Focused sequential stream is out of order or starts before its nominal time")
        previous = row["ended_monotonic_ns"]
        late = (row["started_monotonic_ns"] - nominal) / 1e9
        equal(row["start_lateness_seconds"], late)
        overlap = overlaps(receipt, compact)
        equal(row["request_intervals_overlap"], overlap)
        if role == "durable_write":
            equal(receipt["request"], expected_request([400], 7000 + ordinal))
        else:
            equal(receipt["path"], "/api/query")
            response = receipt["response"].get("data", receipt["response"])
            equal(response["returned_count"], 1)
            require(len(response["results"]) == 1, "Focused read lacks exactly one returned record")
            equal(response["results"][0]["key"], {"hash": "measure", "range": "key-000001"})
            equal(response["results"][0]["fields"], known_value)
        elapsed.append(receipt["elapsed_seconds"])
        lateness.append(late)
        if overlap:
            overlapping.append(receipt["elapsed_seconds"])
    all_ack, late_stats = timing(elapsed, True), timing(lateness, True)
    overlap_ack = timing(overlapping, True) if overlapping else None
    subset(value, {"overlap_count": len(overlapping), "all_ack": all_ack,
                   "overlap_ack": overlap_ack, "start_lateness": late_stats})
    result = {"overlap_count": len(overlapping)}
    for prefix, stats in (("all_ack", all_ack), ("lateness", late_stats), ("overlap_ack", overlap_ack)):
        if stats:
            result.update({prefix + "_" + key: stats[key] for key in
                           ("samples", "mean_ms", "median_ms", "p95_ms", "max_ms")})
    return result


def focused(value, checkpoints, raw):
    subset(value, {"all_values_verified": True, "graceful_reopen_verified": True,
                   "nominal_first_delay_seconds": .02, "nominal_interval_seconds": .05})
    compact = compact_interval(value, raw, True)
    _, physical, _ = phase_base(checkpoints["focused-compact-before"], checkpoints["focused-compact-after"], value, raw)
    writes = sorted((row for row in raw if row["phase"] == "focused-compaction"
                     and row["path"] == "/api/mutations/batch"), key=lambda row: row["started_monotonic_ns"])
    equal(value["write_inputs"], dimensions(writes))
    initial = state_at(raw, value["dispatched_monotonic_ns"])
    final = state_at(raw, value["ended_monotonic_ns"])
    equal(value["expected_values_sha256"], state_digest(final))
    require(len(initial) == len(final) == 512, "Focused phase changes the known record count")
    equal(set(value["probes"]), {"warm_read", "durable_write"})
    return {"write_inputs": dimensions(writes), "expected_values_sha256": state_digest(final),
            "compact_wall_seconds": compact["elapsed_seconds"], "physical_cost": physical,
            "roles": {role: focus_role(role, probes, raw, compact, value["dispatched_monotonic_ns"], initial["key-000001"])
                      for role, probes in value["probes"].items()}}


def erased(result, checkpoints, raw):
    subset(result, {"complete": True, "records_remaining": 319,
                    "all_values_verified": True, "graceful_reopen_verified": True})
    equal([phase["label"] for phase in result["phases"]], [spec[0] for spec in ERASE_SPECS])
    phases = {}
    for phase, (name, first, count, width) in zip(result["phases"], ERASE_SPECS):
        requests = sorted((row for row in raw if row["phase"] == name and row["path"] == "/api/mutations/batch"),
                          key=lambda row: row["started_monotonic_ns"])
        require(len(requests) == count // width, "Erase request count differs")
        for ordinal, row in enumerate(requests):
            equal(row["request"], expected_request(range(first + ordinal * width, first + (ordinal + 1) * width), 0,
                                                   operation="delete"))
        timeline = [{key: row[key] for key in ("request_id", "started_monotonic_ns", "ended_monotonic_ns", "elapsed_seconds")}
                    for row in requests]
        equal(phase["request_timeline"], timeline)
        initial, final = state_at(raw, phase["started_monotonic_ns"]), state_at(raw, phase["ended_monotonic_ns"])
        subset(phase, {"width": width, "requested_durability": "durable", "records_before": len(initial),
                       "records_after": len(final), "records_erased": count, "all_values_verified": True})
        require(len(initial) - len(final) == count, "Erase survivor count differs from actual delete requests")
        inputs = dimensions(requests)
        source = [initial[f"key-{number:06d}"] for number in range(first, first + count)]
        inputs.update(first_key=first, last_key=first + count - 1,
                      erased_source_fields_json_bytes=sum(len(json.dumps(row, separators=(",", ":")).encode()) for row in source),
                      erased_source_body_bytes=sum(len(row["body"].encode()) for row in source))
        equal(phase["inputs"], inputs)
        stats = timing([row["elapsed_seconds"] for row in requests], True)
        equal(phase["request_ack"], stats)
        base, metrics, flush = phase_base(checkpoints[name + "-before"], checkpoints[name + "-after"], phase, raw)
        ack_sequence(phase, requests, flush)
        metrics.update({"ack_" + key: val for key, val in stats.items()})
        for key in ("wall_through_final_flush_seconds", "ack_sequence_seconds", "final_explicit_flush_seconds",
                    "generation_delta", "frontier_delta", "accepted_sequence_delta", "completed_sequence_delta"):
            metrics[key] = phase[key]
        metrics.update({"process_" + key: base["process_" + key] for key in COUNTERS})
        for key in ("logical_bytes_net", "allocated_bytes_net", "new_or_grown_logical_bytes", "new_or_grown_allocated_bytes",
                    "process_ri_diskio_bytesread", "process_ri_diskio_byteswritten", "process_ri_logical_writes",
                    "wall_through_final_flush_seconds"):
            metrics[key + "_per_erased_record"] = metrics[key] / count
        expected_hash = state_digest(final)
        equal(phase["expected_values_sha256"], expected_hash)
        if width == 129:
            subset(phase, {"client_requests": 1, "expected_internal_erase_chunks": 3,
                           "batch_ack_seconds": requests[0]["elapsed_seconds"]})
        phases[name] = {"inputs": inputs, "metrics": metrics, "expected_values_sha256": expected_hash}
    equal(result["expected_values_sha256"], expected_hash)
    require(len(final) == 319, "Final erase state does not contain 319 records")
    return {"phases": phases, "expected_values_sha256": expected_hash}
