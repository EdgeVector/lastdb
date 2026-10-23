"""Independent arithmetic for the fixed matched-capture evidence audit."""

import hashlib
import json
import math
import statistics

COUNTERS = ("ri_diskio_bytesread", "ri_diskio_byteswritten", "ri_logical_writes",
            "ri_user_time", "ri_system_time")
MEMORY = ("ri_resident_size", "ri_phys_footprint", "ri_lifetime_max_phys_footprint")
IDENTITY = ("pid", "ri_proc_start_abstime", "executable_uuid")
NATIVE = ("generation", "frontier", "accepted_sequence", "completed_sequence")
WRITE_SPECS = (("durable-single-64", 64, 1, 0, "durable", 1000),
               ("durable-batch32-16", 16, 32, 0, "durable", 2000),
               ("queued-single-256", 256, 1, 128, "queued", 3000),
               ("queued-batch32-8", 8, 32, 0, "queued", 4000),
               ("durable-single-after-compact-24", 24, 1, 450, "durable", 6000))
ERASE_SPECS = (("erase-durable-single-64", 0, 64, 1),
               ("erase-durable-batch129", 64, 129, 129))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def equal(actual, expected, label="value"):
    if isinstance(expected, dict):
        require(isinstance(actual, dict) and set(actual) == set(expected), label + ": object keys differ")
        for key, value in expected.items():
            equal(actual[key], value, label + "." + str(key))
    elif isinstance(expected, (list, tuple)):
        require(isinstance(actual, (list, tuple)) and len(actual) == len(expected), label + ": lengths differ")
        for index, (first, last) in enumerate(zip(actual, expected)):
            equal(first, last, f"{label}[{index}]")
    elif isinstance(expected, (int, float)) and not isinstance(expected, bool):
        require(isinstance(actual, (int, float)) and not isinstance(actual, bool)
                and math.isfinite(actual) and math.isfinite(expected), label + ": invalid number")
        require(actual == expected if isinstance(expected, int) else
                math.isclose(actual, expected, rel_tol=1e-10, abs_tol=1e-9), label + ": number differs")
    else:
        require(type(actual) is type(expected) and actual == expected, label + ": value differs")


def subset(actual, expected, label="value"):
    equal({key: actual[key] for key in expected}, expected, label)


def timing(values, extended=False):
    ordered = sorted(values)
    require(bool(ordered) and all(math.isfinite(value) and value >= 0 for value in ordered),
            "Timing samples are empty, negative, or nonfinite")
    result = {"samples": len(ordered), "median_ms": statistics.median(ordered) * 1000,
              "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1] * 1000,
              "max_ms": ordered[-1] * 1000}
    if extended:
        result.update(mean_ms=statistics.mean(ordered) * 1000, min_ms=ordered[0] * 1000)
    return result


def distribution(values):
    require(len(values) == 3 and all(math.isfinite(value) for value in values), "Expected three finite run values")
    return {"values": values, "median": statistics.median(values), "min": min(values), "max": max(values)}


def paired(pairs):
    before, after = [row[0] for row in pairs], [row[1] for row in pairs]
    result = {"before": distribution(before), "after": distribution(after),
              "after_minus_before": distribution([last - first for first, last in pairs])}
    if all(first > 0 and last >= 0 for first, last in pairs):
        result["reduction_percent"] = distribution([100 - 100 * last / first for first, last in pairs])
        if all(last > 0 for last in after):
            result["before_over_after_ratio"] = distribution([first / last for first, last in pairs])
    return result


def dimensions(requests):
    result = dict.fromkeys(("requests", "logical_mutations", "changed_field_values", "request_json_bytes",
                           "fields_json_bytes", "body_value_bytes", "title_value_bytes"), 0)
    digest = hashlib.sha256()
    for row in requests:
        body = row["request"]
        digest.update((json.dumps(body, sort_keys=True, separators=(",", ":")) + "\n").encode())
        result["requests"] += 1
        result["request_json_bytes"] += len(json.dumps(body, separators=(",", ":")).encode())
        for mutation in body["mutations"]:
            fields = mutation["fields_and_values"]
            result["logical_mutations"] += 1
            result["changed_field_values"] += len(fields)
            result["fields_json_bytes"] += len(json.dumps(fields, separators=(",", ":")).encode())
            result["body_value_bytes"] += len(fields.get("body", "").encode())
            result["title_value_bytes"] += len(fields.get("title", "").encode())
    result["ordered_input_sha256"] = digest.hexdigest()
    return result


def expected_request(numbers, revision, durability="durable", operation="update"):
    changes = []
    for number in numbers:
        fields = {} if operation == "delete" else {
            "title": f"title-{number}-revision-{revision}",
            "body": "".join(hashlib.sha256(f"owned-disk:{number}:{revision}:{part}".encode()).hexdigest()
                            for part in range(16))}
        if operation == "create":
            fields.update(bucket="measure", id=f"key-{number:06d}")
        changes.append({"type": "mutation", "schema": "DiskGrowthMeasure", "mutation_type": operation,
                        "durability": durability, "key_value": {"hash": "measure", "range": f"key-{number:06d}"},
                        "fields_and_values": fields})
    return {"mutations": changes}


def inventory(value):
    require(len(value["_files"]) <= 20000, "Inventory exceeds its file bound")
    families = {}
    for name, sizes in value["_files"].items():
        require(not name.startswith("/") and ".." not in name.split("/") and len(sizes) == 2
                and all(isinstance(size, int) and size >= 0 for size in sizes), "Invalid file metadata")
        family = name.split("/")[0] if "/" in name else "root-markers"
        target = families.setdefault(family, {"files": 0, "bytes": 0, "allocated_bytes": 0})
        target["files"] += 1
        target["bytes"] += sizes[0]
        target["allocated_bytes"] += sizes[1]
    equal(value["families"], families, "inventory families")
    for name in ("files", "bytes", "allocated_bytes"):
        equal(value[name], sum(family[name] for family in families.values()), "inventory " + name)


def cost(before, after, phase):
    for point in (before, after):
        native = point["native"]
        require(native["writer_failed"] is False and not native["accepted_batches"]
                and not native["core_warm_dirty"] and native["accepted_sequence"] == native["completed_sequence"]
                and native["parts"]["auto_maintenance"] is False, "Invalid controlled phase boundary")
        inventory(point["inventory"])
    equal([before["process"][key] for key in IDENTITY], [after["process"][key] for key in IDENTITY], "phase identity")
    deltas = {key: after["process"][key] - before["process"][key] for key in COUNTERS}
    require(all(value >= 0 for value in deltas.values()), "Process counters decrease within a phase")
    process = {"cumulative_counter_delta": deltas,
               "process_lifetime_max_memory_bytes": after["process"]["ri_lifetime_max_phys_footprint"]}
    for when, point in (("before", before), ("after", after)):
        process[f"memory_{when}_bytes"] = {key: point["process"][key] for key in MEMORY[:2]}
        equal(phase["inventory_" + when], {key: val for key, val in point["inventory"].items() if key != "_files"})
    equal(phase["process"], process, "phase process cost")
    old, new = before["inventory"]["_files"], after["inventory"]["_files"]
    result = {"logical_bytes_net": after["inventory"]["bytes"] - before["inventory"]["bytes"],
              "allocated_bytes_net": after["inventory"]["allocated_bytes"] - before["inventory"]["allocated_bytes"],
              "files_net": len(new) - len(old), "new_path_files": len(set(new) - set(old)),
              "new_or_grown_logical_bytes": sum(max(0, sizes[0] - old.get(name, [0, 0])[0]) for name, sizes in new.items()),
              "new_or_grown_allocated_bytes": sum(max(0, sizes[1] - old.get(name, [0, 0])[1]) for name, sizes in new.items())}
    grown = {}
    for name, sizes in new.items():
        extra = max(0, sizes[0] - old.get(name, [0, 0])[0])
        if extra:
            family = name.split("/")[0] if "/" in name else "root-markers"
            target = grown.setdefault(family, {"bytes": 0, "files": 0})
            target["bytes"] += extra
            target["files"] += int(name not in old)
    equal(phase["growth"], {"files_net": result["files_net"], "bytes_net": result["logical_bytes_net"],
                           "allocated_bytes_net": result["allocated_bytes_net"], "new_or_grown": grown}, "phase growth")
    return result, process


def phase_base(before, after, phase, raw):
    physical, process = cost(before, after, phase)
    metrics = dict(physical)
    for key in NATIVE:
        metrics[key + "_delta"] = after["native"][key] - before["native"][key]
        equal(phase[key + "_delta"], metrics[key + "_delta"], "native delta")
    start = phase.get("started_monotonic_ns", phase.get("dispatched_monotonic_ns"))
    end = phase["ended_monotonic_ns"]
    require(before["process"]["sampled_monotonic_ns"] <= start <= end
            <= after["process"]["sampled_monotonic_ns"], "Phase leaves its captured process boundaries")
    equal(phase["wall_through_final_flush_seconds"], (end - start) / 1e9, "phase wall time")
    flushes = [row for row in raw if row["path"] == "/api/db/flush" and start <= row["started_monotonic_ns"]
               and row["ended_monotonic_ns"] <= end]
    require(len(flushes) == 1, "Expected exactly one timed phase flush")
    flush = flushes[0]
    equal(phase["final_explicit_flush_seconds"], flush["elapsed_seconds"], "flush time")
    equal(phase["final_explicit_flush"], unwrap(flush), "flush response")
    require(unwrap(flush)["durable"] is True, "Final flush lacks durable completion")
    metrics.update(durable_completion_wall_seconds=(end - start) / 1e9,
                   final_explicit_flush_seconds=flush["elapsed_seconds"],
                   process_lifetime_peak_memory_bytes=process["process_lifetime_max_memory_bytes"])
    metrics.update({"process_" + key: value for key, value in process["cumulative_counter_delta"].items()})
    for when in ("before", "after"):
        metrics.update({f"memory_{when}_{key}": value for key, value in process[f"memory_{when}_bytes"].items()})
    return metrics, physical, flush


def unwrap(row):
    return row["response"].get("data", row["response"])


def ack_sequence(phase, requests, flush):
    value = phase["ack_sequence_seconds"]
    start = phase["started_monotonic_ns"]
    require(start <= requests[0]["started_monotonic_ns"] and all(
        first["ended_monotonic_ns"] <= last["started_monotonic_ns"] for first, last in zip(requests, requests[1:])),
        "Sequential mutation requests overlap or precede their phase")
    require((requests[-1]["ended_monotonic_ns"] - start) / 1e9 <= value
            <= (flush["started_monotonic_ns"] - start) / 1e9, "Ack sequence falls outside raw completion bounds")


def write_metrics(phase, before, after, raw):
    name, count, width, offset, durability, revision = next(spec for spec in WRITE_SPECS if spec[0] == phase["label"])
    requests = [row for row in raw if row["phase"] == name and row["path"] == "/api/mutations/batch"]
    require(len(requests) == count, "Matched write request count differs")
    for ordinal, row in enumerate(requests):
        numbers = [(offset + ordinal * width + item) % 512 for item in range(width)]
        equal(row["request"], expected_request(numbers, revision + ordinal, durability), name + " request")
    equal(phase["inputs"], dimensions(requests), name + " inputs")
    equal(phase["request_ack"], timing([row["elapsed_seconds"] for row in requests]), name + " ack")
    subset(phase, {"requested_durability": durability, "width": width})
    metrics, _, flush = phase_base(before, after, phase, raw)
    ack_sequence(phase, requests, flush)
    samples = [row["elapsed_seconds"] for row in requests]
    values = timing(samples, True)
    metrics.update(ack_mean_ms=values["mean_ms"], ack_p50_ms=values["median_ms"], ack_p95_ms=values["p95_ms"],
                   ack_max_ms=values["max_ms"], ack_sequence_seconds=phase["ack_sequence_seconds"])
    records, commits = count * width, metrics["frontier_delta"]
    require(commits > 0, "Write phase lacks a committed frontier change")
    metrics.update(logical_mutations=records, requests=count, requests_per_commit=count / commits,
                   accepted_native_requests_per_commit=metrics["accepted_sequence_delta"] / commits,
                   generations_per_commit=metrics["generation_delta"] / commits, logical_mutations_per_commit=records / commits,
                   durable_completion_seconds_per_record=metrics["durable_completion_wall_seconds"] / records,
                   process_write_bytes_per_commit=metrics["process_ri_diskio_byteswritten"] / commits)
    for key in ("logical_bytes_net", "allocated_bytes_net", "new_path_files", "files_net", "new_or_grown_logical_bytes",
                "new_or_grown_allocated_bytes", "process_ri_diskio_byteswritten", "process_ri_diskio_bytesread", "process_ri_logical_writes"):
        metrics[key + "_per_record"] = metrics[key] / records
    return metrics


def compact_interval(phase, raw, focused=False):
    name = "focused-compaction" if focused else "compaction"
    requests = [row for row in raw if row["phase"] == name and row["path"] == "/api/db/compact"]
    require(len(requests) == 1, "Expected one compaction request")
    compact = requests[0]
    prefix = "compact" if focused else "compact_request"
    equal(phase[prefix + "_started_monotonic_ns"], compact["started_monotonic_ns"])
    equal(phase[prefix + "_ended_monotonic_ns"], compact["ended_monotonic_ns"])
    equal(phase["compact_wall_seconds"], compact["elapsed_seconds"])
    equal(phase["report"], unwrap(compact), "compaction response")
    equal(compact["request"], {"dry_run": False, "grace_ms": 0})
    baseline = [row for row in raw if row["phase"] == name and row["path"] == "/api/query"][:8]
    require(len(baseline) == 8 and all(row["ended_monotonic_ns"] <= compact["started_monotonic_ns"] for row in baseline),
            "Compaction baseline lacks eight preceding read requests")
    equal(phase["warm_read_baseline"], timing([row["elapsed_seconds"] for row in baseline]))
    return compact


def overlaps(first, second):
    return max(first["started_monotonic_ns"], second["started_monotonic_ns"]) < min(
        first["ended_monotonic_ns"], second["ended_monotonic_ns"])


def compact_metrics(phase, before, after, raw):
    metrics, _, _ = phase_base(before, after, phase, raw)
    compact = compact_interval(phase, raw)
    report = unwrap(compact)["compact"]["native_disk"]
    metrics["compact_wall_seconds"] = compact["elapsed_seconds"]
    for key in ("pack_millis", "reclaim_millis", "records_copied", "tombstones_copied", "bytes_removed", "files_removed"):
        metrics[key] = report[key]
    for percentile, source in (("p50", "median_ms"), ("p95", "p95_ms")):
        metrics[f"warm_read_baseline_{percentile}_ms"] = phase["warm_read_baseline"][source]
    equal(set(phase["probes"]), {"warm_read", "durable_write"})
    for role, probe in phase["probes"].items():
        candidates = [row for row in raw if row["phase"] == "compaction"
                      and row["started_monotonic_ns"] == probe["started_monotonic_ns"]]
        require(len(candidates) == 1, "Compaction probe lacks a unique raw request")
        request = candidates[0]
        subset(probe, {"ack_seconds": request["elapsed_seconds"], "ended_monotonic_ns": request["ended_monotonic_ns"],
                       "start_after_dispatch_seconds": (request["started_monotonic_ns"] - phase["started_monotonic_ns"]) / 1e9,
                       "request_intervals_overlap": overlaps(request, compact)})
        if role == "durable_write":
            equal(request["request"], expected_request([400], 5000))
            equal(phase["probe_write_inputs"], dimensions([request]))
            equal(phase["write_reply"], unwrap(request))
        else:
            equal(request["path"], "/api/query")
            equal(request["request"], {"schema_name": "DiskGrowthMeasure", "fields": ["bucket", "id", "title", "body"],
                                       "filter": {"HashRangeKeys": [["measure", "key-000001"]]}, "limit": 1})
        metrics[role + "_probe_ack_seconds"] = request["elapsed_seconds"]
    return metrics
