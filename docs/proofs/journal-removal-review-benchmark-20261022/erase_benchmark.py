"""A separate bounded erase workload after matched writes and focused compaction."""

import hashlib
import json
import math
from pathlib import Path
import sys
import time

sys.dont_write_bytecode = True
from reuse import original
from focused import timing_stats
from benchmark_support import FIELDS, SCHEMA, phase_cost, save

SPECS = (("erase-durable-single-64", 0, 64, 1), ("erase-durable-batch129", 64, 129, 129))
SCOPE = "Combined change from the original journal baseline; this does not isolate meter-journal removal"


def digest_expected(capture):
    return hashlib.sha256(json.dumps(capture.expected, sort_keys=True).encode()).hexdigest()


def readback(capture, label):
    # Retain all known keys in the exact request so deleted absence is checked.
    request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 512,
               "filter": {"HashRangeKeys": [["measure", f"key-{n:06d}"] for n in range(512)]}}
    result = capture.call("/api/query", request)
    if capture.rows(result) != capture.expected or result.get("has_more") is not False:
        raise RuntimeError(f"Exact surviving values or deleted absence differ after {label}")
    actual, order, cursor = {}, [], None
    for _ in range(16):
        request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 37,
                   "filter": {"HashRangeRange": {"hash": "measure", "start": "key-000000",
                                                "end": "key-000512"}}}
        if cursor is not None:
            request["cursor"] = cursor
        result = capture.call("/api/query", request)
        values = capture.rows(result)
        if len(values) > 37 or set(values).intersection(actual):
            raise RuntimeError("The erase range repeats a key or exceeds its page bound")
        actual.update(values)
        order.extend(row["key"]["range"] for row in result["results"])
        if not result.get("has_more"):
            break
        next_cursor = result.get("next_cursor")
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError("The erase range cursor cannot advance")
        cursor = next_cursor
    else:
        raise RuntimeError("The erase range exceeds sixteen pages")
    if actual != capture.expected or order != sorted(capture.expected):
        raise RuntimeError(f"Surviving range values, membership, or order differ after {label}")
    capture.remember("erase-readback", label=label, known_keys=512,
                     surviving_records=len(actual), range_page_size=37,
                     expected_values_sha256=digest_expected(capture))


def delete(capture, numbers):
    for number in numbers:
        if f"key-{number:06d}" not in capture.expected:
            raise RuntimeError("The erase input includes an absent generated record")
    request = {"mutations": [{"type": "mutation", "schema": SCHEMA,
                "mutation_type": "delete", "durability": "durable",
                "key_value": {"hash": "measure", "range": f"key-{n:06d}"},
                "fields_and_values": {}} for n in numbers]}
    result, receipt = capture.call_with_receipt("/api/mutations/batch", request)
    if result.get("count") != len(numbers) or result.get("durability") != "durable":
        raise RuntimeError("The erase request lacks its complete durable acknowledgement")
    for number in numbers:
        capture.expected.pop(f"key-{number:06d}")
    return receipt


def phase(capture, spec):
    label, first, count, width = spec
    capture.phase = label
    before = capture.checkpoint(label + "-before")
    records_before = len(capture.expected)
    source_values = [capture.expected[f"key-{n:06d}"] for n in range(first, first + count)]
    source_fields_bytes = sum(len(json.dumps(row, separators=(",", ":")).encode()) for row in source_values)
    source_body_bytes = sum(len(row["body"].encode()) for row in source_values)
    started_unix, started = time.time_ns(), time.monotonic_ns()
    receipts = [delete(capture, list(range(n, n + width))) for n in range(first, first + count, width)]
    last_ack = time.monotonic_ns()
    flush, flush_receipt = capture.flush(label + "-timed-final-flush")
    ended, ended_unix = time.monotonic_ns(), time.time_ns()
    after = capture.checkpoint(label + "-after")
    inputs = original("benchmark").dimensions(receipts)
    inputs.update(first_key=first, last_key=first + count - 1,
                  erased_source_fields_json_bytes=source_fields_bytes,
                  erased_source_body_bytes=source_body_bytes)
    result = {"kind": "erase-phase", "label": label, "width": width, "inputs": inputs,
              "requested_durability": "durable", "records_before": records_before,
              "records_after": len(capture.expected), "records_erased": count,
              "started_unix_ns": started_unix, "ended_unix_ns": ended_unix,
              "started_monotonic_ns": started, "ended_monotonic_ns": ended,
              "ack_sequence_seconds": (last_ack - started) / 1e9,
              "wall_through_final_flush_seconds": (ended - started) / 1e9,
              "request_ack": timing_stats([row["elapsed_seconds"] for row in receipts]),
              "final_explicit_flush": flush, "final_explicit_flush_seconds": flush_receipt["elapsed_seconds"],
              "request_timeline": [{key: row[key] for key in ("request_id", "started_monotonic_ns",
                                    "ended_monotonic_ns", "elapsed_seconds")} for row in receipts],
              "scope": SCOPE, **phase_cost(before, after)}
    if records_before - len(capture.expected) != count:
        raise RuntimeError("The erase phase survivor count differs")
    if width == 129:
        result["client_requests"] = 1
        result["expected_internal_erase_chunks"] = 3
        result["batch_ack_seconds"] = receipts[0]["elapsed_seconds"]
        result["chunk_claim"] = "129 keys cross the 64-key product boundary; no internal chunk timer is measured"
    readback(capture, label)
    result.update(all_values_verified=True, expected_values_sha256=digest_expected(capture))
    capture.remember("erase-phase-summary", summary=result)
    capture.save_receipts()
    save(capture.output / f"{label}.json", result)
    return result


def run(capture):
    if len(capture.expected) != 512:
        raise RuntimeError("The separate erase workload requires the existing 512 records")
    checkpoint_start = len(capture.checkpoints)
    phases = [phase(capture, spec) for spec in SPECS]
    if len(capture.expected) != 319:
        raise RuntimeError("The erase workload must leave 319 live records")
    capture.phase = "erase-reopen-verification"
    capture.stop()
    capture.save_receipts()
    capture.start()
    readback(capture, "erase-graceful-reopen")
    capture.checkpoint("erase-reopened")
    capture.save_receipts()
    result = {"complete": True, "phases": phases, "records_remaining": 319,
              "all_values_verified": True, "graceful_reopen_verified": True,
              "expected_values_sha256": digest_expected(capture), "scope": SCOPE}
    save(capture.output / "erase-checkpoints.json", capture.checkpoints[checkpoint_start:])
    save(capture.output / "erase-results.json", result)
    return {"file": "erase-results.json", "records_remaining": 319,
            "all_values_verified": True, "graceful_reopen_verified": True,
            "matched_measurements_unchanged": True}


def read_result(root, label, pair):
    analyzer = original("summarize")
    folder = root / f"{label}-{pair}"
    summary = analyzer.load(folder / "summary.json")
    result = analyzer.load(folder / "erase-results.json")
    required = ("complete", "all_values_verified", "graceful_reopen_verified")
    if (not all(result.get(key) is True for key in required) or result.get("records_remaining") != 319
            or not summary.get("extra_erase", {}).get("matched_measurements_unchanged")):
        raise ValueError("The erase workload lacks complete values, reopen, or measured-phase separation")
    if [row["label"] for row in result["phases"]] != [spec[0] for spec in SPECS]:
        raise ValueError("The erase phase list differs from the fixed workload")
    checkpoints = {row["label"]: row for row in analyzer.load(folder / "erase-checkpoints.json")}
    phases = {}
    for value, (_, first, count, width) in zip(result["phases"], SPECS):
        name = value["label"]
        before, after = checkpoints[name + "-before"], checkpoints[name + "-after"]
        analyzer.validate_boundary(before, after, value)
        metrics = analyzer.physical_cost(before, after, value)
        samples = [row["elapsed_seconds"] for row in value["request_timeline"]]
        for row in value["request_timeline"]:
            if not math.isclose(row["elapsed_seconds"],
                    (row["ended_monotonic_ns"] - row["started_monotonic_ns"]) / 1e9, abs_tol=1e-12):
                raise ValueError("An erase acknowledgement time differs from its request interval")
        if (len(samples) != count // width or value["request_ack"] != timing_stats(samples)
                or value["records_erased"] != count or value["records_before"] - value["records_after"] != count
                or value["inputs"]["logical_mutations"] != count or value["width"] != width):
            raise ValueError("The erase counts or acknowledgement statistics differ")
        metrics.update({f"ack_{key}": amount for key, amount in value["request_ack"].items()})
        for key in ("wall_through_final_flush_seconds", "ack_sequence_seconds", "final_explicit_flush_seconds",
                    "generation_delta", "frontier_delta", "accepted_sequence_delta", "completed_sequence_delta"):
            metrics[key] = value[key]
        metrics.update({f"process_{key}": amount for key, amount in value["process"]["cumulative_counter_delta"].items()})
        for key in ("logical_bytes_net", "allocated_bytes_net", "new_or_grown_logical_bytes",
                    "new_or_grown_allocated_bytes", "process_ri_diskio_bytesread", "process_ri_diskio_byteswritten",
                    "process_ri_logical_writes", "wall_through_final_flush_seconds"):
            metrics[key + "_per_erased_record"] = metrics[key] / count
        phases[name] = {"inputs": value["inputs"], "metrics": metrics,
                        "expected_values_sha256": value["expected_values_sha256"]}
    return {"phases": phases, "expected_values_sha256": result["expected_values_sha256"]}


def summarize(root):
    analyzer = original("summarize")
    runs = {f"{label}-{number}": read_result(root, label, number)
            for number in (1, 2, 3) for label in ("before", "after")}
    comparisons = {}
    for name, *_ in SPECS:
        pairs = []
        for number in (1, 2, 3):
            first = runs[f"before-{number}"]["phases"][name]
            last = runs[f"after-{number}"]["phases"][name]
            if first["inputs"] != last["inputs"] or first["expected_values_sha256"] != last["expected_values_sha256"]:
                raise ValueError("The paired erase input dimensions or surviving values differ")
            pairs.append((first["metrics"], last["metrics"]))
        comparisons[name] = {key: analyzer.paired([(first[key], last[key]) for first, last in pairs])
                             for key in pairs[0][0]}
    return {"independent_pairs": 3, "requests_pooled": False, "scope": SCOPE,
            "comparisons": comparisons, "runs": runs}


def report(result):
    formatted = original("summarize").formatted
    lines = ["", "Separate durable erase workload", "64 single deletes of keys 0–63, then one 129-delete request for keys 64–192; 319 records remain.",
             "Each phase checks all known keys, deleted absence, and range pages. A graceful reopen checks the final state.", SCOPE + "."]
    for phase_name, metrics in result["comparisons"].items():
        lines.append(phase_name)
        for key in ("ack_median_ms", "ack_p95_ms", "ack_max_ms", "wall_through_final_flush_seconds",
                    "final_explicit_flush_seconds", "logical_bytes_net_per_erased_record",
                    "allocated_bytes_net_per_erased_record", "new_or_grown_allocated_bytes_per_erased_record",
                    "process_ri_diskio_byteswritten_per_erased_record", "process_ri_diskio_bytesread_per_erased_record"):
            value = metrics[key]
            lines.append(f'  {key}: before {formatted(value["before"], 3)}; after {formatted(value["after"], 3)}')
    return "\n".join(lines) + "\n"
