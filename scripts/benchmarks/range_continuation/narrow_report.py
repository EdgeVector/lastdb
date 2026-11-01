"""Per-run narrow latency and exact slow-request observations from CI."""

import json

from process_counters import counter_delta
from ranges import latency


PHASES = ("hydrate_records_us", "hydrate_format_us", "hydrate_sort_us", "annotate_us", "response_envelope_us")
RECEIPT_TIMINGS = ("query_storage_elapsed_nanos", "socket_queue_wait_nanos", "blocking_queue_wait_nanos",
                   "lock_wait_nanos", "read_nanos", "decode_nanos", "decrypt_nanos", "open_nanos", "metadata_nanos")


def narrow_samples_complete(value):
    samples = value.get("narrow_samples", [])
    coverage = value.get("narrow_phase_coverage", {})
    return (len(samples) == 64 and coverage.get("complete") is True
            and coverage.get("matched") == 64 and coverage.get("requests") == 64
            and all("server_request_ops" in sample and "process_counters" in sample for sample in samples))


def percentiles(samples, name):
    values = []
    for sample in samples:
        operation = sample["server_request_ops"]
        source = operation if name == "duration_ms" else operation.get("phases", {})
        if name not in source:
            return "unreported"
        values.append({"elapsed_ms": source[name] / (1 if name == "duration_ms" else 1000)})
    row = latency(values)
    return " / ".join(f"{row[name]:.3f}" for name in ("p50_ms", "p95_ms", "p99_ms"))


def slow_sample(ordinal, sample):
    operation = sample["server_request_ops"]
    receipt = sample["storage_read_receipt"]
    counters = sample["process_counters"]
    delta = counter_delta(counters["before"], counters["after"])
    return {"sample_ordinal": ordinal, "request_id": sample["request_id"], "elapsed_ms": sample["elapsed_ms"],
        "server_duration_ms": operation["duration_ms"], "server_phases_us": operation.get("phases", {}),
        "receipt_timings_ns": {name: receipt[name] for name in RECEIPT_TIMINGS if name in receipt},
        "receipt_work": {name: receipt[name] for name in
            ("requested_keys", "logical_warm_hits", "read_operations", "payload_bytes", "format_probe_bytes")},
        "process_delta": delta,
        "counter_capture_elapsed_ns": {phase: counters[phase].get("capture_elapsed_ns")
                                       for phase in ("before", "after")}}


def narrow_diagnostics(grouped, mode):
    lines = ["", "Narrow requests by run: p50 / p95 / p99 ms. Runs remain separate.", "",
        "| Mode / source / pair | HTTP | Server | Core | Format | Sort | Annotate | Render |",
        "|---|---|---|---|---|---|---|---|"]
    slow = []
    for role, values in grouped.items():
        for value in values:
            if not narrow_samples_complete(value):
                raise RuntimeError("A narrow run lacks64/64 request phases or process observations")
            samples, timing = value["narrow_samples"], value["narrow_latency"]
            label = mode + " / " + role + " / pair " + str(value["pair"])
            http = " / ".join(f"{timing[name]:.3f}" for name in ("p50_ms", "p95_ms", "p99_ms"))
            columns = [http, percentiles(samples, "duration_ms")]
            columns.extend(percentiles(samples, name) for name in PHASES)
            lines.append("| " + label + " | " + " | ".join(columns) + " |")
            ranked = sorted(enumerate(samples), key=lambda row: row[1]["elapsed_ms"], reverse=True)[:5]
            slow.append((label, [slow_sample(ordinal, sample) for ordinal, sample in ranked]))
    lines.extend(["", "Core is hydrate_records. Missing phases remain unreported, not zero.",
        "Five slowest HTTP samples per run follow with their original zero-based sample ordinal.",
        "Each row retains exact request IDs, existing server phases, receipt times, and process counter deltas.",
        "Counter snapshots occur before the HTTP timer and after its end. The interval includes observer work.",
        "Snapshots alter gaps and throughput relative to the old recipe. Both builds use the same revised recipe.",
        "Process CPU has clock-tick precision. Stable thread counters report runtime and run-queue wait.",
        "Host CPU and pressure totals describe shared load; they do not establish the cause of a request delay.",
        "Parent and child phases overlap. Percentiles are separate observations; no percentile subtraction occurs."])
    for label, rows in slow:
        lines.extend(["", label + ":", "```json", json.dumps(rows, sort_keys=True, indent=2), "```"])
    return lines
