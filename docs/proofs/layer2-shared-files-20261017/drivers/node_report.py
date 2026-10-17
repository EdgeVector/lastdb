#!/usr/bin/env python3
"""Render completed real-request receipts without access to a node or data home."""
import argparse
from collections import defaultdict
import json
import math
from pathlib import Path
import statistics


def statistics_ms(values):
    ordered = sorted(value / 1_000_000 for value in values)
    return {
        "samples": len(ordered),
        "median_ms": statistics.median(ordered),
        "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1] if len(ordered) >= 5 else None,
        "minimum_ms": ordered[0],
        "maximum_ms": ordered[-1],
    }


def add_latency(cases, key, variant, metric, value):
    cases[tuple(key) + (metric,)][variant].append(value)


def latency_cases(receipts):
    cases = defaultdict(lambda: defaultdict(list))
    for row in receipts:
        operation, variant = row["operation"], row["variant"]
        if operation == "query":
            key = (operation, row["phase"], row["shape"], row["batch"])
            add_latency(cases, key, variant, "response", row["elapsed_nanos"])
        elif operation == "mutation":
            key = (operation, row["durability"], "", row["batch"])
            add_latency(cases, key, variant, "ack", row["elapsed_nanos"])
            add_latency(cases, key, variant, "explicit_flush", row["flush_elapsed_nanos"])
            add_latency(cases, key, variant, "request_to_explicit_durable", row["request_start_to_durable_nanos"])
        elif operation == "concurrent-queued":
            key = (operation, f'{row["requests"]} requests', "", row["records_per_request"])
            for metric, field in [
                ("all_acks", "all_acks_elapsed_nanos"),
                ("all_durable", "all_durable_elapsed_nanos"),
                ("explicit_flush", "flush_elapsed_nanos"),
                ("client_collection", "client_collection_elapsed_nanos"),
            ]:
                add_latency(cases, key, variant, metric, row[field])
            for request in row["requests_results"]:
                add_latency(cases, key, variant, "individual_ack", request["elapsed_nanos"])
        elif operation == "normal-flush":
            key = (operation, "queued request", "", row["batch"])
            add_latency(cases, key, variant, "ack", row["ack_elapsed_nanos"])
            upper = row["disk_completion_after_ack_upper_nanos"]
            add_latency(cases, key, variant, "disk_after_ack_upper_bound", upper)
            add_latency(cases, key, variant, "request_to_disk_upper_bound", upper + row["ack_elapsed_nanos"])
        elif operation in {"unrelated-mutation", "delete-request", "recreate-request", "queued-delete-boundary"}:
            key = (operation, "", "", row["batch"])
            add_latency(cases, key, variant, "ack", row["elapsed_nanos"])
    comparisons, unmatched, boundaries = [], [], []
    for key, values in sorted(cases.items()):
        case = dict(zip(["operation", "phase", "shape", "batch", "metric"], key))
        case["variants"] = {variant: statistics_ms(samples) for variant, samples in values.items()}
        if set(values) != {"before", "after"}:
            unmatched.append(case)
            continue
        if key[0] == "queued-delete-boundary":
            case["guarantees_differ"] = True
            boundaries.append(case)
            continue
        before, after = (case["variants"][variant]["median_ms"] for variant in ["before", "after"])
        case.update(delta_ms=after - before, delta_percent=(after / before - 1) * 100 if before else None)
        comparisons.append(case)
    return comparisons, unmatched, boundaries


def query_costs(receipts):
    cases = defaultdict(lambda: defaultdict(list))
    for row in receipts:
        if row["operation"] == "query":
            cases[(row["phase"], row["shape"], row["batch"])][row["variant"]].append(row)
    result = []
    for key, variants in sorted(cases.items()):
        case = dict(zip(["phase", "shape", "batch"], key))
        case["variants"] = {}
        for variant, rows in variants.items():
            fields = set().union(*(row["cost"].keys() for row in rows))
            numeric = {}
            for field in sorted(fields):
                values = [row["cost"].get(field) for row in rows]
                if all(isinstance(value, (int, float)) and not isinstance(value, bool) for value in values):
                    numeric[field] = {
                        "median": statistics.median(values), "minimum": min(values), "maximum": max(values),
                        "nonzero_samples": sum(value != 0 for value in values),
                    }
            case["variants"][variant] = {"samples": len(rows), "costs": numeric}
        result.append(case)
    return result


def unwrap(value):
    if isinstance(value, dict) and isinstance(value.get("data"), dict):
        return value["data"]
    return value


def app_storage(value):
    value = unwrap(value)
    return value.get("app_storage", value)


def native_flush(value):
    return unwrap(value).get("native_flush", {})


def queue_status(value):
    fields = ["generation", "frontier", "accepted_batch", "accepted_batches", "accepted_sequence",
              "completed_sequence", "writer_failed"]
    return {field: value[field] for field in fields if field in value}


def concurrent_details(receipts):
    result = []
    for row in receipts:
        if row["operation"] != "concurrent-queued":
            continue
        requests = [{
            "slot": item["slot"], "ack_ms": item["elapsed_nanos"] / 1_000_000,
            "response_after_start_ms": item["end_after_start_nanos"] / 1_000_000,
            "actual_durability": item["actual_durability"],
        } for item in row["requests_results"]]
        start, accepted, end = (row[field] for field in ["start", "accepted_after_all_acks", "end"])
        detail = {
            "variant": row["variant"], "repeat": row["repeat"], "requests": row["requests"],
            "records_per_request": row["records_per_request"], "request_details": requests,
            "all_acks_ms": row["all_acks_elapsed_nanos"] / 1_000_000,
            "all_durable_ms": row["all_durable_elapsed_nanos"] / 1_000_000,
            "flush_ms": row["flush_elapsed_nanos"] / 1_000_000,
            "start": queue_status(start), "after_all_acks": queue_status(accepted), "end": queue_status(end),
            "flush_receipt": native_flush(row["flush"]),
            "frontier_delta": end["frontier"] - start["frontier"],
        }
        receipt = detail["flush_receipt"]
        if "target_sequence" in receipt:
            detail["captured_prefix_complete"] = receipt["completed_sequence"] >= receipt["target_sequence"]
        result.append(detail)
    return result


def normal_flush_details(receipts):
    result = []
    for row in receipts:
        if row["operation"] != "normal-flush":
            continue
        polls = row["polls"]
        final = polls[-1]
        previous = polls[-2] if len(polls) > 1 else None
        result.append({
            "variant": row["variant"], "repeat": row["repeat"],
            "actual_durability": row["actual_durability"],
            "ack_ms": row["ack_elapsed_nanos"] / 1_000_000,
            "disk_after_ack_upper_ms": row["disk_completion_after_ack_upper_nanos"] / 1_000_000,
            "previous_status_response_after_ack_ms": None if previous is None else previous["after_ack_nanos"] / 1_000_000,
            "poll_count": len(polls), "forced_flush_during_sample": row["forced_flush_during_sample"],
            "first_status": queue_status(polls[0]["status"]), "final_status": queue_status(final["status"]),
            "polls": [{"after_ack_ms": item["after_ack_nanos"] / 1_000_000,
                       "status": queue_status(item["status"])} for item in polls],
        })
    return result


def fmt(value):
    return "—" if value is None else f"{value:.3f}"


def case_name(case):
    return " / ".join(str(case[field]) for field in ["operation", "phase", "shape", "batch", "metric"] if case[field] != "")


def comparison_table(cases):
    lines = ["| Case | Before ms | After ms | Change ms | Change % | Samples before/after |",
             "|---|---:|---:|---:|---:|---:|"]
    for case in cases:
        before, after = (case["variants"][variant] for variant in ["before", "after"])
        lines.append(f'| {case_name(case)} | {fmt(before["median_ms"])} | {fmt(after["median_ms"])} | '
                     f'{fmt(case["delta_ms"])} | {fmt(case["delta_percent"])} | {before["samples"]}/{after["samples"]} |')
    return lines


def render_markdown(report):
    lines = ["# Final node request comparison", "", f'Source: `{report["source_revision"]}`.', "",
             "The driver uses fresh generated data through real HTTP requests over owned local sockets.",
             "The values are elapsed wall time. Each median is descriptive. No significance check is part of this report.",
             "A case with one sample is a single call. Cases with fewer than five samples have no p95 value.",
             "Normal flush cases use three observed upper bounds. Their min and max values stay in the JSON.",
             "The file cache state is unknown. A completion file confirms that the driver reached its final step.", "",
             "## All matched latency cases", ""]
    lines += comparison_table(report["matched_cases"])
    lines += ["", "## All median latency increases", "",
              "This list includes every positive median change. Small changes can reflect the host or the request order.",
              "Queued Delete has different ack guarantees. Its times appear only in the separate boundary section.", ""]
    lines += comparison_table(report["latency_increases"])
    lines += ["", "## Warm read costs", "",
              "Each cell shows median / maximum. Zero costs describe only the operations that the receipt measures.", "",
              "| Phase / shape / keys | Variant | File opens | Read operations | Metadata operations | Directory reads | Payload bytes |",
              "|---|---|---:|---:|---:|---:|---:|"]
    for case in report["query_costs"]:
        if case["phase"] not in {"steady", "after-unrelated-write"}:
            continue
        for variant, data in sorted(case["variants"].items()):
            fields = []
            for field in ["file_opens", "read_operations", "metadata_operations", "directory_reads", "payload_bytes"]:
                value = data["costs"].get(field)
                fields.append("—" if value is None else f'{value["median"]:g} / {value["maximum"]:g}')
            lines.append(f'| {case["phase"]} / {case["shape"]} / {case["batch"]} | {variant} | ' + " | ".join(fields) + " |")
    lines += ["", "## Concurrent complete requests", "",
              "All acks use the last response timestamp. All durable spans end at the explicit flush response.",
              "The JSON includes each request span and the exact accepted and completed sequence fields.",
              "An accepted batch count above one proves that multiple complete requests share the queue.",
              "Clock and change-feed requests can also advance the sequence and the durable frontier.", "",
              "| Variant / round | All acks ms | All durable ms | Flush ms | Accepted batches after acks | Flush target / completed | Frontier change |",
              "|---|---:|---:|---:|---:|---|---:|"]
    for row in report["concurrent"]:
        receipt = row["flush_receipt"]
        prefix = f'{receipt.get("target_sequence", "—")} / {receipt.get("completed_sequence", "—")}'
        lines.append(f'| {row["variant"]} / {row["repeat"]} | {fmt(row["all_acks_ms"])} | '
                     f'{fmt(row["all_durable_ms"])} | {fmt(row["flush_ms"])} | '
                     f'{row["after_all_acks"].get("accepted_batches", "—")} | {prefix} | {row["frontier_delta"]} |')
    lines += ["", "## Normal flush observations", "",
              "No explicit flush occurs during these samples. The first complete status response gives an upper bound after the ack.",
              "The bound includes status request time and the 20 ms gap between polls. It is not a disk commit timestamp.", "",
              "| Variant / round | Ack ms | Disk after ack upper bound ms | Polls | Reply guarantee |",
              "|---|---:|---:|---:|---|"]
    for row in report["normal_flush"]:
        lines.append(f'| {row["variant"]} / {row["repeat"]} | {fmt(row["ack_ms"])} | '
                     f'{fmt(row["disk_after_ack_upper_ms"])} | {row["poll_count"]} | {row["actual_durability"]} |')
    lines += ["", "## Delete reply boundary", "",
              "The Delete payload uses the schema and the hash plus range key. Empty fields are valid.",
              "The candidate requires native Delete to complete the durable erase before the reply, even when the request asks for queued durability.",
              "The baseline queued reply can precede that erase. The driver checks the baseline result after a flush.",
              "The driver checks the candidate result directly after the stronger reply, then checks the result after a flush.",
              "Mixed native write and erase requests also flush the captured prefix before a durable reply.", "",
              "| Variant / operation | Ack ms | Actual guarantee |", "|---|---:|---|"]
    for row in report["delete"]:
        lines.append(f'| {row["variant"]} / {row["operation"]} | {fmt(row["elapsed_nanos"] / 1_000_000)} | '
                     f'{row["result"].get("durability", "—")} |')
    lines += ["", "## App storage trust", "",
              "These values are the final app storage responses after normal reopen.",
              "An incomplete response does not prove exact live-byte totals. Keep the trust fields with the byte values.", "",
              "| Variant | Complete | Meters origin | Reason | Live bytes | Unattributed bytes | Heavy |",
              "|---|---|---|---|---:|---:|---|"]
    for row in report["app_storage"]:
        data = row["report"]
        lines.append(f'| {row["variant"]} | {data.get("complete", "—")} | {data.get("meters_origin", "—")} | '
                     f'{data.get("incomplete_reason", "—")} | {data.get("live_total_bytes", "—")} | '
                     f'{data.get("unattributed_bytes", "—")} | {data.get("heavy", "—")} |')
    lines += ["", "## Receipt checks and limits", "", f'- Query receipts: {report["checks"]["queries"]}.',
              f'- Query receipts with exact generated values: {report["checks"]["exact_value_queries"]}.',
              f'- Owned node stops: {report["checks"]["node_stops"]}.',
              f'- Forced node stops: {report["checks"]["forced_stops"]}.',
              f'- Refused requests: {report["checks"]["refused_requests"]}.',
              f'- Unmatched latency cases: {len(report["unmatched_cases"])}.', "",
              "The read receipt measures named file and read costs. It does not measure every CPU operation, lock wait, allocation, or kernel cache cost.",
              "Source review indicates that warm reads still perform request validation, dependency checks, response copies, and JSON conversion.",
              "The queue still requotes its accepted prefix for each request and pauses admission during the disk commit and post-commit delivery.",
              "Those source statements explain possible costs. The elapsed receipts do not isolate their separate time.", ""]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("--source-revision", default="5b918355")
    args = parser.parse_args()
    run = args.run.resolve(strict=True)
    complete = run / "completed.json"
    if not complete.is_file():
        parser.error("The run lacks completed.json. Wait for the driver completion notice.")
    receipts = [json.loads(line) for line in (run / "receipts.jsonl").read_text().splitlines() if line]
    matched, unmatched, boundaries = latency_cases(receipts)
    queries = [row for row in receipts if row["operation"] == "query"]
    stops = [row for row in receipts if row["operation"] == "owned-node-stop"]
    report = {
        "source_revision": args.source_revision, "run": str(run), "raw_receipts": str(run / "receipts.jsonl"),
        "inputs": json.loads((run / "inputs.json").read_text()),
        "completed": json.loads(complete.read_text()),
        "matched_cases": matched, "unmatched_cases": unmatched, "different_guarantee_cases": boundaries,
        "latency_increases": sorted([case for case in matched if case["delta_ms"] > 0],
                                    key=lambda case: case["delta_ms"], reverse=True),
        "query_costs": query_costs(receipts), "concurrent": concurrent_details(receipts),
        "normal_flush": normal_flush_details(receipts),
        "delete": [row for row in receipts if row["operation"] in {"delete-request", "queued-delete-boundary"}],
        "app_storage": [{"variant": row["variant"], "report": app_storage(row["result"])}
                        for row in receipts if row["operation"] == "final-storage-report"],
        "checks": {"queries": len(queries), "exact_value_queries": sum(row.get("expected_values_match") is True for row in queries),
                   "node_stops": len(stops), "forced_stops": sum(row["forced_kill"] for row in stops),
                   "stop_exit_codes": [row["exit_code"] for row in stops],
                   "refused_requests": sum(row["operation"] == "refusal" for row in receipts)},
    }
    (run / "node-report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (run / "node-report.md").write_text(render_markdown(report))
    print(json.dumps({"matched_cases": len(matched), "latency_increases": len(report["latency_increases"]),
                      "unmatched_cases": len(unmatched), "json": str(run / "node-report.json"),
                      "markdown": str(run / "node-report.md")}))


if __name__ == "__main__":
    main()
