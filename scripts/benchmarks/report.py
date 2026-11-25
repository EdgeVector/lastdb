#!/usr/bin/env python3
"""Publish advisory performance changes, absolute limits, confirmation decisions, mixed reads, and native diagnostics."""

from __future__ import annotations

import argparse
import gzip
import json
import math
from pathlib import Path
import statistics

from limits import breaches, decide, limit_lines
from node import home_format_line, publish_summary, save
from phases import phase_lines

MEASURES = ("p50_ms", "p95_ms", "p99_ms", "requests_per_second", "records_per_second")
NATIVE_CASES = ("insert/1", "update/1", "update/128", "concurrent-write/1", "concurrent-write/4",
                "concurrent-write/32", "concurrent-mixed/32")


def suspects(comparison, kind):
    if kind == "growth":
        return {(case, name) for case, item in comparison.items() for name in item["advisories"]}
    return {(case, name) for case, item in comparison.items()
            for name, limit in (("p95_ms", 20), ("requests_per_second", -20))
            if (item["changes_percent"][name] >= limit if limit > 0 else item["changes_percent"][name] <= limit)}


def repeat_set(comparison, kind):
    """Measures that start the confirmation pass: relative suspects and absolute speed-limit breaches."""
    return suspects(comparison, kind) | (breaches(comparison) if kind == "speed" else set())


def load_comparison(output, compare_fn, status="success"):
    manifest = json.loads((output / "benchmark.json").read_text())
    if not manifest["complete"] or status != "success":
        raise ValueError("The measurement did not complete successfully")
    comparison = compare_fn(output, manifest)
    save(output / "comparison.json", comparison)
    return manifest, comparison


def confirm(output, manifest, comparison, compare_fn, kind, status):
    if status != "success":
        raise ValueError("The automatic confirmation step did not complete successfully")
    initial, repeated, second = suspects(comparison, kind), set(), {}
    if repeat_set(comparison, kind):
        other, second = load_comparison(output / "confirmation", compare_fn)
        for name in ("profile", "recipe", "driver_revision", "drivers", "binaries", "host"):
            if manifest[name] != other[name]:
                raise ValueError(f"The confirmation differs in {name}")
        if set(comparison) != set(second):
            raise ValueError("The confirmation measured different cases")
        repeated = suspects(second, kind)
    rows = [{"case": case, "metric": name,
             "initial": {label: comparison[case][label][name] for label in ("baseline", "candidate")},
             "repeat": {label: second[case][label][name] for label in ("baseline", "candidate")},
             "confirmed": (case, name) in repeated} for case, name in sorted(initial)]
    decision = {"kind": kind, "repeat_required": bool(initial), "results": rows,
                "repeat_only_suspects": sorted(repeated - initial)}
    save(output / "advisory-confirmation.json", decision)
    return decision, second


def _fmt_pair(pair, kind, metric=""):
    if metric.endswith("_ratio"):
        return " → ".join(f"{pair[label]:.3f}×" for label in ("baseline", "candidate"))
    divisor = 2**20 if kind == "growth" else 1
    unit = " MiB" if kind == "growth" else ""
    return " → ".join(f"{pair[label] / divisor:.3f}" for label in ("baseline", "candidate")) + unit


def confirmation_lines(decision):
    result = ["", "## Automatic advisory confirmation", ""]
    if not decision["repeat_required"]:
        return "\n".join(result + ["No measure crossed an advisory limit.", ""])
    result.extend(["CI repeated this suite once with the same programs, recipe, and fresh generated homes.",
                   "Only the same case and measure above its limit in both runs produces a warning.", "",
                   "| Case | Measure | First base → candidate | Repeat base → candidate | Result |",
                   "|---|---|---:|---:|---|"])
    for row in decision["results"]:
        state = "Confirmed advisory" if row["confirmed"] else "Did not repeat; no warning"
        result.append(f"| {row['case']} | {row['metric']} | {_fmt_pair(row['initial'], decision['kind'], row['metric'])} | "
                      f"{_fmt_pair(row['repeat'], decision['kind'], row['metric'])} | {state} |")
    result.extend(["", "A measure first above its limit in the repeat has no confirmation and produces no warning.",
                   "Both comparisons remain in the artifact. Confirmed changes stay advisory.", ""])
    return "\n".join(result)


def notices(decision):
    for row in decision["results"]:
        if row["confirmed"]:
            print(f"::warning title=Confirmed advisory {decision['kind']} change::{row['case']}: {row['metric']}; "
                  f"first {_fmt_pair(row['initial'], decision['kind'], row['metric'])}; "
                  f"repeat {_fmt_pair(row['repeat'], decision['kind'], row['metric'])}. "
                  "Both comparisons crossed the advisory limit. This does not block merge.")


def _distribution(values):
    ordered = sorted(values)
    return {"reported": len(ordered), **{
        name: ordered[math.ceil(len(ordered) * fraction) - 1] if ordered else None
        for name, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99))}}


def _cohort(samples):
    receipts = [row["read_receipt"] for row in samples if row.get("read_receipt")]
    payloads = [row["payload_bytes"] for row in receipts if row.get("payload_bytes") is not None]
    versions = [row for row in samples if row.get("read_current_case_records") is not None]
    return {
        "requests": len(samples), "rpc": _distribution([row["elapsed_ms"] for row in samples]),
        "socket_queue": _distribution([row["socket_queue_wait_nanos"] / 1e6 for row in receipts
                                       if row.get("socket_queue_wait_nanos") is not None]),
        "storage": _distribution([row["query_storage_elapsed_nanos"] / 1e6 for row in receipts
                                  if row.get("query_storage_elapsed_nanos") is not None]),
        "payload_reported": len(payloads), "payload_fetches": sum(value > 0 for value in payloads),
        "current_case_records": sum(row["read_current_case_records"] for row in versions),
        "version_reported_records": sum(row["records"] for row in versions),
    }


def mixed_read_lines(output, manifest):
    if "mixed" not in manifest["recipe"] or manifest["recipe"].get("insert_only"):
        return []
    lines = ["", "## Initial and later mixed reads", "",
             "All reads remain in the aggregate mixed metrics and slowdown advisories.",
             "Groups use each client's request order, not elapsed time or a warm-up cutoff.",
             "Read-first means turn 0; write-first means the first read at turn 1; later means turn 2 or higher.",
             "A group name does not prove a cache hit or miss. Payload receipts show actual fetches.",
             "Current-case values have titles written during this case; older values can be valid captured snapshots.",
             "Queue and storage columns show their own p95 and reported count; missing means unreported.",
             "Their percentiles cannot be added or subtracted to explain an RPC percentile.",
             "Small initial groups can make p95 equal the maximum; these groups are descriptive, not regression gates.", "",
             "| Run / clients | Read group | Calls | RPC p50 / p95 ms | Socket queue p95 ms (reported) | Storage p95 ms (reported) | Payload fetches / reported | Current-case values / reported records |",
             "|---|---|---:|---:|---:|---:|---:|---:|"]
    results = []
    for run in manifest["runs"]:
        with gzip.open(output / run["directory"] / "samples.jsonl.gz", "rt") as source:
            reads = [row for line in source if (row := json.loads(line))["case"].startswith("concurrent-mixed/")
                     and row["kind"] == "read"]
        for case in sorted({row["case"] for row in reads}):
            groups = {"read-first": [], "write-first": [], "later": []}
            for row in (r for r in reads if r["case"] == case):
                turn = row["client_request"]
                groups["read-first" if turn == 0 else "write-first" if turn == 1 else "later"].append(row)
            for name, rows in groups.items():
                value = _cohort(rows)
                results.append({"run": run["directory"], "case": case, "group": name, **value})
                rpc = f"{value['rpc']['p50_ms']:.3f} / {value['rpc']['p95_ms']:.3f}" if rows else "unreported"
                fmt = lambda v: f"{v['p95_ms']:.3f} ({v['reported']})" if v["p95_ms"] is not None else "unreported (0)"
                lines.append(f"| {run['directory']} / {case.rsplit('/', 1)[-1]} | {name} | {len(rows)} | "
                             f"{rpc} | {fmt(value['socket_queue'])} | {fmt(value['storage'])} | "
                             f"{value['payload_fetches']}/{value['payload_reported']} | "
                             f"{value['current_case_records']}/{value['version_reported_records']} |")
    save(output / "mixed-read-cohorts.json", {"groups": results,
         "scope": "Diagnostic partitions only; all samples remain in aggregate metrics"})
    return lines


def compare(output, manifest):
    metrics = {label: {} for label in ("baseline", "candidate")}
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A measurement did not complete")
        metrics[run["label"]][run["pair"]] = json.loads((output / run["directory"] / "metrics.json").read_text())
    pairs = set(range(manifest["recipe"]["pairs"]))
    if any(set(results) != pairs for results in metrics.values()):
        raise ValueError("The measurements lack a complete pair")
    cases = set(metrics["baseline"][0])
    if not cases or any(set(run) != cases for results in metrics.values() for run in results.values()):
        raise ValueError("The measured cases differ across versions or pairs")
    comparison = {}
    for case in sorted(cases):
        item = {}
        for label, results in metrics.items():
            samples = [results[pair][case] for pair in sorted(pairs)]
            if any(s["errors"] or not s["requests"] or s["p95_ms"] is None for s in samples):
                raise ValueError(f"{case} contains an invalid measurement")
            item[label] = {name: statistics.median(s[name] for s in samples) for name in MEASURES}
            item[label].update(requests=sum(s["requests"] for s in samples), errors=0)
        item["changes_percent"] = {
            name: 100 * (item["candidate"][name] / item["baseline"][name] - 1) for name in MEASURES}
        item["paired_p95_changes_percent"] = [
            100 * (metrics["candidate"][p][case]["p95_ms"] / metrics["baseline"][p][case]["p95_ms"] - 1)
            for p in sorted(pairs)]
        comparison[case] = item
    return comparison


def resource_lines(output, manifest):
    lines = ["", "| Run | Sampled daemon memory MiB | Final home MiB | Measured disk growth MiB |", "|---|---:|---:|---:|"]
    for run in manifest["runs"]:
        path = output / run["directory"] / "metrics.json"
        if not path.exists():
            continue
        primary = sorted((m for m in json.loads(path.read_text()).values() if "disk_after_bytes" in m),
                         key=lambda m: m["sequence"])
        memory = [m["sampled_peak_memory_bytes"] for m in primary if m["sampled_peak_memory_bytes"] is not None]
        if primary:
            peak = f"{max(memory) / 2**20:.1f}" if memory else "unavailable"
            lines.append(f"| {run['directory']} | {peak} | {primary[-1]['disk_after_bytes'] / 2**20:.1f} | "
                         f"{sum(m['disk_growth_bytes'] for m in primary) / 2**20:.1f} |")
    return lines


def mixed_lines(output, manifest):
    if "mixed" not in manifest["recipe"] or manifest["recipe"].get("insert_only"):
        return []
    lines = ["", "## Mixed requests across each case", "",
             "| Run / clients | Clients with both kinds | Phase coverage | First / last read ms | First / last write ms |",
             "|---|---:|---:|---:|---:|"]
    for run in manifest["runs"]:
        folder = output / run["directory"]
        metrics = json.loads((folder / "metrics.json").read_text())
        with gzip.open(folder / "samples.jsonl.gz", "rt") as source:
            rows = [row for line in source if (row := json.loads(line))["case"].startswith("concurrent-mixed/")]
        for case, metric in metrics.items():
            if not case.startswith("concurrent-mixed/") or "clients" not in metric:
                continue
            samples = [row for row in rows if row["case"] == case]
            clients = metric["clients"]
            for client in range(clients):
                calls = sorted((r for r in samples if r["client"] == client), key=lambda r: r["client_request"])
                kinds = [r["kind"] for r in calls]
                if (len(calls) * clients != metric["requests"]
                        or [r["client_request"] for r in calls] != list(range(len(calls)))
                        or kinds.count("read") != kinds.count("write")
                        or any(a == b for a, b in zip(kinds, kinds[1:]))):
                    raise ValueError(f"{case}: a live client did not alternate equal read/write calls")
            matched = [row for row in samples if "ts_ms" in row.get("server_request_ops", {})]

            def window(kind):
                if len(matched) != len(samples):
                    return "unavailable"
                times = [row["server_request_ops"]["ts_ms"] - metric["case_started_unix_us"] / 1000
                         for row in matched if row["kind"] == kind]
                return f"{min(times):.3f} / {max(times):.3f}"

            lines.append(f"| {run['directory']} / {clients} | {clients}/{clients} | "
                         f"{len(matched)}/{len(samples)} | {window('read')} | {window('write')} |")
    lines.extend(["", "Each client alternates equal read and write calls. Adjacent clients start with opposite kinds.",
                  "Windows use server completion times from the case start. They are separate from request latency.",
                  "Phase capture follows the timed case. Incomplete coverage leaves both windows unavailable."])
    return lines


def render(output, manifest, comparison):
    lines = ["# LastDB benchmarks", "",
             f"Profile: **{manifest['profile']}**. Pairs: **{manifest['recipe']['pairs']}**.", "",
             f"Base: `{manifest['binaries']['baseline']['revision']}`  ",
             f"Candidate: `{manifest['binaries']['candidate']['revision']}`  ",
             f"Driver: `{manifest['driver_revision']}`", "",
             f"Requested socket workers (0 = product default): {manifest['recipe'].get('uds_workers', 'unreported')}", "",
             home_format_line(output, manifest), "",
             "Each number is the median of the per-run measures. Percent changes use the base commit.",
             "Positive response-time changes and negative throughput changes mean slower results.", "",
             "| Case | Base p95 ms | Candidate p50 / p95 / p99 ms | p95 change | Candidate requests/s | Rate change | Errors |",
             "|---|---:|---:|---:|---:|---:|---:|"]
    for case, item in comparison.items():
        base, cand, change = item["baseline"], item["candidate"], item["changes_percent"]
        lat = " / ".join(f"{cand[name]:.3f}" for name in ("p50_ms", "p95_ms", "p99_ms"))
        lines.append(f"| {case} | {base['p95_ms']:.3f} | {lat} | {change['p95_ms']:+.1f}% | "
                     f"{cand['requests_per_second']:.1f} | {change['requests_per_second']:+.1f}% | 0 |")
    lines.extend(resource_lines(output, manifest) + mixed_lines(output, manifest)
                 + mixed_read_lines(output, manifest) + phase_lines(output, manifest))
    lines.extend(["", "CAUTION: Shared GitHub machines vary. These results establish no fixed speed guarantee.",
                  "Cold reads miss the LastDB warm set. The operating system file cache stays uncontrolled.",
                  "The throughput includes the client cost. The memory measure excludes seed and startup.",
                  "A 20% slowdown requires confirmation in a second complete comparison before a notice. Relative slowdowns never fail the check.",
                  "The absolute limits below fail the check when the same breach appears in both complete passes.",
                  "Raw samples, receipts, per-pair changes, resource measures, and build logs are in the run artifact.",
                  "Generated homes and identity files stay out of the artifact and leave the runner after shutdown."])
    return "\n".join(lines) + "\n"


def render_native(output):
    manifest = json.loads((output / "benchmark.json").read_text())
    if not manifest["complete"] or not manifest["recipe"]["native_timing"]:
        raise ValueError("The separate native diagnostic campaign is incomplete")
    largest = lambda phases: (", ".join(f"{k}={v['mean'] / 1000:.3f} ({v['reported']} records)"
                              for k, v in sorted(phases.items(), key=lambda i: i[1]["mean"], reverse=True)[:5]) or "unreported")
    lines = ["# Native write diagnostics — separate from ordinary latency", "",
             f"Driver: `{manifest['driver_revision']}`. Pairs: {manifest['recipe']['pairs']}.",
             f"Requested socket workers (0 = product default): {manifest['recipe'].get('uds_workers', 'unreported')}",
             "Native timing is enabled only for these generated CI homes.",
             "Instrumentation can change latency; use the ordinary campaign for the speed comparison.",
             "Scope means are per native request; physical means are per distinct commit.",
             "A physical commit can serve several requests, including background work.",
             "Nested phase values overlap. Do not add them or assign one shared commit to each request.", "",
             "| Run / case | Write scopes | Physical commits | Scope mean ms | Largest scope phases ms |",
             "|---|---:|---:|---:|---|"]
    physical_lines = ["", "| Run / case | Commit mean ms | Largest physical phases ms |", "|---|---:|---|"]
    tails = ["", "## Exact slow update requests", "",
             "Each request joins to an enclosed scope and one distinct physical commit by sequence and time.",
             "Shared physical commits retain their request count; their cost is not assigned to every request.", ""]
    for run in manifest["runs"]:
        if not run["complete"]:
            raise ValueError("A native diagnostic run did not complete")
        data = json.loads((output / run["directory"] / "native-phases.json").read_text())
        if not data["complete"]:
            raise ValueError("Native phase coverage is incomplete")
        for name in NATIVE_CASES:
            case = data["cases"][name]
            label = f"{run['directory']} / {name}"
            lines.append(f"| {label} | {case['scopes']} | {case['distinct_physical_commits']} | "
                         f"{case['scope_mean_us'] / 1000:.3f} | {largest(case['scope_phases_us'])} |")
            physical_lines.append(f"| {label} | {case['physical_mean_us'] / 1000:.3f} | {largest(case['physical_phases_us'])} |")
            for req in case.get("slowest_requests", []):
                tails.extend([f"### {label}: {req['request_id']}", "", "```json", json.dumps(req, sort_keys=True), "```", ""])
    return "\n".join(lines + physical_lines + tails) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("suite", "native"), default="suite")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-status", default="success")
    parser.add_argument("--confirmation-status")
    args = parser.parse_args()
    if args.mode == "native":
        try:
            report, rc = render_native(args.output), 0
        except (OSError, ValueError, KeyError, TypeError) as error:
            report, rc = f"# Native diagnostics — incomplete\n\n{type(error).__name__}: {error}\n", 1
        publish_summary(args.output, report, "native-summary.md")
        return rc
    if args.confirmation_status is None:
        parser.error("--confirmation-status is required in suite mode")
    try:
        manifest = json.loads((args.output / "benchmark.json").read_text())
        if not manifest["complete"] or args.job_status != "success":
            raise ValueError("The build or measurement did not complete successfully")
        comparison = compare(args.output, manifest)
        save(args.output / "comparison.json", comparison)
        decision, second = confirm(args.output, manifest, comparison, compare, "speed", args.confirmation_status)
        limit_rows = decide(comparison, second)
        save(args.output / "limit-decision.json", {"results": limit_rows})
        report = (render(args.output, manifest, comparison) + "\n".join(limit_lines(comparison, limit_rows))
                  + "\n" + confirmation_lines(decision))
        complete = True
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError) as failure:
        report = ("# LastDB benchmarks — incomplete\n\n"
                  f"{type(failure).__name__}: {failure}\n\n"
                  "This run supplies no valid speed comparison. Inspect the build logs, result files, and raw samples.\n"
                  "Relative slowdowns are advisory. A build failure, incomplete measurement, or confirmed\n"
                  "absolute-limit breach is an error.\n")
        complete, limit_rows = False, []
    publish_summary(args.output, report)
    failed = [row for row in limit_rows if row["failed"]]
    for row in failed:
        print(f"::error title=Absolute response-time limit::{row['case']} {row['metric']} "
              f"{row['first']:.3f} then {row['repeat']:.3f} ms; limit {row['limit']:.1f} ms in both passes.")
    if complete:
        notices(decision)
    return 0 if complete and not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
