#!/usr/bin/env python3
"""CI-only cold-read layout investigation; separate from the standard recipe."""

import argparse
import json
import os
from pathlib import Path
import re
import time

from node import Node, file_hash, save
from workload import PROFILES, Workload, describe, read_request, validate_read


def snapshot(node):
    response, _, _ = node.call("/api/db/native-status")
    native = response["native_disk"]
    runs = []
    for path in node.home.rglob("*.ldrun"):
        if len(runs) >= 10_000:
            raise RuntimeError("Generated run-file observation exceeded its bound")
        try:
            info = path.stat()
        except FileNotFoundError:
            continue  # A merge can retire a run during this observation.
        runs.append((str(path.relative_to(node.home)), info.st_size))
    return {"generation": native["generation"], "frontier": native["frontier"],
            "completed_sequence": native["completed_sequence"], "runs": sorted(runs),
            "started_monotonic": time.monotonic()}


def stable_boundary(node):
    """Observe three stable seconds; this is not a product quiescence barrier."""
    deadline = time.monotonic() + 60
    history, previous, since = [], None, time.monotonic()
    while time.monotonic() < deadline:
        current = snapshot(node)
        identity = {k: v for k, v in current.items() if k != "started_monotonic"}
        if identity != previous:
            previous, since = identity, time.monotonic()
        history.append(current)
        if time.monotonic() - since >= 3:
            return history
        time.sleep(0.1)
    raise RuntimeError("The generated home did not show three stable seconds")


def trace(node, output):
    """Status calls alter pacing: this trace is not a speed comparison."""
    rows = []
    started = time.perf_counter()
    with (output / "trace.jsonl").open("w") as stream:
        for number in range(256):
            body = read_request([number])
            before = snapshot(node)
            result, elapsed, receipt = node.call(
                "/api/query", body, receipt=True, request_id=f"cold-layout-{number}")
            after = snapshot(node)
            validate_read(result, [number])
            if not receipt or receipt["read_operations"] <= 0 or receipt["payload_bytes"] <= 0:
                raise RuntimeError("A traced cold read lacks positive disk payload evidence")
            if receipt["logical_warm_hits"] != 0:
                raise RuntimeError("A traced cold read unexpectedly hit a logical warm record")
            row = {"request": number, "elapsed_ms": elapsed, "records": 1,
                   "before": before, "after": after, "read_receipt": receipt}
            stream.write(json.dumps(row) + "\n")
            rows.append(row)
    metric = describe(rows, time.perf_counter() - started)
    metric["generation_transitions"] = sum(
        a["after"]["generation"] != b["after"]["generation"] for a, b in zip(rows, rows[1:]))
    metric["frontiers"] = sorted({r[side]["frontier"] for r in rows for side in ("before", "after")})
    metric["run_counts"] = sorted({len(r[side]["runs"]) for r in rows for side in ("before", "after")})
    return metric


def measure(binary, revision, output, mode):
    output.mkdir()
    node = Node(binary, revision, output, environment={"LASTDB_DISK_FORMAT": "2"})
    work, error, metric = None, None, None
    try:
        node.start()
        if node.disk_format() != 2:
            raise RuntimeError("The investigation requires a v2 run home")
        work = Workload(node, {**PROFILES["standard"], "native_timing": False}, output)
        work.seed()
        if mode == "observed-stable":
            save(output / "settle.json", stable_boundary(node))
            boundary = node.call("/api/admin/shed", {})[0]["shed"]
            save(output / "stable-cold-boundary.json", boundary)
            if boundary.get("error") or not boundary.get("target_met"):
                raise RuntimeError("The stable boundary failed to purge logical warm records")
        if mode == "trace":
            metric = trace(node, output)
        else:
            # Reuse the original case, without extra per-request status calls.
            work.cold_next = 0
            work.measure("cold-read/1", "cold")
            metric = work.metrics["cold-read/1"]
        save(output / "after.json", snapshot(node))
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        if work:
            work.raw.close()
        try:
            node.close()
        except BaseException as failure:
            error = f"{error or ''}; cleanup: {type(failure).__name__}: {failure}"
    result = {"mode": mode, "complete": error is None, "error": error, "metric": metric}
    save(output / "result.json", result)
    if error:
        raise RuntimeError(error)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        raise RuntimeError("This diagnostic runs only in GitHub Actions on an exact revision")
    args.output.mkdir(parents=True, exist_ok=True)
    modes = ["immediate", "trace", "observed-stable"]
    results = []
    manifest = {"complete": False, "product_revision": args.revision,
                "binary_sha256": file_hash(args.binary), "runs": results,
                "scope": "Three independent fresh homes per trial, same program and seed contents. "
                         "Trace status calls change pacing; observed stability is not an idle barrier. "
                         "No OS cache flush. Original immediate-case samples remain intact."}
    save(args.output / "investigation.json", manifest)
    for trial in range(3):
        for mode in modes[trial:] + modes[:trial]:
            result = measure(args.binary.resolve(), args.revision, args.output / f"{trial}-{mode}", mode)
            results.append({"trial": trial, **result})
            save(args.output / "investigation.json", manifest)
    manifest["complete"] = True
    save(args.output / "investigation.json", manifest)
    lines = ["## Cold-read layout investigation", "", f"Product: `{args.revision}`", "",
             manifest["scope"], "", "| Trial | Mode | P50 ms | P95 ms |", "|---|---|---:|---:|"]
    for row in results:
        metric = row["metric"]
        lines.append(f"| {row['trial']} | {row['mode']} | {metric['p50_ms']:.3f} | {metric['p95_ms']:.3f} |")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
