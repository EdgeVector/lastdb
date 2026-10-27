#!/usr/bin/env python3
"""CI-only investigation of the original 32-client read advisory, with longer controls."""
import argparse
import gzip
import json
import os
from pathlib import Path
import re

from cold_layout import snapshot, stable_boundary
from node import Node, file_hash, save
from phases import capture_phases
from range_continuation.process_counters import read_counters, counter_delta
from report import compare
from workload import PROFILES, Workload, read_request, validate_read

PAIRS = 4
TARGETS = ("concurrent-read/32", "stable-short-read/32", "stable-long-read/32")


class ObservedWorkload(Workload):
    def worker(self, *args):
        rows = super().worker(*args)
        self.captured[args[-1]] = rows
        return rows

    def measure(self, name, *args, **kwargs):
        self.captured = {}
        before = snapshot(self.node) if name in TARGETS else None
        counters = read_counters(self.node.process.pid) if before else None
        super().measure(name, *args, **kwargs)
        if before:
            rows = [row for client in sorted(self.captured) for row in self.captured[client]]
            coverage = capture_phases(self.node, self.output, name, len(self.metrics), rows)
            after = snapshot(self.node)
            with gzip.open(self.output / f"{name.replace('/', '-')}-phases.jsonl.gz", "wt") as stream:
                for row in rows:
                    stream.write(json.dumps(row) + "\n")
            save(self.output / f"{name.replace('/', '-')}-observations.json", {
                "before": before, "after": after, "coverage": coverage,
                "process": counter_delta(counters, read_counters(self.node.process.pid)),
                "all_warm": all(row["read_receipt"]["logical_warm_hits"] == 4 for row in rows),
                "payload_calls": sum(row["read_receipt"]["payload_bytes"] > 0 for row in rows)})


def measure(binary, revision, output):
    output.mkdir()
    node, work, error = Node(binary, revision, output), None, None
    try:
        node.start()
        work = ObservedWorkload(node, {**PROFILES["standard"], "native_timing": False}, output)
        work.seed()
        work.cold_next = 0
        work.measure("cold-read/1", "cold")
        work.measure("cold-read/32", "cold", batch=32)
        hot = list(range(work.recipe["records"] - 256, work.recipe["records"]))
        for start in range(0, len(hot), 64):
            numbers = hot[start:start + 64]
            validate_read(node.call("/api/query", read_request(numbers))[0], numbers)
        work.measure("hot-read/1", "hot")
        work.measure("hot-read/32", "hot", batch=32)
        for mode in ("insert", "update"):
            for batch in (1, 128):
                samples = work.recipe["single_write_samples"] if batch == 1 else (
                    work.recipe["batch_update_samples"] if mode == "update" else work.recipe["write_samples"])
                work.measure(f"{mode}/{batch}", mode, batch=batch, samples=samples)
        for clients in (1, 4, 16):
            for mode in ("read", "write", "mixed"):
                work.measure(f"concurrent-{mode}/{clients}", mode, clients=clients)
        # Preserve the exact original 256-request case after the preceding writes.
        work.measure("concurrent-read/32", "read", clients=32)
        save(output / "stable-before-short.json", stable_boundary(node))
        work.measure("stable-short-read/32", "read", clients=32)
        save(output / "stable-before-long.json", stable_boundary(node))
        work.measure("stable-long-read/32", "read", clients=32, samples=4096)
    except BaseException as failure:
        error = f"{type(failure).__name__}: {failure}"
    finally:
        if work:
            work.raw.close()
        try:
            node.close()
        except BaseException as failure:
            error = f"{error or ''}; cleanup: {failure}"
        save(output / "result.json", {"complete": error is None, "error": error})
    if error:
        raise RuntimeError(error)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    revisions = {k: getattr(args, f"{k}_revision") for k in ("baseline", "candidate")}
    if os.environ.get("GITHUB_ACTIONS") != "true" or any(not re.fullmatch(r"[0-9a-f]{40}", v) for v in revisions.values()):
        raise RuntimeError("Run only in GitHub Actions with exact product revisions")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = {k: getattr(args, f"{k}_binary").resolve() for k in revisions}
    manifest = {"complete": False, "driver_revision": os.environ["DRIVER_SHA"], "runs": [],
                "recipe": {"pairs": PAIRS, "original_samples": 256, "long_samples": 4096},
                "binaries": {k: {"revision": revisions[k], "sha256": file_hash(b)} for k, b in binaries.items()}}
    save(args.output / "benchmark.json", manifest)
    for pair in range(PAIRS):
        for label in (("baseline", "candidate") if pair % 2 == 0 else ("candidate", "baseline")):
            directory = f"{pair}-{label}"
            measure(binaries[label], revisions[label], args.output / directory)
            manifest["runs"].append({"pair": pair, "label": label, "directory": directory, "complete": True})
            save(args.output / "benchmark.json", manifest)
    comparison = compare(args.output, manifest)
    save(args.output / "comparison.json", comparison)
    manifest["complete"] = True
    save(args.output / "benchmark.json", manifest)
    lines = ["## 32-client read advisory investigation", "", f"Base: `{revisions['baseline']}`  ",
             f"Candidate: `{revisions['candidate']}`  ", f"Driver: `{manifest['driver_revision']}`", "",
             "Four alternating pairs, real 10,000-record generated homes, default product maintenance and socket worker settings.",
             "The original recipe through concurrent-read/32 stays intact. All 256 original calls remain measured.",
             "Additional cases follow three observed stable seconds, first 256 calls then 4,096 calls with 32 clients.",
             "Observed stability is not a maintenance barrier. Before/after file states and process counters remain explicit.",
             "Every read retains its warm/payload and queue/storage receipt. Phase coverage uses exact IDs after timing.",
             "The finite ring limits long-case phase coverage. Missing phases are unreported, not zero.", "",
             "| Case | Base P50 / P95 ms | Candidate P50 / P95 ms | P95 change | Pair changes |",
             "|---|---:|---:|---:|---:|---|"]
    for case in TARGETS:
        item = comparison[case]
        a, b = item["baseline"], item["candidate"]
        pairs = ", ".join(f"{v:+.1f}%" for v in item["paired_p95_changes_percent"])
        lines.append(f"| {case} | {a['p50_ms']:.3f} / {a['p95_ms']:.3f} | {b['p50_ms']:.3f} / {b['p95_ms']:.3f} | "
                     f"{item['changes_percent']['p95_ms']:+.1f}% | {pairs} |")
    lines += ["", "The original CI advisory remains valid evidence. This separate investigation does not overwrite it.",
              "A file-state change correlates with background activity; it does not prove a request-level cause."]
    summary = "\n".join(lines) + "\n"
    (args.output / "summary.md").write_text(summary)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(summary)


if __name__ == "__main__":
    main()
