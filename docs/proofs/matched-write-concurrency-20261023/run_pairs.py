#!/usr/bin/env python3
"""CI-only comparison of eight-record requests with one and four clients."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True
from analyze import analyze

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def save(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")


def compress(path):
    raw = path.read_bytes()
    target = path.with_suffix(path.suffix + ".gz")
    if target.exists():
        raise RuntimeError("preserve the preceding archive")
    packed = gzip.compress(raw, mtime=0)
    if gzip.decompress(packed) != raw:
        raise RuntimeError("archive differs")
    target.write_bytes(packed)
    result = {"file": path.name, "raw_sha256": hashlib.sha256(raw).hexdigest(),
              "gzip_sha256": hashlib.sha256(packed).hexdigest(), "bytes": len(raw)}
    path.unlink()
    return result


def cleanup(scratch, owner, output):
    info = scratch.stat()
    if (scratch.is_symlink() or scratch.resolve() != scratch
            or (info.st_uid, info.st_dev, info.st_ino) != (owner["uid"], owner["device"], owner["inode"])
            or json.loads((scratch / "owner.json").read_text()) != owner
            or not json.loads((output / "cleanup.json").read_text())["removed"]
            or (scratch / "home").exists()):
        raise RuntimeError("retain scratch: ownership or home cleanup differs")
    shutil.rmtree(scratch)
    save(output / "scratch-cleanup.json", {"owner": owner, "removed": not scratch.exists()})


def run(build, pair, clients, label, args):
    binary = build["binary"]
    if hashlib.sha256(binary.read_bytes()).hexdigest() != build["binary_sha256"]:
        raise RuntimeError("the frozen executable differs")
    scratch = Path(tempfile.mkdtemp(prefix="mc.", dir=os.environ["RUNNER_TEMP"])).resolve()
    info = scratch.stat()
    owner = {"root": str(scratch), "uid": os.getuid(), "device": info.st_dev, "inode": info.st_ino}
    save(scratch / "owner.json", owner)
    name = f"pair{pair}-{label}-{clients}"
    output = args.output / name
    save(args.output / (name + "-owner.json"), owner)
    argv = [sys.executable, str(HERE / "capture.py"), "--binary", str(binary), "--revision", build["revision"],
            "--scratch", str(scratch), "--output", str(output), "--clients", str(clients),
            "--count", str(args.count)]
    if args.profile:
        argv.append("--profile")
    print(json.dumps({"start": name, "load": os.getloadavg()}), flush=True)
    with (args.output / (name + "-stdout.log")).open("w") as log:
        subprocess.run(argv, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
    result = analyze(output)
    archives = [compress(output / file) for file in ("writes.json", "seed.json", "schema-metadata.json")]
    save(output / "archives.json", archives)
    cleanup(scratch, owner, output)
    print(json.dumps({"complete": name, "load": os.getloadavg()}), flush=True)
    return {"name": name, "source": build["revision"], "label": label, "clients": clients,
            "complete": True, "result": result}


def summarize(records, output):
    runs = [run for record in records for run in record["runs"]]
    digests = {json.dumps(run["result"]["expected_digest"], sort_keys=True) for run in runs}
    if len(digests) != 1:
        raise RuntimeError("the final known-key values differ across matched runs")
    groups = {}
    for label in ("before", "after"):
        for clients in (1, 4):
            selected = [r["result"] for r in runs if r["label"] == label and r["clients"] == clients]
            groups[f"{label}-{clients}"] = {
                "runs": len(selected),
                "median_run_average_ms": statistics.median(r["client"]["average_ms"] for r in selected),
                "median_run_p99_ms": statistics.median(r["client"]["p99_ms"] for r in selected),
                "median_records_per_second": statistics.median(r["records_per_second"] for r in selected)}
    ratios = [{"pair": i, "clients": clients, "after_over_before_average_ms":
               next(r["result"]["client"]["average_ms"] for r in record["runs"]
                    if r["label"] == "after" and r["clients"] == clients) /
               next(r["result"]["client"]["average_ms"] for r in record["runs"]
                    if r["label"] == "before" and r["clients"] == clients)}
              for i, record in enumerate(records) for clients in (1, 4)]
    save(output / "comparison.json", {"groups": groups, "paired_ratios": ratios,
         "records_per_request": 8, "all_final_values_equal": True,
         "latency_unit": "one complete eight-record request; never divided by eight",
         "diagnostics_enabled": records[0]["profile"], "slowdowns_are_advisory": True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for label in ("before", "after"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("LastDB comparisons run only on GitHub-hosted Actions runners")
    if args.pairs < 1 or args.count < 64 or args.count % 4:
        raise RuntimeError("use complete equal client assignments and at least one pair")
    args.output = (args.output or Path(os.environ["RUNNER_TEMP"]) / ("matched-" + uuid.uuid4().hex)).resolve()
    if args.output.is_relative_to(ROOT.resolve()):
        raise RuntimeError("raw comparison output must stay outside the source tree")
    args.output.mkdir(parents=True)
    builds = {}
    for label in ("before", "after"):
        binary = getattr(args, label + "_binary").resolve(strict=True)
        builds[label] = {"binary": binary, "revision": getattr(args, label + "_revision"),
                         "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
    records = []
    for pair in range(args.pairs):
        order = [("before", 1), ("after", 4), ("before", 4), ("after", 1)]
        if pair % 2:
            order.reverse()
        record = {"started_ns": time.time_ns(), "order": order, "runs": [], "complete": False,
                  "records_per_request": 8, "requests_per_run": args.count, "profile": args.profile,
                  "github_run_id": os.environ.get("GITHUB_RUN_ID"), "host_activity_controlled": False}
        index = args.output / f"pair{pair}.json"
        save(index, record)
        for label, clients in order:
            record["runs"].append(run(builds[label], pair, clients, label, args))
            save(index, record)
        record.update(complete=True, ended_ns=time.time_ns())
        save(index, record)
        records.append(record)
    summarize(records, args.output)


if __name__ == "__main__":
    main()
