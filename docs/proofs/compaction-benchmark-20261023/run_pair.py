#!/usr/bin/env python3
"""Run one ordered before/after pair under the existing outer host lock."""
import argparse
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output
import signal
import shutil
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("owned_cleanup",
    HERE.parent / "snapshot-commits-20261022" / "cleanup.py")
cleanup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cleanup)


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def archive(run):
    source = run / "receipts.jsonl"
    raw = source.read_bytes()
    target = run / "receipts.jsonl.gz"
    target.write_bytes(gzip.compress(raw, compresslevel=9, mtime=0))
    if gzip.decompress(target.read_bytes()) != raw:
        raise RuntimeError("Archived receipts differ")
    save(run / "receipt-archive.json", {"raw_sha256": digest(source),
        "archive_sha256": digest(target), "raw_bytes": len(raw),
        "archive_bytes": target.stat().st_size, "decompressed_bytes_equal": True})
    source.unlink()


def execute(command, log):
    with log.open("w") as stream:
        child = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        try:
            code = child.wait(timeout=1500)
        except BaseException:
            child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=60)
            except subprocess.TimeoutExpired:
                # Retain the run and any node ownership records for recovery.
                child.kill()
                child.wait(timeout=10)
            raise
    if code:
        raise RuntimeError(f"Benchmark driver exited {code}; retain {log}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--builds", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=default_output('compaction-benchmark-20261023'))
    parser.add_argument("--pair", type=int, required=True)
    parser.add_argument("--records", type=int, default=100000)
    parser.add_argument("--only-this-pair", action="store_true")
    args = parser.parse_args()
    args.output = resolve_output(args.output)
    if not 0 <= args.pair < 3:
        parser.error("pair must be 0..2")
    builds = json.loads(args.builds.read_text())
    args.output.mkdir(parents=True, exist_ok=True)
    index_path = args.output / f"pair-{args.pair}.json"
    if index_path.exists():
        raise ValueError("Refuse to replace an existing benchmark pair")
    order = ("before", "after") if args.pair % 2 == 0 else ("after", "before")
    index = {"pair": args.pair, "order": order, "records": args.records,
             "complete": False, "runs": [], "started_ns": time.time_ns(),
             "host_load_average": os.getloadavg(),
             "runner_sha256": digest(Path(__file__)),
             "exclusive_window": "existing with-fold-host-cargo-lock wrapper"}
    save(index_path, index)
    for label in order:
        free = shutil.disk_usage(os.environ["TMPDIR"]).free
        if free < 20 * 1024 ** 3:
            raise RuntimeError("Less than 20 GiB remains for the generated home")
        build = builds[label]
        binary = Path(build["binary"])
        if digest(binary) != build["sha256"]:
            raise ValueError("Executable differs from its build receipt")
        name = f"pair-{args.pair}-{label}"
        run = (args.output / name).absolute()
        scratch = Path(tempfile.mkdtemp(prefix=f"cb{args.pair}{label[0]}.",
                                      dir=os.environ["TMPDIR"]))
        command = [sys.executable, str(HERE / "capture.py"), "--binary", str(binary),
            "--source-revision", build["revision"], "--scratch", str(scratch),
            "--output", str(run), "--records", str(args.records),
            "--label", label, "--pair", str(args.pair)]
        started = time.time_ns()
        print(json.dumps({"start": name, "records": args.records}), flush=True)
        execute(command, args.output / f"{name}.stdout.log")
        summary = json.loads((run / "summary.json").read_text())
        if not summary["complete"] or not summary["all_owned_processes_stopped"]:
            raise RuntimeError(f"Run remains incomplete: {name}")
        cleanup.cleanup(run)
        archive(run)
        index["runs"].append({"name": name, "label": label,
            "started_ns": started, "ended_ns": time.time_ns(),
            "end_host_load_average": os.getloadavg(),
            "temporary_volume_free_at_start": free,
            "source_revision": build["revision"], "binary_sha256": build["sha256"]})
        save(index_path, index)
        print(json.dumps({"complete": name, "capacity_refusals": summary["capacity_refusals"]}),
              flush=True)
    index.update(complete=True, ended_ns=time.time_ns())
    save(index_path, index)
    if not args.only_this_pair:
        for following in range(args.pair + 1, 3):
            subprocess.run([sys.executable, str(Path(__file__).resolve()),
                "--builds", str(args.builds), "--output", str(args.output),
                "--records", str(args.records), "--pair", str(following),
                "--only-this-pair"], check=True)


if __name__ == "__main__":
    main()
