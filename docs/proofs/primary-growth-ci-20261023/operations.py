#!/usr/bin/env python3
"""Capture real product operations on owned generated homes, never primary copies."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

sys.dont_write_bytecode = True
HERE = Path(os.environ["LASTDB_GROWTH_REPORT_DIR"]).resolve(strict=True)
OLD = Path(__file__).with_name("product.py")
spec = importlib.util.spec_from_file_location("upgrade_checks", OLD)
checks = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checks)


def capture(command, output):
    start = time.monotonic()
    with output.open("w") as stream, output.with_suffix(".err").open("w") as errors:
        subprocess.run(list(map(str, command)), stdout=stream, stderr=errors, check=True)
    return {"seconds": time.monotonic() - start, "output": json.loads(output.read_text())}


def size(home):
    root = home / "data/disk-pack"
    if not root.exists():
        root = home
    stack, total, files, missing = [(root, 0)], 0, 0, 0
    start = time.monotonic()
    while stack:
        path, depth = stack.pop()
        if depth > 6 or time.monotonic() - start > 30:
            raise RuntimeError("Physical metadata walk bound")
        try:
            iterator = os.scandir(path)
        except FileNotFoundError:
            missing += 1
            continue
        with iterator:
            for entry in iterator:
                try:
                    if entry.is_symlink():
                        raise RuntimeError("Unexpected physical-home symlink")
                    if entry.is_dir(follow_symlinks=False):
                        stack.append((Path(entry.path), depth + 1))
                    else:
                        total += entry.stat(follow_symlinks=False).st_size
                        files += 1
                        if files > 262144:
                            raise RuntimeError("Physical file bound")
                except FileNotFoundError:
                    missing += 1
    return {"bytes": total, "files": files, "concurrent_removals": missing}


class GrowthNode(checks.Node):
    def start(self):
        environment = {k: v for k, v in os.environ.items()
                       if not k.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_"))}
        environment.update(LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
            LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
            LASTDB_LOOKUP_REPACK_BYTES=str(8 * 1024 * 1024),
            LASTDB_LOOKUP_REPACK_PARTS="128", RUST_MIN_STACK="16777216")
        self.stream = self.log.open("ab")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.sock), "--full-socket-path", str(self.sock) + "f"],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.stream,
            stderr=self.stream, start_new_session=True)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("Owned daemon exited")
            try:
                value, _ = checks.call(self.sock, "/health", timeout=2)
                if value.get("status") == "ok":
                    return
            except (OSError, checks.http.client.HTTPException):
                pass
            time.sleep(.1)
        raise RuntimeError("Owned daemon readiness deadline")


def grow(args):
    report = {"source": args.source, "primary_copy": False, "variants": {}}
    variants = [("baseline", args.scratch / "baseline-daemon"), ("candidate", args.candidate)]
    for label, binary in variants:
        node = GrowthNode(binary, args.scratch / label, args.scratch / (label[0] + "g.sock"), args.scratch / (label + "-growth.log"))
        expected = {f"key-{n:03d}": checks.fields(n) for n in range(64)}
        if label == "candidate":
            expected["key-000"] = checks.fields(0, 1)
            expected.pop("key-001")
            expected["key-064"] = checks.fields(64)
        row = {"before": size(node.home), "samples": []}
        try:
            node.start()
            latencies = []
            for revision in range(256):
                mutations = [checks.mutation(n, "update", checks.fields(n, revision + 100)) for n in range(16, 32)]
                result = checks.durable(node.sock, mutations)
                latencies.append(result["milliseconds"])
                expected.update({f"key-{n:03d}": checks.fields(n, revision + 100) for n in range(16, 32)})
                if revision % 16 == 15:
                    status, _ = checks.call(node.sock, "/api/db/native-status")
                    sample = {"round": revision, "size": size(node.home), "native": status["native_disk"]}
                    row["samples"].append(sample)
                    print(json.dumps({"variant": label, "round": revision, "size": sample["size"]}), flush=True)
            row["readback"] = checks.rows(node.sock, expected)
            row["max_write_ms"] = max(latencies)
            if label == "candidate":
                deadline = time.monotonic() + 180
                while time.monotonic() < deadline:
                    native, _ = checks.call(node.sock, "/api/db/native-status")
                    auto = native["native_disk"]["automatic_maintenance"]
                    if auto["last_error"]:
                        raise RuntimeError(auto["last_error"])
                    if auto["full_runs"] >= 2 and not auto["cleanup_pending"]:
                        row["automatic"] = auto
                        break
                    time.sleep(.5)
                else:
                    raise RuntimeError("Automatic maintenance did not complete two full payload cycles")
            row["after"] = size(node.home)
            row["shutdown"] = node.stop()
            node.start()
            row["reopen_readback"] = checks.rows(node.sock, expected)
            row["reopen_shutdown"] = node.stop()
            if label == "candidate":
                old = checks.Node(args.scratch / "baseline-daemon", node.home,
                                  args.scratch / "rg.sock", args.scratch / "old-after-growth.log")
                try:
                    old.start()
                    row["old_program_readback"] = checks.rows(old.sock, expected)
                    row["old_program_shutdown"] = old.stop()
                finally:
                    old.cleanup_process()
            report["variants"][label] = row
            checks.save(HERE / "growth.json", report)
        finally:
            node.cleanup_process()
    before = report["variants"]["baseline"]["after"]["bytes"]
    after = report["variants"]["candidate"]["after"]["bytes"]
    if after >= before / 2:
        raise RuntimeError("Generated workload did not reduce retained disk bytes by half")
    report.update(passed=True, candidate_to_baseline_disk_ratio=after / before)
    checks.save(HERE / "growth.json", report)


def fnv(text):
    value = 0xcbf29ce484222325
    for byte in text.encode():
        value = ((value ^ byte) * 0x100000001b3) & ((1 << 64) - 1)
    return value


def scale_input(args):
    prefixes, n = {}, 0
    while len(prefixes) < 1024:
        prefix = f"growth-{n}\0"
        prefixes.setdefault(fnv(prefix) & 1023, prefix)
        n += 1
    known = []
    with (args.scratch / "scale.jsonl").open("w") as stream:
        for group, prefix in sorted(prefixes.items()):
            for index in range(1025):
                key = prefix + f"{index:07d}/" + hashlib.sha256(f"{group}/{index}".encode()).hexdigest() * 7
                value = f"value-{group}-{index}".encode()
                stream.write(json.dumps({"collection": "growth", "key": key, "body_hex": value.hex()}, separators=(",", ":")) + "\n")
                if index in (0, 512, 1024):
                    known.append({"collection": "growth", "key": key, "expected_hex": value.hex()})
    checks.save(args.scratch / "scale-keys.json", known)
    print(json.dumps({"generated_records": 1024 * 1025, "input_bytes": (args.scratch / "scale.jsonl").stat().st_size}), flush=True)


def exact_scale_reads(binary, home, scratch, known, label):
    count = 0
    for ordinal, first in enumerate(range(0, len(known), 384)):
        batch = known[first:first + 384]
        request = scratch / "scale-read.json"
        checks.save(request, {"coordinates": [{"collection": x["collection"], "key": x["key"]} for x in batch]})
        result = capture([binary, "read", "--home", home, "--input", request], HERE / f"scale-read-{label}-{ordinal}.json")
        actual = result["output"]["results"]
        expected = [{"body_hex": x["expected_hex"]} for x in batch]
        if actual != expected:
            raise RuntimeError("Exact generated scale keys differ")
        count += len(actual)
    return {"matched": count, "batches": ordinal + 1, "all_values_match": True}


def build_scale(args):
    node = checks.Node(args.scratch / "baseline-daemon", args.scratch / "scale-node", args.scratch / "s.sock", args.scratch / "scale-node.log")
    try:
        node.start()
        shutdown = node.stop()
    finally:
        node.cleanup_process()
    home = node.home / "data/disk-pack"
    chunk = args.scratch / "scale-chunk.jsonl"
    records, chunks = 0, 0
    start = time.monotonic()
    with (args.scratch / "scale.jsonl").open() as source:
        while True:
            lines = []
            for _ in range(4096):
                line = source.readline()
                if not line:
                    break
                lines.append(line)
            if not lines:
                break
            chunk.write_text("".join(lines))
            result = capture([args.scratch / "baseline-disk-home", "write", "--home", home, "--input", chunk], HERE / f"scale-append-{chunks:03d}.json")
            if result["output"]["operations"] != len(lines):
                raise RuntimeError("Generated stored batch count differs")
            records += len(lines)
            chunks += 1
            if chunks % 32 == 0:
                print(json.dumps({"scale_chunks": chunks, "records": records}), flush=True)
    return home, {"records": records, "chunks": chunks, "seconds": time.monotonic() - start,
                  "initial_native_shutdown": shutdown, "baseline_native_home": True}


def scale(args):
    baseline = args.scratch / "baseline-disk-home"
    report = {"generated_records": 1024 * 1025, "primary_copy": False}
    seed = json.loads((HERE / "scale-seed.json").read_text())
    home = Path(seed["home"])
    report["build"] = seed
    report["before"] = size(home)
    # Measure the actual write-gate pause separately from incremental deletion.
    # The seed is generated by the old program and was never a primary copy.
    node = GrowthNode(args.candidate, home.parents[1], args.scratch / "sf.sock", args.scratch / "scale-final.log")
    started = time.monotonic()
    samples = []
    first_full = None
    try:
        node.start()
        deadline = time.monotonic() + 600
        while time.monotonic() < deadline:
            status, _ = checks.call(node.sock, "/api/db/native-status")
            native = status["native_disk"]
            auto = native["automatic_maintenance"]
            samples.append(auto)
            if native["writer_failed"] or auto["last_error"]:
                raise RuntimeError("Large automatic compaction failed")
            if first_full is None and auto["full_runs"] >= 1:
                if not auto["cleanup_pending"]:
                    raise RuntimeError("The large home did not exercise interrupted cleanup")
                first_full = auto
                report["interrupted_cleanup_shutdown"] = node.stop()
                node.start()
                continue
            if first_full is not None and not auto["cleanup_pending"]:
                report["automatic_full_compaction"] = {
                    "seconds": time.monotonic() - started,
                    "write_pause_seconds": first_full["last_repack_ms"] / 1000,
                    "max_cleanup_step_ms": max(x["last_cleanup_ms"] for x in samples),
                    "automatic": first_full,
                    "after_restart": auto,
                    "cleanup_resumed_after_restart": True,
                }
                break
            time.sleep(5)
        else:
            raise RuntimeError("Large automatic compaction did not settle")
        report["automatic_shutdown"] = node.stop()
    finally:
        node.cleanup_process()
    report["repack"] = capture([args.maintain, "repack", "--home", home], HERE / "scale-repack.json")
    if report["repack"]["output"]["result"]["records_copied"] < 1024 * 1025:
        raise RuntimeError("Repack did not include every generated stored record")
    known = json.loads((args.scratch / "scale-keys.json").read_text())
    checks.save(args.scratch / "scale-read.json", {"coordinates": [{"collection": x["collection"], "key": x["key"]} for x in known]})
    report["old_reader"] = exact_scale_reads(baseline, home, args.scratch, known, "before-cleanup")
    report["cleanup"] = capture([args.maintain, "reclaim", "--home", home, "--grace-ms", 0], HERE / "scale-cleanup.json")
    report["after_cleanup_read"] = exact_scale_reads(baseline, home, args.scratch, known, "after-cleanup")
    report["full_compaction"] = capture([args.maintain, "compact", "--home", home, "--grace-ms", 0], HERE / "scale-full-compaction.json")
    if report["full_compaction"]["output"]["result"]["records_copied"] < 1024 * 1025:
        raise RuntimeError("Full compaction did not cover the generated keys")
    report["after_full_compaction_read"] = exact_scale_reads(baseline, home, args.scratch, known, "after-full-compaction")
    report["after"] = size(home)
    report["passed"] = True
    checks.save(HERE / "scale.json", report)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("scale-input", "scale", "growth"))
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--maintain", type=Path)
    parser.add_argument("--source")
    args = parser.parse_args()
    owner = json.loads((args.scratch / "owner.json").read_text())
    if owner["primary_data"] or owner["scratch"] != str(args.scratch.resolve()) or owner["owner_uid"] != os.getuid():
        raise RuntimeError("Owned generated home differs")
    {"scale-input": scale_input, "scale": scale, "growth": grow}[args.mode](args)
