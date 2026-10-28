#!/usr/bin/env python3
"""Operate isolated peer-sync prototype homes with the real example command."""

import argparse
import filecmp
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import stat
import subprocess
import tempfile
import time


HASH = "device-demo"
PAGE_ENTRIES = 2
MAX_COMMANDS = 256
MAX_TRANSFER_PAGES = 64
MAX_RANGE_PAGES = 128
MAX_DUMP_RECORDS = 4096


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class Operation:
    def __init__(self, binary, output, work):
        self.binary = binary
        self.output = output
        self.work = work
        self.count = 0
        self.timings = []
        self.observed = {}
        self.devices = {name: work / name for name in ("a", "b")}

    def command(self, *args, refuse=False):
        if self.count >= MAX_COMMANDS:
            raise RuntimeError("operation exceeds the command budget")
        command = [str(self.binary), *map(str, args)]
        started = time.perf_counter()
        result = subprocess.run(command, cwd=self.work, capture_output=True,
                                text=True, timeout=90, check=False)
        elapsed = (time.perf_counter() - started) * 1000
        self.count += 1
        value = json.loads(result.stdout) if result.returncode == 0 else None
        clock = re.search(r"command=\S+ elapsed_ms=([0-9.]+)", result.stderr)
        timing = {"step": self.count, "command": str(args[0]),
                  "process_ms": elapsed,
                  "command_ms": float(clock[1]) if clock else None}
        if isinstance(value, dict) and "durable_write_ms" in value:
            timing["durable_write_ms"] = value["durable_write_ms"]
        self.timings.append(timing)
        record = {**timing, "argv": command, "returncode": result.returncode,
                  "result": value, "stderr": result.stderr[-8192:],
                  "requested_refusal": refuse}
        with (self.output / "commands.jsonl").open("a") as stream:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
        if refuse:
            if result.returncode <= 0:
                raise RuntimeError("the invalid operation did not receive a normal refusal")
        elif result.returncode != 0:
            raise RuntimeError(f"command {args[0]} failed; see commands.jsonl")
        return value

    def status(self, device):
        return self.command("status", self.devices[device])

    def dump(self, device, label):
        path = self.work / f"{label}-{device}.jsonl"
        result = self.command("dump", self.devices[device], HASH, path)
        digest = hashlib.sha256()
        records = 0
        with path.open() as source:
            for line in source:
                records += 1
                if records > MAX_DUMP_RECORDS or len(line) > 128 * 1024:
                    raise RuntimeError("dump exceeds the operation budget")
                value = json.loads(line)
                digest.update(json.dumps(value, sort_keys=True).encode() + b"\n")
        if result["records"] != records:
            raise RuntimeError("dump count differs from the actual command output")
        return path, {"records": records, "canonical_sha256": digest.hexdigest()}

    def transfer(self, source, target):
        moved = 0
        for origin, high in self.status(source)["frontiers"].items():
            for page in range(MAX_TRANSFER_PAGES):
                before = self.status(target)["frontiers"].get(origin, {}).get("sequence", 0)
                if before >= high["sequence"]:
                    break
                packet = self.work / f"transfer-{self.count}-{page}.packet"
                result = self.command("export", self.devices[source], origin,
                                      before, packet, PAGE_ENTRIES)
                if not 0 < result["entries"] <= PAGE_ENTRIES:
                    raise RuntimeError("source did not provide a bounded nonempty tail")
                self.command("import", self.devices[target], packet)
                after = self.status(target)["frontiers"].get(origin, {}).get("sequence", 0)
                if after != result["last_sequence"] or after <= before:
                    raise RuntimeError("received tail did not advance its actual source position")
                moved += result["entries"]
            else:
                raise RuntimeError("tail exceeds the page budget")
        return moved


def disk_totals(root):
    pending = [(root, 0)]
    result = {"files": 0, "file_bytes": 0, "allocated_bytes": 0}
    visited = 0
    while pending:
        directory, depth = pending.pop()
        if depth > 20:
            raise RuntimeError("owned directory exceeds the depth budget")
        with os.scandir(directory) as entries:
            for entry in entries:
                visited += 1
                if visited > 10000:
                    raise RuntimeError("owned directory exceeds the entry budget")
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise RuntimeError("owned directory contains a symlink")
                if stat.S_ISDIR(info.st_mode):
                    pending.append((Path(entry.path), depth + 1))
                elif stat.S_ISREG(info.st_mode):
                    result["files"] += 1
                    result["file_bytes"] += info.st_size
                    result["allocated_bytes"] += info.st_blocks * 512
                else:
                    raise RuntimeError("owned directory contains an unexpected file kind")
    return result


def initialize(operation):
    key = operation.work / "account-key"
    operation.command("key", key)
    identities = {}
    for name, home in operation.devices.items():
        status = operation.command("init", home, key)
        identities[name] = status["origin"]
        operation.observed["compiled_limits"] = status.get("limits")
    if identities["a"] == identities["b"]:
        raise RuntimeError("the two homes share one writer identity")
    operation.observed["origins"] = identities
    operation.command("batch", operation.devices["a"], HASH, 0, 64, 256)
    operation.command("put", operation.devices["a"], HASH, "shared", "a-version")
    operation.command("put", operation.devices["a"], HASH, "removed", "a-deleted-value")
    operation.command("delete", operation.devices["a"], HASH, "removed")
    operation.command("batch", operation.devices["b"], HASH, 1000, 64, 256)
    operation.command("put", operation.devices["b"], HASH, "shared", "b-version")
    operation.command("put", operation.devices["b"], HASH, "b-private", "unpublished-b-value")
    if operation.command("get", operation.devices["a"], HASH, "removed") is not None:
        raise RuntimeError("the actual deleted key remains readable")
    private = operation.command("get", operation.devices["b"], HASH, "b-private")
    if private != "unpublished-b-value":
        raise RuntimeError("the actual private keyed read differs from its write")
    operation.observed["private_before_adoption"] = private
    operation.observed["divergent_frontiers"] = {
        name: operation.status(name)["frontiers"] for name in operation.devices}
    return identities


def gap_and_duplicate(operation, origins):
    root_a, root_b = operation.devices["a"], operation.devices["b"]
    gap = operation.work / "gap.packet"
    exported = operation.command("export", root_a, origins["a"], 1, gap, 1)
    if exported["entries"] != 1 or exported["last_sequence"] != 2:
        raise RuntimeError("gap request did not select the actual second event")
    before = operation.status("b")
    _, view_before = operation.dump("b", "before-gap")
    operation.command("import", root_b, gap, refuse=True)
    _, view_after = operation.dump("b", "after-gap")
    if operation.status("b") != before or view_before != view_after:
        raise RuntimeError("gap refusal changed the actual state or source position")
    first = operation.work / "first.packet"
    operation.command("export", root_a, origins["a"], 0, first, 1)
    operation.command("import", root_b, first)
    before = operation.status("b")
    _, view_before = operation.dump("b", "before-duplicate")
    duplicate = operation.command("import", root_b, first)
    _, view_after = operation.dump("b", "after-duplicate")
    if operation.status("b") != before or view_before != view_after:
        raise RuntimeError("duplicate delivery changed the actual state or source position")
    operation.observed["gap_refused_without_publication"] = True
    operation.observed["duplicate_import"] = duplicate


def snapshot_and_exchange(operation):
    snapshot = operation.work / "snapshot-a"
    operation.observed["snapshot"] = operation.command(
        "snapshot", operation.devices["a"], snapshot)
    operation.observed["adoption"] = operation.command(
        "adopt", operation.devices["b"], snapshot)
    private = operation.command("get", operation.devices["b"], HASH, "b-private")
    if private != operation.observed["private_before_adoption"]:
        raise RuntimeError("snapshot adoption lost the actual unpublished local value")
    operation.observed["private_after_adoption"] = private
    operation.observed["after_adoption_frontiers"] = operation.status("b")["frontiers"]
    for round_number in range(4):
        moved = operation.transfer("b", "a") + operation.transfer("a", "b")
        left = operation.status("a")["frontiers"]
        right = operation.status("b")["frontiers"]
        if left == right:
            operation.observed["converged_frontiers"] = left
            operation.observed["exchange_rounds"] = round_number + 1
            return
        if moved == 0:
            break
    raise RuntimeError("bounded exchanges did not reach the same source positions")


def range_dump(operation, device):
    path = operation.work / f"range-{device}.jsonl"
    after = "-"
    pages = 0
    with path.open("x") as output:
        for _ in range(MAX_RANGE_PAGES):
            page = operation.command("range", operation.devices[device], HASH, after, 17)
            pages += 1
            for item in page["items"]:
                output.write(json.dumps(item, separators=(",", ":")) + "\n")
            if page["next"] is None:
                return path, pages
            if page["next"] == after:
                raise RuntimeError("actual range cursor did not advance")
            after = page["next"]
    raise RuntimeError("actual range exceeded the page budget")


def observe_final(operation):
    paths, views = {}, {}
    for name in operation.devices:
        paths[name], views[name] = operation.dump(name, "converged")
        ranged, pages = range_dump(operation, name)
        if not filecmp.cmp(paths[name], ranged, shallow=False):
            raise RuntimeError("paged range differs from the actual canonical dump")
        views[name]["range_pages"] = pages
        if operation.command("get", operation.devices[name], HASH, "removed") is not None:
            raise RuntimeError("a deleted key returned after convergence")
    if not filecmp.cmp(paths["a"], paths["b"], shallow=False):
        raise RuntimeError("the actual peer views differ after convergence")
    shared = {name: operation.command("get", root, HASH, "shared")
              for name, root in operation.devices.items()}
    if shared["a"] != shared["b"] or shared["a"] is None:
        raise RuntimeError("the actual conflicting keyed values differ")
    operation.observed.update({"views": views, "shared_key": shared,
                              "disk_totals": {name: disk_totals(root)
                                              for name, root in operation.devices.items()},
                              "snapshot_disk_totals": disk_totals(operation.work / "snapshot-a")})


def summary(results):
    lines = ["# Shared snapshot and device log operation", "",
             f"Result: {results['result']}. Source: `{results['source_revision']}`.", "",
             "The command uses fresh V2 homes and synthetic device changes.",
             "Each command reopens its home. No daemon or cloud service starts.", ""]
    observed = results.get("observed", {})
    if "views" in observed:
        lines += [f"Both peers return the same {observed['views']['a']['records']} records.",
                  "The keyed reads, range pages, and source positions agree.",
                  "Snapshot adoption preserves the actual unpublished B value.",
                  "A gap receives a refusal. Duplicate delivery leaves the actual state unchanged.", ""]
    lines += ["| Operation | Process ms | Command ms | Durable batch write ms |",
              "| --- | ---: | ---: | ---: |"]
    for row in results.get("timings", []):
        if row["command"] in {"batch", "snapshot", "adopt"}:
            command_ms = row["command_ms"]
            durable = row.get("durable_write_ms")
            lines.append(f"| {row['command']} | {row['process_ms']:.3f} | "
                         f"{command_ms if command_ms is not None else '-'} | "
                         f"{durable if durable is not None else '-'} |")
    lines += ["", "Process time includes startup. Durable batch time covers the native write call.",
              f"Largest child process memory peak: {results['largest_child_peak_memory_bytes'] / 1048576:.2f} MiB.",
              "This Linux child-process measurement has no per-command attribution and establishes no hard memory bound.",
              "These small operations establish no production latency or crash guarantee.",
              f"Owned temporary homes removed: {results['cleanup']['removed']}."]
    if "error" in results:
        lines += ["", f"Operation error: {results['error']}"]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise RuntimeError("this operational driver runs only on a hosted CI runner")
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        raise RuntimeError("the source revision must be a complete commit SHA")
    binary = args.binary.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    results = {"result": "incomplete", "source_revision": args.revision,
               "program_sha256": checksum(binary),
               "operation_limits": {"commands": MAX_COMMANDS, "transfer_pages": MAX_TRANSFER_PAGES,
                                    "range_pages": MAX_RANGE_PAGES, "dump_records": MAX_DUMP_RECORDS,
                                    "events_per_transfer_page": PAGE_ENTRIES}}
    operation, owned = None, None
    try:
        with tempfile.TemporaryDirectory(prefix="lastdb-peer-sync-operation-",
                                         dir=os.environ["RUNNER_TEMP"]) as directory:
            owned = Path(directory)
            operation = Operation(binary, args.output, owned)
            origins = initialize(operation)
            gap_and_duplicate(operation, origins)
            snapshot_and_exchange(operation)
            observe_final(operation)
            results["result"] = "passed"
    except Exception as error:
        results["error"] = str(error)
        results["result"] = "failed"
    if operation:
        results.update({"observed": operation.observed, "timings": operation.timings,
                        "command_count": operation.count})
    results["largest_child_peak_memory_bytes"] = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024
    results["cleanup"] = {"removed": owned is not None and not owned.exists()}
    if not results["cleanup"]["removed"]:
        results["result"] = "failed"
    save(args.output / "results.json", results)
    text = summary(results)
    (args.output / "summary.md").write_text(text)
    print(text)
    return 0 if results["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
