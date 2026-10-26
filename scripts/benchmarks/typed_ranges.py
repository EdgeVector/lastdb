"""Prepare an ordinary paired measurement through the product's offline converter."""

import json
import os
import signal
import stat
import subprocess
import time

from node import TYPED_RUN_NODE_MARKER, file_hash, save


def run_converter(command, stdout, stderr):
    process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
    try:
        code = process.wait(timeout=1800)
        if code:
            raise subprocess.CalledProcessError(code, command)
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        raise


def source_hashes(root):
    stack, result, entries = [(root, 0)], {}, 0
    deadline = time.monotonic() + 60
    while stack:
        parent, depth = stack.pop()
        if depth > 12 or entries > 1_000_000 or time.monotonic() > deadline:
            raise RuntimeError("The stopped conversion-source inventory exceeded its bound")
        for path in parent.iterdir():
            entries += 1
            if entries > 1_000_000 or time.monotonic() > deadline:
                raise RuntimeError("The stopped conversion-source inventory exceeded its bound")
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                stack.append((path, depth + 1))
            elif stat.S_ISREG(info.st_mode):
                result[str(path.relative_to(root))] = file_hash(path)
            else:
                raise RuntimeError("The stopped conversion-source contains an unexpected file type")
    return result


def convert(node):
    if node.process is not None:
        raise RuntimeError("The typed converter requires the owned daemon to stop")
    cli = node.binary.with_name("lastdb")
    version = subprocess.check_output([str(cli), "--version"], text=True, timeout=10).strip()
    if f"bench-{node.revision}" not in version:
        raise RuntimeError("The conversion CLI differs from the measured daemon revision")
    before = source_hashes(node.home / "data")
    source_usage = node.disk_usage()
    target = node.root / "typed-home"
    command = [str(cli), "--data-dir", str(node.home), "db", "convert-typed-ranges",
               "--target-home", str(target), "--source-hash-encoding", "blind_v1",
               "--source-range-encoding", "ope_v1", "--max-records", "1000000",
               "--max-metadata-mib", "1024", "--json"]
    resource_path = node.output / "typed-conversion-resources.txt"
    started = time.monotonic()
    with (node.output / "typed-conversion.stdout").open("w") as stdout, \
            (node.output / "typed-conversion.stderr").open("w") as stderr:
        run_converter(["/usr/bin/time", "--verbose", "--output", str(resource_path), *command], stdout, stderr)
    elapsed = time.monotonic() - started
    result = json.loads((node.output / "typed-conversion.stdout").read_text())
    if (not result.get("ok") or not result["conversion"].get("activation_ready")
            or not result["conversion"].get("source_unchanged")
            or before != source_hashes(node.home / "data")):
        raise RuntimeError("The typed conversion did not preserve its complete stopped source")
    if (target / "data" / "laststore-pack-node-v1").read_text() != TYPED_RUN_NODE_MARKER:
        raise RuntimeError("The converter did not publish the exact typed V2 marker")
    node.home = target
    node.environment["LASTDB_RANGE_KEY_ENCODING"] = "plain"
    target_usage = node.disk_usage()
    save(node.output / "typed-conversion.json", {
        "elapsed_seconds": elapsed, "cli_sha256": file_hash(cli), "cli_version": version,
        "source_file_count": len(before), "source_hashes_unchanged": True,
        "source_usage": source_usage, "target_usage": target_usage,
        "retained_source_and_target_bytes": source_usage["logical_bytes"] + target_usage["logical_bytes"],
        "conversion": result["conversion"],
        "limit": "The retained byte sum excludes transient merge files; it is not a peak-disk measurement.",
    })


def conversion_lines(output, manifest):
    if not manifest["recipe"].get("typed_range_comparison"):
        return []
    lines = ["", "## Explicit typed range conversion", "",
             "Both versions receive the same String schema, values, and blinded-hash/OPE source configuration.",
             "Only the candidate converts its stopped generated source with its matching product CLI.",
             "Measured requests then use that typed target. Repeats retain the same format choice.", "",
             "| Run | Conversion seconds | Source MiB | Typed target MiB | Retained total MiB | Source hashes unchanged |",
             "|---|---:|---:|---:|---:|---|"]
    for run in manifest["runs"]:
        if run["label"] != "candidate":
            continue
        result = json.loads((output / run["directory"] / "typed-conversion.json").read_text())
        if not result["source_hashes_unchanged"]:
            raise ValueError("A typed conversion changed its source")
        lines.append(f"| {run['directory']} | {result['elapsed_seconds']:.3f} | "
                     f"{result['source_usage']['logical_bytes'] / 2**20:.2f} | "
                     f"{result['target_usage']['logical_bytes'] / 2**20:.2f} | "
                     f"{result['retained_source_and_target_bytes'] / 2**20:.2f} | true |")
    lines.extend(["", "The retained total excludes transient merge files; it is not a peak-disk measurement.",
                  "The artifact retains conversion output, CLI hashes, and Linux process resource measurements.",
                  "This comparison covers String range keys. Other scalar domains use separate product operations."])
    return lines
