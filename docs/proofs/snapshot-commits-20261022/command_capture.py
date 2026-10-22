#!/usr/bin/env python3
"""Capture two complete physical commands on a fresh product-created home."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from capture import Capture, file_hash, save


COLLECTION = "snapshot-command-capture"
PREFIX = "capture\0"
KEYS = ["repeat", "erased", "empty", "never", "stay", "missing"]


def entry(key, body=None):
    result = {"collection": COLLECTION, "key": PREFIX + key}
    if body is None:
        result["deleted"] = True
    else:
        result["body_hex"] = body.encode().hex()
    return result


def parts(root):
    """Read only the explicit owned physical subtree, with finite bounds."""
    stack, found, examined = [(root, 0)], {}, 0
    deadline = time.monotonic() + 20
    while stack:
        parent, depth = stack.pop()
        if depth > 8:
            raise RuntimeError("The physical inventory exceeds eight directory levels")
        for path in parent.iterdir():
            examined += 1
            if examined > 40000 or time.monotonic() > deadline:
                raise RuntimeError("The physical inventory exceeds its entry or time bound")
            if path.is_symlink():
                raise RuntimeError("The physical inventory refuses symlinks")
            if path.is_dir():
                stack.append((path, depth + 1))
            elif path.is_file():
                relative = str(path.relative_to(root))
                found[relative] = path.stat().st_size
                if len(found) > 20000:
                    raise RuntimeError("The physical inventory exceeds twenty thousand files")
    return found


def state(capture, phase):
    physical = capture.home / "data" / "disk-pack"
    current = (physical / "CURRENT").read_bytes()
    if len(current) != 4096:
        raise RuntimeError("The generated CURRENT has an unexpected size")
    inventory = parts(physical)
    generations = sorted({int(Path(name).parts[1]) for name in inventory
                          if name.startswith("generations/")
                          and Path(name).parts[1].isdigit()})
    result = {"phase": phase, "generation": struct.unpack_from("<Q", current, 16)[0],
              "frontier": struct.unpack_from("<Q", current, 32)[0],
              "current_sha256": hashlib.sha256(current).hexdigest(),
              "required_features": struct.unpack_from("<Q", current, 112)[0],
              "generations": generations,
              "payload_parts": sorted(name for name in inventory
                                      if name.startswith("packs/")
                                      and len(Path(name).parts) == 3
                                      and Path(name).name.startswith("part-")),
              "files": inventory}
    save(capture.output / f"state-{phase}.json", result)
    capture.inventory(phase)
    return result


def command(capture, binary, phase, verb, request, expect_success=True):
    path = capture.output / f"{phase}-input.json"
    if verb == "write":
        path = path.with_suffix(".jsonl")
        path.write_text("".join(json.dumps(item, sort_keys=True) + "\n" for item in request))
    else:
        save(path, request)
    args = [str(binary), verb, "--home", str(capture.home / "data" / "disk-pack"),
            "--input", str(path)]
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY"))}
    started = time.perf_counter_ns()
    result = subprocess.run(args, env=environment, stdin=subprocess.DEVNULL,
                            capture_output=True, text=True, timeout=60, check=False)
    (capture.output / f"{phase}-stdout.txt").write_text(result.stdout)
    (capture.output / f"{phase}-stderr.txt").write_text(result.stderr)
    capture.remember("physical-command", phase=phase, arguments=args,
                     returncode=result.returncode, elapsed_nanos=time.perf_counter_ns() - started,
                     stdout=result.stdout, stderr=result.stderr)
    if expect_success and result.returncode != 0:
        raise RuntimeError(f"Physical {phase} failed; inspect its command receipt")
    if not expect_success:
        return result
    value = json.loads(result.stdout)
    save(capture.output / f"{phase}-response.json", value)
    return value


def read_request():
    return {"coordinates": [{"collection": COLLECTION, "key": PREFIX + key} for key in KEYS],
            "budget": {"pages": 4096, "blocks": 16384, "bytes": 268435456, "keys": 10000}}


def readback(capture, binary, phase, expected, committed):
    value = command(capture, binary, phase, "read", read_request())
    results = value.get("results")
    wanted = [{"body_hex": None if expected[key] is None else expected[key].encode().hex()}
              for key in KEYS]
    if results != wanted:
        raise RuntimeError(f"The {phase} keyed values differ from the complete batch")
    if (value.get("generation"), value.get("frontier")) != (committed["generation"], committed["frontier"]):
        raise RuntimeError("The keyed read does not use the committed root")
    capture.remember("physical-keyed-readback", phase=phase, keys=KEYS, expected=wanted,
                     generation=value["generation"], frontier=value["frontier"])


def publication(capture, before, after, response, count, tombstones_only=False):
    added = sorted(set(after["generations"]) - set(before["generations"]))
    removed = sorted(set(before["generations"]) - set(after["generations"]))
    if added != [after["generation"]] or removed:
        raise RuntimeError("The complete batch does not create exactly one generation receipt")
    if after["frontier"] != before["frontier"] + 1:
        raise RuntimeError("The complete batch does not advance the frontier once")
    if after["generation"] != before["generation"] + 1:
        raise RuntimeError("The complete batch does not advance the generation once")
    expected = {"previous_frontier": before["frontier"], "frontier": after["frontier"],
                "generation": after["generation"], "operations": count, "published": True}
    if any(response.get(key) != value for key, value in expected.items()):
        raise RuntimeError("The command publication receipt differs from the committed files")
    payload_added = sorted(set(after["payload_parts"]) - set(before["payload_parts"]))
    if tombstones_only and after["payload_parts"] != before["payload_parts"]:
        raise RuntimeError("The tombstone-only batch changes payload parts")
    capture.remember("complete-batch-publication", phase=after["phase"], **expected,
                     generation_receipts_added=added, payload_parts_added=payload_added,
                     tombstones_only=tombstones_only)


def checksum_refusal(capture, binary, expected, committed):
    path = capture.home / "data" / "disk-pack" / "CURRENT"
    original = path.read_bytes()
    changed = bytearray(original)
    changed[200] ^= 1
    try:
        with path.open("r+b") as target:
            target.write(changed)
            target.flush()
            os.fsync(target.fileno())
        result = command(capture, binary, "corrupt-current", "read", read_request(), False)
        if result.returncode == 0 or "checksum" not in result.stderr.lower():
            raise RuntimeError("The corrupt CURRENT does not return an explicit checksum error")
        if result.stdout.strip():
            raise RuntimeError("The corrupt CURRENT command returns unexpected data")
        capture.remember("current-checksum-refusal", changed_byte=200,
                         original_sha256=hashlib.sha256(original).hexdigest(),
                         changed_sha256=hashlib.sha256(changed).hexdigest(),
                         returncode=result.returncode, error=result.stderr.strip())
    finally:
        with path.open("r+b") as target:
            target.write(original)
            target.flush()
            os.fsync(target.fileno())
        if path.read_bytes() != original:
            raise RuntimeError("The original CURRENT bytes did not restore")
        capture.remember("current-restored", sha256=hashlib.sha256(original).hexdigest())
    readback(capture, binary, "restored-current", expected, committed)


def run(capture, binary):
    capture.start()
    capture.stop()
    baseline = state(capture, "baseline")
    mutations = [entry("repeat", "first"), entry("repeat", "last"),
                 entry("erased", "temporary"), entry("erased"), entry("empty", ""),
                 entry("never"), entry("stay", "stable")]
    result = command(capture, binary, "mixed-batch", "write", mutations)
    mixed = state(capture, "mixed-batch")
    publication(capture, baseline, mixed, result, len(mutations))
    expected = {"repeat": "last", "erased": None, "empty": "", "never": None,
                "stay": "stable", "missing": None}
    readback(capture, binary, "mixed-readback", expected, mixed)
    tombstones = [entry("repeat"), entry("stay"), entry("missing")]
    result = command(capture, binary, "tombstone-batch", "write", tombstones)
    deleted = state(capture, "tombstone-batch")
    publication(capture, mixed, deleted, result, len(tombstones), True)
    expected.update(repeat=None, stay=None)
    readback(capture, binary, "tombstone-readback", expected, deleted)
    checksum_refusal(capture, binary, expected, deleted)
    final = state(capture, "final")
    if final["current_sha256"] != deleted["current_sha256"]:
        raise RuntimeError("The final CURRENT differs from the complete committed root")
    return {"complete": True, "mixed_batch_operations": len(mutations),
            "tombstone_batch_operations": len(tombstones), "keys_per_read": len(KEYS),
            "empty_body_preserved": True, "checksum_error_explicit": True,
            "root_publications": 2, "frontier_delta": final["frontier"] - baseline["frontier"],
            "tombstone_new_payload_parts": 0, "current_restored": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", required=True, type=Path)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--command-binary", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-revision", required=True)
    args = parser.parse_args()
    binary = args.command_binary.resolve(strict=True)
    capture = Capture(args)
    save(capture.output / "inputs.json", {"source_revision": args.source_revision,
         "node_binary": str(capture.binary), "node_binary_sha256": file_hash(capture.binary),
         "command_binary": str(binary), "command_binary_sha256": file_hash(binary),
         "driver_sha256": file_hash(Path(__file__)),
         "initialization_driver_sha256": file_hash(Path(__file__).with_name("capture.py")),
         "collection": COLLECTION, "key_prefix": PREFIX, "keys": KEYS,
         "purpose": "Bounded actual product command receipts on a fresh product-created home",
         "claim_limit": "No power-loss or concurrent-reader claim"})
    summary = {"complete": False}
    try:
        summary.update(run(capture, binary))
    except BaseException as failure:
        summary["error"] = str(failure)
        raise
    finally:
        try:
            capture.stop()
        finally:
            summary.update(scratch=str(capture.root), home=str(capture.home),
                           all_owned_processes_stopped=capture.process is None,
                           scratch_retained=True)
            save(capture.output / "summary.json", summary)
            print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
