#!/usr/bin/env python3
"""Check real node and physical-writer receipt features before any profile."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import sys

sys.dont_write_bytecode = True
from reuse import original
from benchmark_support import file_hash, save
from retention_capture import body, command, key, mutation, readback
from auto_retained import readback as logical_readback


def feature(capture, label, expected):
    path = capture.home / "data/disk-pack/CURRENT"
    raw = path.read_bytes()
    if len(raw) != 4096:
        raise ValueError("The generated CURRENT header has the wrong fixed size")
    bits = struct.unpack_from("<Q", raw, 112)[0]
    if bits != (509 | (512 if expected else 0)):
        raise RuntimeError("The real writer receipt capability differs from its exact source variant")
    result = {"features": bits, "counted_bit_512": bool(bits & 512),
              "current_sha256": hashlib.sha256(raw).hexdigest()}
    save(capture.output / (label + "-features.json"), result)
    return result


def run(capture, disk, expected):
    capture.start()
    capture.install()
    capture.flush("capability-schema-ready")
    for first in (0, 32):
        capture.write(list(range(first, first + 32)), 0, create=True)
    capture.flush("capability-node-write-complete")
    logical_readback(capture, "node-generated-sixty-four-keys")
    native = capture.call("/api/db/native-status")["native_disk"]
    if native.get("writer_failed") is not False:
        raise RuntimeError("The node writer reports failure during its capability check")
    capture.stop()
    node = feature(capture, "node-created", expected)
    home = capture.home / "data/disk-pack"
    command(capture, disk, "physical-seed", "write", home, [mutation(number, 0) for number in range(64)])
    physical = feature(capture, "physical-writer", expected)
    readback(capture, disk, home, "physical-capability", {key(number): body(number, 0) for number in range(64)})
    capture.start()
    logical_readback(capture, "node-reopened-after-physical-writer")
    return {"complete": True, "node_created": node, "physical_writer": physical,
            "expected_counted_bit_512": expected, "real_public_and_physical_writes": True,
            "exact_ranges_and_node_reopen_verified": True, "native_after_node_writes": native}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "disk-binary", "scratch", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--expected-counted", choices=("yes", "no"), required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard exclusive lock for actual writer capability checks")
    capture = original("benchmark").Benchmark(args)
    summary = {"complete": False, "capability_driver_sha256": file_hash(Path(__file__)),
               "disk_binary_sha256": file_hash(args.disk_binary)}
    try:
        summary.update(run(capture, args.disk_binary, args.expected_counted == "yes"))
    except BaseException as failure:
        summary["error"] = str(failure)
        raise
    finally:
        try:
            capture.stop()
        finally:
            capture.save_receipts()
            summary.update(all_owned_processes_stopped=capture.process is None, scratch=str(capture.root),
                           home=str(capture.home), scratch_retained=True)
            save(capture.output / "summary.json", summary)
    print(json.dumps({"capability_complete": True, "counted": args.expected_counted}), flush=True)


if __name__ == "__main__":
    main()
