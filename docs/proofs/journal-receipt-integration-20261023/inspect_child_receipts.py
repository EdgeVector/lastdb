#!/usr/bin/env python3
"""Record actual counted-receipt headers from a stopped generated product home."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import time


PAGE_BYTES = 16384
MAX_PART_BYTES = 8 * 1024 * 1024


def load(path):
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("The small operation receipt exceeds one MiB")
    return json.loads(path.read_text())


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def inspect(home):
    current_path = home / "CURRENT"
    if (home.is_symlink() or home.resolve(strict=True) != home
            or current_path.resolve(strict=True) != current_path or current_path.stat().st_size != 4096):
        raise ValueError("Inspect only the exact complete generated native home")
    current = current_path.read_bytes()
    if current[128:160].hex() != sha256(current[:128] + bytes(32) + current[160:]):
        raise ValueError("The emitted CURRENT checksum differs")
    generation = struct.unpack_from("<Q", current, 16)[0]
    features = struct.unpack_from("<Q", current, 112)[0]
    if features != 1021:
        raise ValueError("The current product output does not use the expected counted native features")
    path = home / "generations" / str(generation) / "part-00000000"
    if path.resolve(strict=True) != path or not 0 < path.stat().st_size <= MAX_PART_BYTES:
        raise ValueError("The generated receipt part exceeds its finite bound")
    raw = path.read_bytes()
    if len(raw) % PAGE_BYTES:
        raise ValueError("The emitted receipt part has a partial page")
    pages = []
    parts, locator_objects, locator_templates = 0, set(), set()
    for number in range(len(raw) // PAGE_BYTES):
        page = raw[number * PAGE_BYTES:(number + 1) * PAGE_BYTES]
        checksum = sha256(page[:80] + bytes(32) + page[112:])
        prefix = page[128:256]
        count = struct.unpack_from("<I", prefix, 32)[0]
        embedded = struct.unpack_from("<I", prefix, 36)[0]
        if (page[:8] != b"LDPAGE1\0" or page[10] != 11 or page[80:112].hex() != checksum
                or struct.unpack_from("<Q", page, 40)[0] != number
                or struct.unpack_from("<Q", prefix, 0)[0] != generation
                or struct.unpack_from("<Q", prefix, 88)[0] != features
                or struct.unpack_from("<Q", prefix, 96)[0] != 1
                or any(prefix[104:]) or embedded != (4096 if number == 0 else 0)
                or count > (150 if number == 0 else 201)
                or struct.unpack_from("<Q", prefix, 72)[0] != len(raw) // PAGE_BYTES):
            raise ValueError("An emitted receipt page differs from semantic version one")
        if number == 0 and page[256:4352] != current:
            raise ValueError("The emitted receipt embeds a different CURRENT")
        for at in range(count):
            start = 256 + embedded + at * 80
            entry = page[start:start + 80]
            if len(entry) != 80 or struct.unpack_from("<Q", entry, 24)[0] == 0:
                raise ValueError("An emitted dependency has no positive live count")
            if entry[0] == 2:
                locator_objects.add(entry[8:24].hex())
                if struct.unpack_from("<I", entry, 4)[0] == 0:
                    locator_templates.add(entry[8:24].hex())
        parts += count
        pages.append({"number": number, "sha256": sha256(page), "dependencies": count,
                      "semantic_version": 1, "version_bytes_hex": prefix[96:104].hex()})
    if parts != struct.unpack_from("<Q", raw, 144)[0] or locator_objects != locator_templates:
        raise ValueError("The emitted counts or locator template coordinates differ")
    return {"home": str(home), "generation": generation, "features": features,
            "current_sha256": sha256(current), "receipt_part_sha256": sha256(raw),
            "receipt_part_bytes": len(raw), "dependencies": parts, "pages": pages,
            "locator_objects_with_part_zero": sorted(locator_templates)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    args = parser.parse_args()
    run = args.run.resolve(strict=True)
    owner, summary = load(run / "ownership.json"), load(run / "summary.json")
    root = Path(owner["scratch"])
    info = root.stat()
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700
            or (info.st_dev, info.st_ino, info.st_uid) != (owner["device"], owner["inode"], owner["owner_uid"])
            or owner.get("primary_data") is not False or owner.get("home") != str(root / "home")
            or load(root / ".snapshot-capture-owner.json") != owner
            or summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True):
        raise ValueError("Inspect only a complete stopped exact owned generated home")
    homes = [root / "home/data/disk-pack"]
    homes += [root / name for name in ("earlier-backup", "restored-earlier")
              if (root / name / "CURRENT").exists()]
    output = run / "receipt-format.json"
    if output.exists():
        raise ValueError("Preserve each preceding format receipt")
    inputs = load(run / "inputs.json")
    value = {"complete": True, "at_unix_ns": time.time_ns(), "run": str(run),
             "source_revision": inputs.get("revision", inputs.get("source_revision")),
             "helper_sha256": sha256(Path(__file__).read_bytes()),
             "ownership_sha256": sha256((run / "ownership.json").read_bytes()),
             "summary_sha256": sha256((run / "summary.json").read_bytes()),
             "observed": [inspect(home) for home in homes],
             "scope": "Actual emitted headers and positive dependency counts; no altered bytes or prototype-refusal runtime claim"}
    output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"complete": True, "receipt": str(output), "observed_homes": len(homes)}))


if __name__ == "__main__":
    main()
