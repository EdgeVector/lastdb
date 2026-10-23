#!/usr/bin/env python3
"""Use the older physical command against the completed owned counted home."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import stat
import struct
import subprocess
import time


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def inventory(root):
    stack, result, total, deadline = [(root, 0)], {}, 0, time.monotonic() + 20
    while stack:
        parent, depth = stack.pop()
        if depth > 8:
            raise ValueError("The generated physical inventory exceeds depth eight")
        for path in parent.iterdir():
            if path.is_symlink() or path.stat().st_uid != os.getuid() or time.monotonic() > deadline:
                raise ValueError("The generated physical inventory is linked, foreign, or too slow")
            if path.is_dir():
                stack.append((path, depth + 1))
            elif path.is_file():
                info = path.stat()
                total += info.st_size
                if len(result) >= 20000 or total > 64 * 1024 * 1024:
                    raise ValueError("The generated physical inventory exceeds its finite cap")
                result[str(path.relative_to(root))] = {"bytes": info.st_size, "sha256": digest(path)}
            else:
                raise ValueError("The physical inventory contains a non-file object")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    run = args.run.resolve(strict=True)
    ownership = json.loads((run / "ownership.json").read_text())
    summary = json.loads((run / "summary.json").read_text())
    root = Path(ownership["scratch"])
    info = root.stat()
    if (root.is_symlink() or root.resolve(strict=True) != root
            or root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True)
            or stat.S_IMODE(info.st_mode) != 0o700 or info.st_uid != os.getuid()
            or (info.st_dev, info.st_ino, info.st_uid) != (
                ownership["device"], ownership["inode"], ownership["owner_uid"])
            or ownership.get("primary_data") is not False
            or json.loads((root / ".snapshot-capture-owner.json").read_text()) != ownership
            or summary.get("complete") is not True or summary.get("all_owned_processes_stopped") is not True):
        raise ValueError("Use one exact complete and stopped generated capture")
    home = root / "home" / "data" / "disk-pack"
    if home.resolve(strict=True) != home or ownership["home"] != str(root / "home"):
        raise ValueError("The generated physical home contains an unexpected link")
    if struct.unpack_from("<Q", (home / "CURRENT").read_bytes(), 112)[0] & 512 == 0:
        raise ValueError("The older-program refusal requires the new counted format")
    binary = args.binary.resolve(strict=True)
    result = {"complete": False, "owner": ownership, "binary": str(binary),
              "binary_sha256": digest(binary), "driver_sha256": digest(Path(__file__)), "commands": []}
    target = run / "older-program-refusal.json"
    before = inventory(home)
    environment = {name: value for name, value in os.environ.items()
                   if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_SENTRY"))}
    try:
        for verb in ("read", "write"):
            path = root / f"older-feature-{verb}.json"
            request = ({"coordinates": [{"collection": "receipt-pruning-large-generated", "key": "edge\0key-000000-"}]}
                       if verb == "read" else {"collection": "receipt-pruning-large-generated",
                                                "key": "edge\0key-000000-", "body_hex": "00"})
            if verb == "write":
                path.write_text(json.dumps(request, sort_keys=True) + "\n")
            else:
                save(path, request)
            argv = [str(binary), verb, "--home", str(home), "--input", str(path)]
            started = time.monotonic_ns()
            process = subprocess.run(argv, env=environment, stdin=subprocess.DEVNULL, capture_output=True,
                                     text=True, timeout=60, check=False)
            receipt = {"argv": argv, "exit_code": process.returncode, "stdout": process.stdout,
                       "stderr": process.stderr, "elapsed_nanos": time.monotonic_ns() - started}
            result["commands"].append(receipt)
            if (process.returncode == 0 or process.stdout.strip()
                    or "unsupported CURRENT feature" not in process.stderr):
                raise RuntimeError("The older physical program does not refuse the new feature explicitly")
            if inventory(home) != before:
                raise RuntimeError("The older command changes the counted physical home before refusal")
        result.update(complete=True, read_refused=True, write_refused=True,
                      exact_physical_file_hashes_unchanged=True,
                      physical_files=len(before), physical_bytes=sum(row["bytes"] for row in before.values()))
    except BaseException as failure:
        result["error"] = str(failure)
        raise
    finally:
        save(target, result)
    print(json.dumps({"complete": True, "output": str(target)}))


if __name__ == "__main__":
    main()
