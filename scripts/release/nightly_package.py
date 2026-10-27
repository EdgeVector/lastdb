#!/usr/bin/env python3
"""Package the exact, matched nightly CLI and daemon without host installation."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tarfile


TARGETS = {
    "aarch64-apple-darwin": ("darwin", "arm64"),
    "x86_64-unknown-linux-gnu": ("linux", "x86_64"),
}
MAX_BINARY_BYTES = 512 * 1024 * 1024


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def binary_version(path):
    result = subprocess.run([str(path), "--version"], check=True, capture_output=True,
                            text=True, timeout=20)
    words = result.stdout.strip().split()
    if len(words) != 2 or words[0] != path.name:
        raise ValueError(f"Unexpected version response from {path.name}")
    return words[1]


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def validate_pair(directory, version):
    binaries = []
    for name in ("lastdb", "lastdbd"):
        path = (directory / name).absolute()
        if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"The release lacks a regular executable {name}")
        if not 0 < path.stat().st_size <= MAX_BINARY_BYTES:
            raise ValueError(f"The release executable {name} exceeds its size limit")
        if binary_version(path) != version:
            raise ValueError(f"The release executable {name} has a different version")
        binaries.append({"name": name, "version": version, "sha256": digest(path),
                         "size": path.stat().st_size})
    return binaries


def validate_architecture(directory, target):
    for name in ("lastdb", "lastdbd"):
        with (directory / name).open("rb") as stream:
            header = stream.read(20)
        if target == "aarch64-apple-darwin":
            valid = header[:4] == b"\xcf\xfa\xed\xfe" and int.from_bytes(header[4:8], "little") == 0x100000c
        else:
            valid = (header[:6] == b"\x7fELF\x02\x01"
                     and int.from_bytes(header[18:20], "little") == 62)
        if not valid:
            raise ValueError(f"The executable {name} does not match {target}")


def archive_pair(path, directory, binaries, epoch):
    # Fixed ownership, mode, order, and timestamps remove archive-only changes.
    # Compiler or SDK changes can still produce different executable bytes.
    with path.open("wb") as stream:
        with gzip.GzipFile(filename="", fileobj=stream, mode="wb", mtime=epoch) as zipped:
            with tarfile.open(fileobj=zipped, mode="w|") as archive:
                for binary in binaries:
                    member = tarfile.TarInfo(binary["name"])
                    member.size, member.mode, member.mtime = binary["size"], 0o755, epoch
                    member.uid = member.gid = 0
                    member.uname = member.gname = ""
                    with (directory / binary["name"]).open("rb") as content:
                        archive.addfile(member, content)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--git-oid", required=True)
    parser.add_argument("--expect-version", required=True)
    parser.add_argument("--source-epoch", type=int, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.git_oid):
        raise ValueError("A full source commit is required")
    if not re.fullmatch(r"\d+\.\d+\.\d+-nightly\.\d{8}\.g[0-9a-f]{9}", args.expect_version):
        raise ValueError("The nightly version is invalid")
    if not args.expect_version.endswith(".g" + args.git_oid[:9]):
        raise ValueError("The version and source commit differ")
    if not 0 <= args.source_epoch <= 0xffffffff:
        raise ValueError("The source timestamp exceeds the archive format")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    binaries = validate_pair(args.bin_dir, args.expect_version)
    validate_architecture(args.bin_dir, args.target)
    archive = args.out_dir / f"lastdb-{args.target}.tar.gz"
    archive_pair(archive, args.bin_dir, binaries, args.source_epoch)
    if archive.stat().st_size > MAX_BINARY_BYTES:
        raise ValueError("The archive exceeds the 512 MiB release asset limit")
    system, architecture = TARGETS[args.target]
    manifest = {
        "schema": "lastdb.mini.bundle.v1", "target": args.target,
        "source_git_oid": args.git_oid, "version": args.expect_version,
        "artifact": {"file": archive.name, "sha256": digest(archive),
                     "size": archive.stat().st_size},
        "binaries": binaries, "platform": {"os": system, "arch": architecture},
        "build_host": {"system": platform.system(), "release": platform.release(),
                       "machine": platform.machine()},
        "signing": "ad-hoc" if system == "darwin" else "none",
    }
    write_json(args.out_dir / f"lastdb-{args.target}.manifest.json", manifest)
    print(f"Matched nightly bundle: {args.target} {args.git_oid} {args.expect_version}")


if __name__ == "__main__":
    main()
