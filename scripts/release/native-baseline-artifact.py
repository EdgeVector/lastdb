#!/usr/bin/env python3
"""Verify and extract a published matched baseline pair for native CI."""

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import tarfile
import zipfile


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.archive.read_bytes()
    require(hashlib.sha256(raw).hexdigest() == args.sha256, "Baseline ZIP checksum differs")
    manifest_name = "lastdb-aarch64-apple-darwin.manifest.json"
    archive_name = "lastdb-aarch64-apple-darwin.tar.gz"
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        require(set(archive.namelist()) == {manifest_name, archive_name}, "Unexpected baseline ZIP members")
        manifest = json.loads(archive.read(manifest_name))
        payload = archive.read(archive_name)
    require(manifest["source_git_oid"] == args.source and manifest["target"] == "aarch64-apple-darwin",
            "Baseline source or architecture differs")
    require(hashlib.sha256(payload).hexdigest() == manifest["artifact"]["sha256"], "Baseline tar checksum differs")
    expected = {item["name"]: item for item in manifest["binaries"]}
    require(set(expected) == {"lastdb", "lastdbd"}, "Baseline program set differs")
    args.output.mkdir(mode=0o700)
    seen = set()
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as tar:
        for member in tar:
            if member.name == "." and member.isdir():
                continue
            name = member.name.removeprefix("./")
            require(name in expected and name not in seen and member.isfile(), "Unexpected baseline tar member")
            require(0 < member.size < 512 * 1024 * 1024, "Baseline program size exceeds bound")
            body = tar.extractfile(member).read(member.size + 1)
            require(hashlib.sha256(body).hexdigest() == expected[name]["sha256"], "Baseline program checksum differs")
            descriptor = os.open(args.output / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o755)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(body)
            seen.add(name)
    require(seen == set(expected), "Baseline pair is incomplete")
    print("Verified the published baseline pair for " + args.source)


if __name__ == "__main__":
    main()
