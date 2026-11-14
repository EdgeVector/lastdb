#!/usr/bin/env python3
"""Download the latest published LastDB nightly Linux client.

Check out EdgeVector/lastdb, including at a nightly tag, and run:

    python3 scripts/release/published_client.py --out-dir DIR

The command resolves the release with nightly_remote.latest_nightly: the
first non-draft prerelease whose tag is nightly-YYYYMMDD-<sha9>. It then
downloads that release and prints:

    version=<core>-nightly.<YYYYMMDD>.g<sha9>
    tarball=<DIR>/lastdb-x86_64-unknown-linux-gnu.tar.gz

The tag for that same release is nightly-<YYYYMMDD>-<sha9>. A missing
release or a failed download raises. This command does not compile a client.
`gh` must be able to read EdgeVector/lastdb releases. contents: read on this
repository's GITHUB_TOKEN is enough.
"""

import argparse
import os
from datetime import datetime
from pathlib import Path
import re
import shutil
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

from nightly_assets import INDEX, document
from nightly_plan import CORE_VERSION, REPOSITORY, SHA
from nightly_remote import download, latest_nightly


LINUX = "lastdb-x86_64-unknown-linux-gnu.tar.gz"


def published_version(release, index):
    """Return the release version when the index names that release tag."""
    if not isinstance(release, dict) or not isinstance(index, dict):
        raise ValueError("The published nightly version is missing")
    version = index.get("version") if isinstance(index.get("version"), str) else ""
    match = re.fullmatch(rf"{CORE_VERSION}-nightly\.([0-9]{{8}})\.g([0-9a-f]{{9}})", version)
    source = index.get("source_git_oid")
    tag = release.get("tag_name")
    if (index.get("schema") != "lastdb.nightly.release.v1" or index.get("repository") != REPOSITORY
            or match is None or not isinstance(source, str) or not SHA.fullmatch(source)
            or index.get("tag") != tag or match[2] != source[:9]
            or tag != f"nightly-{match[1]}-{source[:9]}"):
        raise ValueError("The published nightly version does not match its tag")
    datetime.strptime(match[1], "%Y%m%d")
    return version


def place(source, target):
    if source.is_symlink() or not source.is_file():
        raise ValueError("The nightly Linux client archive is missing")
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o644)
    with os.fdopen(descriptor, "wb") as outgoing, source.open("rb") as incoming:
        shutil.copyfileobj(incoming, outgoing, 1024 * 1024)
        outgoing.flush()
        os.fsync(outgoing.fileno())
    if target.is_symlink() or target.stat().st_size != source.stat().st_size:
        raise ValueError("The nightly Linux client archive copy is incomplete")


def prepare_folder(folder):
    folder = Path(folder)
    if folder.is_symlink() or (folder.exists() and not folder.is_dir()):
        raise ValueError("The nightly client output must be a directory")
    folder.mkdir(parents=True, exist_ok=True)
    if folder.is_symlink() or any(folder.iterdir()):
        raise ValueError("The nightly client output directory must be empty")
    return folder


def fetch(folder):
    folder = prepare_folder(folder)
    release = latest_nightly()
    if not isinstance(release, dict):
        raise ValueError("No published nightly release")
    root = os.environ.get("RUNNER_TEMP") or None
    with tempfile.TemporaryDirectory(prefix="published-nightly-", dir=root) as temporary:
        cache = Path(temporary)
        download(release, cache)
        version = published_version(release, document(cache / INDEX))
        place(cache / LINUX, folder / LINUX)
    return version, folder / LINUX


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    version, tarball = fetch(args.out_dir)
    print(f"version={version}")
    print(f"tarball={tarball}")


if __name__ == "__main__":
    main()
