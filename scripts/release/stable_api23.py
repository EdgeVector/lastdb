#!/usr/bin/env python3
"""Backfill API 23 app evidence using an attested nightly on a generated CI home."""

import argparse
from pathlib import Path
import tempfile

from nightly_assets import document
from nightly_package import write_json
from nightly_probe import OwnedNode, unpack
from nightly_remote import download, prior_plan, release_for_tag
from stable_apps import prove
from stable_plan import load

TAG = "nightly-20261103-b252ab1df"
APP_COMMITS = {"brain": "6a3e326669e5d876924feaed7df2043b83318d79",
               "lastsecrets": "310460ad282152d4f810c403af503babec4ce33c"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--bundles", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = load(args.plan)
    args.output.mkdir(parents=True)
    old = release_for_tag(TAG)
    if old is None:
        raise ValueError("The immutable API 23 nightly no longer exists")
    previous = prior_plan(old, plan)
    target = "x86_64-unknown-linux-gnu"
    with tempfile.TemporaryDirectory(prefix="api23-assets-") as temporary:
        folder = Path(temporary)
        download(old, folder)
        node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), args.output,
                         previous["version"], enable_search=True)
        try:
            unpack(folder / f"lastdb-{target}.tar.gz", document(folder / f"lastdb-{target}.manifest.json"),
                   node.binaries, previous["version"], previous["source"])
            version = node.start()
            if version.get("api_version") != 23:
                raise ValueError("The historical package is not API 23")
            # The paired API 23 CLI/daemon run all app operations. Only the
            # read-only declaration preparation uses the new CLI, preserving
            # its complete typed-manifest and owned-schema checks.
            prepare = args.output / "prepare-bin"
            prepare.mkdir()
            unpack(args.bundles / f"lastdb-{target}.tar.gz",
                   document(args.bundles / f"lastdb-{target}.manifest.json"),
                   prepare, plan["version"], plan["source"])
            app_plan = {**plan, "apps": {**plan["apps"], **APP_COMMITS}}
            apps = prove(node, app_plan, args.output, prepare / "lastdb")
            if any(proof["api_version"] != 23 for proof in apps.values()):
                raise ValueError("An app declaration reports another API")
        finally:
            node.close()
    write_json(args.output / "compatibility.json", {
        "schema": "lastdb.app.compatibility.v1", "api_version": 23, "apps": apps,
        "source": previous["source"], "version": previous["version"], "release_tag": TAG,
        "run_url": plan["run_url"], "complete": True, "daemon_stopped": True, "home_removed": True})


if __name__ == "__main__":
    main()
