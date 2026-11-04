#!/usr/bin/env python3
"""Run the frozen apps before a main-only CI publisher signs their declarations."""

import argparse
from pathlib import Path
import tempfile

from nightly_assets import digest, document, validate_bundle
from nightly_package import write_json
from nightly_probe import OwnedNode, operations, unpack
from stable_apps import prove
from stable_plan import load


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--bundles", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = load(args.plan)
    args.output.mkdir(parents=True)
    target = "x86_64-unknown-linux-gnu"
    validate_bundle(args.bundles, target, plan)
    bundle = args.bundles / f"lastdb-{target}.tar.gz"
    manifest = document(args.bundles / f"lastdb-{target}.manifest.json")
    node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), args.output,
                     plan["version"], enable_search=True)
    try:
        unpack(bundle, manifest, node.binaries, plan["version"], plan["source"])
        observed = operations(node)
        apps = prove(node, plan, args.output)
    finally:
        node.close()
    write_json(args.output / "app-compatibility.json", {
        "schema": "lastdb.app.compatibility.proof.v1", "source": plan["source"],
        "run_url": plan["run_url"], "lastdb_source": plan["source"], "lastdb_build": plan["version"],
        "api_version": observed["api_version"], "outbox_format_version": observed["current_outbox_format_version"],
        "bundle_sha256": digest(bundle), "apps": apps, "complete": True,
        "daemon_stopped": True, "home_removed": True})


if __name__ == "__main__":
    main()
