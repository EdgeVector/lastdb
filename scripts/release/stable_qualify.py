#!/usr/bin/env python3
"""Qualify the packaged release for a full uninterrupted hour in CI."""

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import tempfile
import time

from nightly_assets import digest, document
from nightly_package import write_json
from nightly_probe import OwnedNode, operations, read, unpack, write
from home_format import HOME_FORMAT
from stable_apps import prove
from stable_plan import load

HOUR = 3600


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--bundles", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = load(args.plan)
    args.output.mkdir(parents=True)
    target = "x86_64-unknown-linux-gnu"
    bundle = args.bundles / f"lastdb-{target}.tar.gz"
    manifest = document(args.bundles / f"lastdb-{target}.manifest.json")
    node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), args.output, plan["version"],
                     enable_search=True)
    try:
        unpack(bundle, manifest, node.binaries, plan["version"], plan["source"])
        version = operations(node)
        apps = prove(node, plan, args.output)
        started = datetime.now(timezone.utc)
        beginning = time.monotonic()
        latencies = []
        # PRs exercise the actual command path briefly, but cannot produce a
        # qualified result. There is no input that shortens a publishing run.
        duration = HOUR if plan["publish"] else 30
        while True:
            if node.process.poll() is not None:
                raise RuntimeError("The daemon exited during the quiet window")
            tick = time.monotonic()
            if node.call("/health").get("status") != "ok":
                raise RuntimeError("The daemon was unhealthy during qualification")
            latencies.append(time.monotonic() - tick)
            write(node, range(8))
            read(node, set(range(8)), ranged=True)
            elapsed = time.monotonic() - beginning
            print(f"Stable qualification: {elapsed:.0f}/{duration}s; {len(latencies)} healthy samples", flush=True)
            if elapsed >= duration:
                break
            time.sleep(min(30, duration - elapsed))
        elapsed = time.monotonic() - beginning
        p95 = sorted(latencies)[math.ceil(len(latencies) * .95) - 1]
        if p95 > 2:
            raise RuntimeError("Qualification health P95 exceeded the existing two-second gate")
        node.stop()
        node.start()
        read(node, set(range(8)))
        node.stop(crash=True)
        node.start()
        read(node, set(range(8)), ranged=True)
    finally:
        node.close()
    write_json(args.output / "qualification.json", {
        "schema": "lastdb.stable.qualification.v1", "source": plan["source"],
        "version": plan["version"], "bundle_sha256": digest(bundle), "api_version": version["api_version"],
        "home_format": HOME_FORMAT,
        "started_at": started.isoformat(), "ended_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed, "health_p95_seconds": p95, "healthy_samples": len(latencies),
        "qualified": plan["publish"] and elapsed >= HOUR, "apps": apps,
        "durable_operations": True, "recovery": True, "daemon_stopped": True, "home_removed": True,
        "run_url": plan["run_url"]})


if __name__ == "__main__":
    main()
