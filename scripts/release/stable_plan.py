#!/usr/bin/env python3
"""Freeze, gate, package, and compatibility-prove a stable LastDB release."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.error import HTTPError

from nightly_plan import RELEASE_GATE_WAIT_SECONDS, _capture, wait_gates

TAP = "EdgeVector/homebrew-lastdb"
APPS = ("brain", "lastsecrets", "search")
HOUR = 3600


def app_sources():
    from stable_app_publish import catalog
    # ci-required keeps this file equal to the Rust constants (scripts/lints/api_surface.py).
    api_version = json.loads(Path("lastdb_node/api-surface.json").read_text())["api_version"]
    outbox = re.search(r"pub const CURRENT_OUTBOX_FORMAT_VERSION: u32 = ([0-9]+);",
                       Path("lastdb_node/src/version.rs").read_text())
    if outbox is None:
        raise ValueError("The source has no definite outbox interface")
    try:
        listing = catalog(api_version)
    except HTTPError as error:
        if error.code not in (403, 404):
            raise
        listing = {"apps": []}
    result = {}
    for app in APPS:
        entries = [row for row in listing["apps"] if row.get("app_id") == app and row.get("status") == "listed"
                   and row.get("source") == f"https://github.com/EdgeVector/{app}.git"
                   and row.get("outbox_format_version") in (None, int(outbox[1]))]
        result[app] = (max(entries, key=lambda row: row["published_at"])["sha"] if entries
                       else api(f"repos/EdgeVector/{app}/commits/main")["sha"])
    return result


def api(path, method="GET", payload=None):
    if not path.startswith(("repos/EdgeVector/lastdb/", "repos/" + TAP + "/",
                            *(f"repos/EdgeVector/{app}/" for app in APPS))):
        raise ValueError("Release request outside the fixed repositories")
    command = ["gh", "api", "--method", method, path]
    body = b""
    if payload is not None:
        command += ["--input", "-"]
        body = json.dumps(payload).encode()
    return json.loads(_capture(command, body) or "null")


def load(path):
    result = json.loads(Path(path).read_text())
    if (result.get("repository") != "EdgeVector/lastdb"
            or not re.fullmatch(r"[0-9a-f]{40}", result.get("source", ""))
            or not re.fullmatch(r"\d+\.\d+\.\d+", result.get("version", ""))
            or result.get("tag") != "v" + result["version"]
            or type(result.get("publish")) is not bool):
        raise ValueError("Invalid stable plan")
    return result


def make(output):
    source = _capture(["git", "rev-parse", "HEAD"]).strip()
    event = os.environ["GITHUB_EVENT_NAME"]
    publish = event != "pull_request"
    if publish and os.environ.get("GITHUB_REF") != "refs/heads/main":
        raise ValueError("Stable publication only runs from main")
    if _capture(["git", "status", "--porcelain"]).strip():
        raise ValueError("Release source must be clean")
    formula = api(f"repos/{TAP}/contents/Formula/lastdb.rb?ref=main")
    text = base64.b64decode(formula["content"]).decode()
    match = re.search(r'^\s*version "(\d+)\.(\d+)\.(\d+)"', text, re.M)
    if match is None:
        raise ValueError("The stable formula has no explicit numeric version")
    version = ".".join((*match.groups()[:2], str(int(match[3]) + 1)))
    plan = {"repository": "EdgeVector/lastdb", "source": source, "version": version,
            "base": os.environ.get("BASE_SHA") or _capture(["git", "rev-parse", "HEAD^"]).strip(),
            "tag": "v" + version, "publish": publish, "event_name": event,
            "epoch": int(_capture(["git", "show", "-s", "--format=%ct", source])),
            "run_url": f"https://github.com/EdgeVector/lastdb/actions/runs/{os.environ['GITHUB_RUN_ID']}",
            "apps": app_sources()}
    marker = f"<!-- lastdb-stable:v1 source={source} "
    rows = api(f"repos/{TAP}/releases?per_page=100")
    skip = publish and any(not row["draft"] and not row["prerelease"]
                           and marker in row.get("body", "") for row in rows)
    Path(output).write_text(json.dumps(plan, indent=2) + "\n")
    with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
        for key in ("source", "version", "tag", "base"):
            stream.write(f"{key}={plan[key]}\n")
        stream.write(f"skip={str(skip).lower()}\n")
    return plan


def package_bundle(plan_path, target, bin_dir, output):
    from nightly_package import MAX_BINARY_BYTES, archive_pair, digest, validate_architecture, validate_pair, write_json
    plan = load(plan_path)
    output.mkdir(parents=True)
    binaries = validate_pair(bin_dir, plan["version"])
    validate_architecture(bin_dir, target)
    bundle = output / f"lastdb-{target}.tar.gz"
    archive_pair(bundle, bin_dir, binaries, plan["epoch"])
    if bundle.stat().st_size > MAX_BINARY_BYTES:
        raise ValueError("Release archive exceeds the package bound")
    write_json(output / f"lastdb-{target}.manifest.json", {
        "schema": "lastdb.mini.bundle.v1", "target": target,
        "source_git_oid": plan["source"], "version": plan["version"], "binaries": binaries,
        "artifact": {"file": bundle.name, "size": bundle.stat().st_size, "sha256": digest(bundle)}})


def prove_compatibility(plan_path, bundles, output):
    from nightly_assets import digest, document, validate_bundle
    from nightly_package import write_json
    from nightly_probe import OwnedNode, operations, unpack
    from stable_apps import prove
    plan = load(plan_path)
    output.mkdir(parents=True)
    target = "x86_64-unknown-linux-gnu"
    validate_bundle(bundles, target, plan)
    bundle = bundles / f"lastdb-{target}.tar.gz"
    manifest = document(bundles / f"lastdb-{target}.manifest.json")
    node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), output,
                     plan["version"], enable_search=True)
    try:
        unpack(bundle, manifest, node.binaries, plan["version"], plan["source"])
        observed = operations(node)
        apps = prove(node, plan, output)
    finally:
        node.close()
    write_json(output / "app-compatibility.json", {
        "schema": "lastdb.app.compatibility.proof.v1", "source": plan["source"],
        "run_url": plan["run_url"], "lastdb_source": plan["source"], "lastdb_build": plan["version"],
        "api_version": observed["api_version"], "outbox_format_version": observed["current_outbox_format_version"],
        "bundle_sha256": digest(bundle), "apps": apps, "complete": True,
        "daemon_stopped": True, "home_removed": True})


def qualify_release(plan_path, bundles, output):
    from nightly_assets import digest, document
    from nightly_package import write_json
    from nightly_probe import OwnedNode, operations, read, unpack, write
    from home_format import HOME_FORMAT
    from stable_apps import prove
    plan = load(plan_path)
    output.mkdir(parents=True)
    target = "x86_64-unknown-linux-gnu"
    bundle = bundles / f"lastdb-{target}.tar.gz"
    manifest = document(bundles / f"lastdb-{target}.manifest.json")
    node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), output, plan["version"],
                     enable_search=True)
    try:
        unpack(bundle, manifest, node.binaries, plan["version"], plan["source"])
        version = operations(node)
        apps = prove(node, plan, output)
        if plan["publish"] and any(not proof["installed_from_registry"] for proof in apps.values()):
            raise RuntimeError("Publish the validated bootstrap app declarations, then retry qualification; no hour started")
        started = datetime.now(timezone.utc)
        beginning = time.monotonic()
        latencies = []
        # PRs exercise the actual command path briefly, but cannot produce a
        # qualified result. There is no input that shortens a publishing run.
        duration = HOUR if plan["publish"] else 30
        required_samples = duration // 30 + 1
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
            if elapsed >= duration and len(latencies) >= required_samples:
                break
            next_sample = beginning + len(latencies) * 30
            time.sleep(max(0, min(30, next_sample - time.monotonic())))
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
    write_json(output / "qualification.json", {
        "schema": "lastdb.stable.qualification.v1", "source": plan["source"],
        "version": plan["version"], "bundle_sha256": digest(bundle), "api_version": version["api_version"],
        "home_format": HOME_FORMAT,
        "outbox_format_version": version["current_outbox_format_version"],
        "started_at": started.isoformat(), "ended_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed, "health_p95_seconds": p95, "healthy_samples": len(latencies),
        "qualified": plan["publish"] and elapsed >= HOUR, "apps": apps,
        "durable_operations": True, "recovery": True, "daemon_stopped": True, "home_removed": True,
        "run_url": plan["run_url"]})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    plan_cmd = sub.add_parser("plan")
    plan_cmd.add_argument("--plan", type=Path, required=True)
    gates_cmd = sub.add_parser("gates")
    gates_cmd.add_argument("--plan", type=Path, required=True)
    gates_cmd.add_argument("--wait-seconds", type=int, default=RELEASE_GATE_WAIT_SECONDS)
    pkg_cmd = sub.add_parser("package")
    pkg_cmd.add_argument("--plan", type=Path, required=True)
    pkg_cmd.add_argument("--target", choices=("aarch64-apple-darwin", "x86_64-unknown-linux-gnu"), required=True)
    pkg_cmd.add_argument("--bin-dir", type=Path, required=True)
    pkg_cmd.add_argument("--output", type=Path, required=True)
    for name in ("compatibility", "qualify"):
        bundle_cmd = sub.add_parser(name)
        bundle_cmd.add_argument("--plan", type=Path, required=True)
        bundle_cmd.add_argument("--bundles", type=Path, required=True)
        bundle_cmd.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(json.dumps(make(args.plan), sort_keys=True))
    elif args.command == "gates":
        print(json.dumps(wait_gates(load(args.plan), args.wait_seconds), sort_keys=True))
    elif args.command == "package":
        package_bundle(args.plan, args.target, args.bin_dir, args.output)
    elif args.command == "compatibility":
        prove_compatibility(args.plan, args.bundles, args.output)
    else:
        qualify_release(args.plan, args.bundles, args.output)


if __name__ == "__main__":
    main()
