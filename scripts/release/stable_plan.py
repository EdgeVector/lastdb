#!/usr/bin/env python3
"""Freeze, gate, package, and compatibility-prove a stable LastDB release."""

from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import re
import tempfile
from urllib.error import HTTPError

from nightly_plan import RELEASE_GATE_WAIT_SECONDS, _capture, wait_gates

TAP = "EdgeVector/homebrew-lastdb"
APPS = ("brain", "lastsecrets", "search")


def app_sources():
    from stable_registry import catalog
    text = Path("lastdb_node/src/lastdb_uds/uds_router.rs").read_text()
    match = re.search(r"pub const API_VERSION: u32 = ([0-9]+);", text)
    if match is None:
        raise ValueError("The source has no definite API version")
    outbox = re.search(r"pub const CURRENT_OUTBOX_FORMAT_VERSION: u32 = ([0-9]+);",
                       Path("lastdb_node/src/version.rs").read_text())
    if outbox is None:
        raise ValueError("The source has no definite outbox interface")
    try:
        listing = catalog(int(match[1]))
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
    compat_cmd = sub.add_parser("compatibility")
    compat_cmd.add_argument("--plan", type=Path, required=True)
    compat_cmd.add_argument("--bundles", type=Path, required=True)
    compat_cmd.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(json.dumps(make(args.plan), sort_keys=True))
    elif args.command == "gates":
        print(json.dumps(wait_gates(load(args.plan), args.wait_seconds), sort_keys=True))
    elif args.command == "package":
        package_bundle(args.plan, args.target, args.bin_dir, args.output)
    else:
        prove_compatibility(args.plan, args.bundles, args.output)


if __name__ == "__main__":
    main()
