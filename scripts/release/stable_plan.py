#!/usr/bin/env python3
"""Freeze, gate, and compatibility-prove a stable LastDB release."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
from urllib.error import HTTPError

from nightly_package import write_json
from nightly_plan import RELEASE_GATE_WAIT_SECONDS, _capture, wait_gates

TAP = "EdgeVector/homebrew-lastdb"
APPS = ("brain", "lastsecrets", "search")
HOUR = 3600


def surface():
    # ci-required keeps this file equal to the Rust constants (scripts/lints/api_surface.py).
    return json.loads(Path("lastdb_node/api-surface.json").read_text())


PROFILE = Path(__file__).resolve().parents[2] / "lastdb_node" / "environments.json"


def download(url):
    import urllib.request
    with urllib.request.urlopen(url, timeout=30) as response:
        raw = response.read(2 * 1024 ** 2 + 1)
    if len(raw) > 2 * 1024 ** 2:
        raise ValueError("Registry response exceeds the signed-list bound")
    return raw


def catalog(api_version):
    """Read the signed stable list for one API version."""
    import hashlib
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    prod = json.loads(PROFILE.read_text())["environments"]["prod"]
    base = prod["registry_index"]
    raw = download(f"{base}/stable/api/{api_version}.json")
    envelope = json.loads(download(f"{base}/stable/api/{api_version}.json.sig"))
    digest = hashlib.sha256(raw).hexdigest()
    key = base64.b64decode(prod["registry_list_key"], validate=True)
    if (envelope.get("alg") != "ed25519" or envelope.get("payload_sha256") != digest
            or envelope.get("key_id") != hashlib.sha256(key).hexdigest()):
        raise ValueError("Invalid registry list signature envelope")
    Ed25519PublicKey.from_public_bytes(key).verify(
        base64.b64decode(envelope["sig"], validate=True), digest.encode())
    listing = json.loads(raw)
    if (listing.get("api_version") != api_version or listing.get("channel") != "stable"
            or listing.get("list_version") != 1):
        raise ValueError("Registry list belongs to another interface")
    return listing


def listed_apps(api_version):
    """The newest listed commit of each app in the signed stable list for one API version."""
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
        if entries:
            result[app] = max(entries, key=lambda row: row["published_at"])["sha"]
    return result


def app_sources():
    listed = listed_apps(surface()["api_version"])
    return {app: listed.get(app) or api(f"repos/EdgeVector/{app}/commits/main")["sha"] for app in APPS}


def installed_app_sources(current):
    """App commits that users can still have installed across MIN_SUPPORTED_API_VERSION..API_VERSION."""
    versions = surface()
    result = {}
    for api_version in range(versions["min_supported_api_version"], versions["api_version"] + 1):
        listed = listed_apps(api_version)
        if len(listed) == len(APPS) and listed != current and listed not in result.values():
            result[str(api_version)] = listed
    return result


def api(path, method="GET", payload=None):
    if not path.startswith(("repos/EdgeVector/lastdb/", "repos/" + TAP + "/",
                            *(f"repos/EdgeVector/{app}/" for app in APPS))):
        raise ValueError("Release request outside the fixed repositories")
    cmd = ["gh", "api", "--method", method, path]
    body = b""
    if payload is not None:
        cmd += ["--input", "-"]
        body = json.dumps(payload).encode()
    return json.loads(_capture(cmd, body) or "null")


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
    match = re.search(r'^\s*version "(\d+)\.(\d+)\.(\d+)"', base64.b64decode(formula["content"]).decode(), re.M)
    if match is None:
        raise ValueError("The stable formula has no explicit numeric version")
    version = ".".join((*match.groups()[:2], str(int(match[3]) + 1)))
    plan = {"repository": "EdgeVector/lastdb", "source": source, "version": version,
            "base": os.environ.get("BASE_SHA") or _capture(["git", "rev-parse", "HEAD^"]).strip(),
            "tag": "v" + version, "publish": publish, "event_name": event,
            "epoch": int(_capture(["git", "show", "-s", "--format=%ct", source])),
            "run_url": f"https://github.com/EdgeVector/lastdb/actions/runs/{os.environ['GITHUB_RUN_ID']}",
            "apps": app_sources()}
    plan["installed_apps"] = {} if publish else installed_app_sources(plan["apps"])
    marker = f"<!-- lastdb-stable:v1 source={source} "
    rows = api(f"repos/{TAP}/releases?per_page=100")
    skip = publish and any(not row["draft"] and not row["prerelease"] and marker in row.get("body", "") for row in rows)
    Path(output).write_text(json.dumps(plan, indent=2) + "\n")
    with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
        for key in ("source", "version", "tag", "base"):
            stream.write(f"{key}={plan[key]}\n")
        stream.write(f"skip={str(skip).lower()}\n")
    return plan


def command(argv, environment, directory, output, name, body=None, timeout=300, retain_output=True, accept_exit=(0,)):
    with (output / (name + ".log")).open("w") as log:
        result = subprocess.run(argv, cwd=directory, env=environment, input=body, text=True,
                                stdout=subprocess.PIPE, stderr=log, timeout=timeout)
        if result.returncode not in accept_exit or len(result.stdout) > 1024 ** 2:
            raise RuntimeError(f"App operation {name} failed; see its CI artifact")
        if retain_output:
            log.write(result.stdout)
    return result.stdout


def source_for(node, plan, app, output, environment, cli):
    prefix = [str(cli), "--data-dir", str(node.home), "app"]
    # The owned home sets ENVIRONMENT=dev so the daemon stays off the cloud.
    # Frozen stable SHAs come from the prod index, so name that index here.
    registry = ["--env", "prod", "--channel", "stable"]
    resolved = subprocess.run([*prefix, "list", app, *registry, "--json"],
                              env=environment, capture_output=True, text=True, timeout=60)
    if resolved.returncode == 0 and json.loads(resolved.stdout).get("sha") == plan["apps"][app]:
        command([*prefix, "install", app, *registry, "--accept-install"],
                environment, output, output, app + "-registry-install", timeout=600)
        active = Path(environment["LASTDB_APPS_ROOT"]) / app / "current"
        receipt = json.loads((active / "lastdb-app-install.json").read_text())
        if receipt["sha"] != plan["apps"][app] or receipt.get("inactive") is not False:
            raise ValueError("The activated registry app differs from the frozen source")
        write_json(output / (app + ".install-receipt.json"), receipt)
        return active / "source", True
    source = output / app
    command(["git", "clone", "--no-checkout", f"https://github.com/EdgeVector/{app}.git", str(source)],
            environment, output, output, app + "-clone")
    command(["git", "checkout", "--detach", plan["apps"][app]], environment, source, output, app + "-checkout")
    manifest = json.loads((source / "lastdb-app.json").read_text())
    for phase in ("prepare", "probe"):
        for index, step in enumerate(manifest["install"][phase]):
            command(step["argv"], environment, source, output, f"{app}-{phase}-{index}",
                    timeout=min(step.get("timeout_s", 300), 600), accept_exit=tuple(step.get("accept_exit", [0])))
    return source, False


def prove_apps(node, plan, output, declare=True):
    observed = node.call("/api/version")
    environment = {**os.environ, **node.environment,
                   "PATH": os.pathsep.join((str(node.binaries), str(node.root / "os-home/.local/bin"), os.environ["PATH"])),
                   "LASTDB_APPS_ROOT": str(node.root / "managed-apps"),
                   "LASTSECRETS_SOCKET_PATH": str(node.socket), "LASTDB_SOCKET_PATH": str(node.socket)}
    for name in ("GH_TOKEN", "GITHUB_TOKEN", "ACTIONS_ID_TOKEN_REQUEST_TOKEN", "ACTIONS_RUNTIME_TOKEN"):
        environment.pop(name, None)
    results = {}
    for app in APPS:
        source, installed = source_for(node, plan, app, output, environment, node.binaries / "lastdb")
        manifest = json.loads((source / "lastdb-app.json").read_text())
        if manifest["app_id"] != app:
            raise ValueError("App source identity differs")
        link = next(item for item in manifest["install"]["links"] if item["command"] == app)
        executable = Path(environment["HOME"]) / ".local/bin" / app if installed else source / link["source"]
        run = lambda args, name, body=None, retain_output=True: command(
            [str(executable), *args], environment, source, output, app + "-" + name, body, retain_output=retain_output)
        if app == "brain":
            run(["init"], "init")
            value = "CI stable qualification record"
            run(["put", "ci-release-qualification", "--type", "concept"], "put", value)
            if value not in run(["get", "ci-release-qualification", "--type", "concept"], "get"):
                raise RuntimeError("Brain did not return the acknowledged record")
        elif app == "lastsecrets":
            run(["init"], "init")
            value = "generated-nonsecret-ci-value"
            run(["put", "ci-release-qualification", "--label", "CI qualification", "--provider", "ci",
                 "--purpose", "release", "--env", "dev", "--value-stdin"], "put", value)
            if run(["get", "ci-release-qualification"], "get", retain_output=False).strip() != value:
                raise RuntimeError("LastSecrets did not return the acknowledged value")
        else:
            inbox = node.home / "apps/search/inbox"
            if not inbox.is_dir() or not any(path.suffix == ".json" for path in inbox.iterdir()):
                raise RuntimeError("Database writes produced no real Search delivery")
            run(["init", "--last-db-home", str(node.home)], "init")
            drained = json.loads(run(["drain", "--last-db-home", str(node.home)], "drain"))
            hits = json.loads(run(["query", "CI stable qualification record", "--json", "--last-db-home", str(node.home)], "query"))
            if (drained.get("ok") is not True or not any(
                    hit.get("schema_name") == "brain/Concept" and hit.get("key_hash") == "ci-release-qualification"
                    and "CI stable qualification record" in hit.get("text", "") for hit in hits.get("hits", []))):
                raise RuntimeError("Search did not query the real database changes")
            run(["status", "--last-db-home", str(node.home)], "status")
        if not declare:
            results[app] = {"sha": plan["apps"][app], "installed_from_registry": installed, "complete": True}
            continue
        statement = output / (app + ".declaration.json")
        command([str(node.binaries / "lastdb"), "--data-dir", str(node.home), "app", "publish",
                 "--manifest", str(source / "lastdb-app.json"), "--channel", "stable",
                 "--prepare", str(statement)], environment, source, output, app + "-declaration")
        declaration = json.loads(statement.read_text())
        if declaration["sha"] != plan["apps"][app]:
            raise ValueError("The validated app commit differs from the plan")
        results[app] = {"sha": declaration["sha"], "api_version": declaration["api_version"],
                        "outbox_format_version": observed["current_outbox_format_version"],
                        "installed_from_registry": installed, "declaration": declaration, "complete": True}
    write_json(output / "apps.json", results)
    return results


def prove_compatibility(plan_path, bundles, output):
    from nightly_package import digest, document, validate_bundle
    from nightly_probe import OwnedNode, operations, unpack
    plan = load(plan_path)
    output.mkdir(parents=True)
    target = "x86_64-unknown-linux-gnu"
    validate_bundle(bundles, target, plan)
    bundle = bundles / f"lastdb-{target}.tar.gz"
    manifest = document(bundles / f"lastdb-{target}.manifest.json")
    node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), output, plan["version"], enable_search=True)
    try:
        unpack(bundle, manifest, node.binaries, plan["version"], plan["source"])
        observed = operations(node)
        apps = prove_apps(node, plan, output)
    finally:
        node.close()
    installed = {}
    for api_version, sources in plan.get("installed_apps", {}).items():
        older = output / f"api-{api_version}"
        older.mkdir()
        node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), older, plan["version"], enable_search=True)
        try:
            unpack(bundle, manifest, node.binaries, plan["version"], plan["source"])
            operations(node)
            installed[api_version] = prove_apps(node, {**plan, "apps": sources}, older, declare=False)
        except Exception as error:
            raise RuntimeError(
                f"The apps in the signed stable list for API {api_version} do not work against this "
                f"build ({error}). Keep the socket interface compatible, or raise "
                "MIN_SUPPORTED_API_VERSION in lastdb_node/src/version.rs above "
                f"{api_version} and run python3 scripts/lints/api_surface.py --write.") from error
        finally:
            node.close()
    write_json(output / "installed-app-compatibility.json", {
        "schema": "lastdb.installed.app.compatibility.v1", "source": plan["source"],
        "run_url": plan["run_url"], "api_version": observed["api_version"],
        "min_supported_api_version": observed.get("min_supported_api_version"),
        "lists": installed, "complete": True})
    write_json(output / "app-compatibility.json", {
        "schema": "lastdb.app.compatibility.proof.v1", "source": plan["source"],
        "run_url": plan["run_url"], "lastdb_source": plan["source"], "lastdb_build": plan["version"],
        "api_version": observed["api_version"], "outbox_format_version": observed["current_outbox_format_version"],
        "bundle_sha256": digest(bundle), "apps": apps, "complete": True,
        "daemon_stopped": True, "home_removed": True})


def qualify_release(plan_path, bundles, output):
    from harness import HOME_FORMAT
    from nightly_package import digest, document
    from nightly_probe import OwnedNode, operations, read, unpack, write
    plan = load(plan_path)
    output.mkdir(parents=True)
    target = "x86_64-unknown-linux-gnu"
    bundle = bundles / f"lastdb-{target}.tar.gz"
    manifest = document(bundles / f"lastdb-{target}.manifest.json")
    node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), output, plan["version"], enable_search=True)
    try:
        unpack(bundle, manifest, node.binaries, plan["version"], plan["source"])
        version = operations(node)
        apps = prove_apps(node, plan, output)
        started = datetime.now(timezone.utc)
        beginning = time.monotonic()
        latencies = []
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
    sub.add_parser("plan").add_argument("--plan", type=Path, required=True)
    gates_cmd = sub.add_parser("gates")
    gates_cmd.add_argument("--plan", type=Path, required=True)
    gates_cmd.add_argument("--wait-seconds", type=int, default=RELEASE_GATE_WAIT_SECONDS)
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
    elif args.command == "compatibility":
        prove_compatibility(args.plan, args.bundles, args.output)
    else:
        qualify_release(args.plan, args.bundles, args.output)


if __name__ == "__main__":
    main()
