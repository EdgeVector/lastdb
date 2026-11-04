#!/usr/bin/env python3
"""Freeze a stable build and its app sources; never consult a developer home."""

import argparse
import base64
import json
import os
from pathlib import Path
import re
from urllib.error import HTTPError

from nightly_plan import _capture, wait_gates

TAP = "EdgeVector/homebrew-lastdb"
APPS = ("brain", "lastsecrets", "search")


def app_sources():
    from stable_registry import catalog
    text = Path("lastdb_uds/src/uds_router.rs").read_text()
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
        # Re-prove the published compatible commit, instead of requiring a
        # publisher to follow every unrelated app main update. Missing API or
        # outbox compatibility selects app main for fresh CI proof instead.
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "gates"))
    parser.add_argument("--plan", type=Path, required=True)
    args = parser.parse_args()
    result = make(args.plan) if args.command == "plan" else wait_gates(load(args.plan))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
