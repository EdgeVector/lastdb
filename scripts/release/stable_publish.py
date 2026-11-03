#!/usr/bin/env python3
"""Promote qualified bytes through the Homebrew repository's ordinary PR gates."""

import argparse
import base64
from datetime import datetime
import json
import os
from pathlib import Path
import re
import time

from nightly_assets import TARGETS, digest, document, validate_bundle
from nightly_plan import _capture, wait_gates
from stable_plan import APPS, TAP, api, load
from stable_registry import require_apps


def qualification(folder, plan):
    result = document(folder / "qualification.json")
    elapsed = (datetime.fromisoformat(result["ended_at"]) - datetime.fromisoformat(result["started_at"])).total_seconds()
    if (not plan["publish"] or result.get("schema") != "lastdb.stable.qualification.v1"
            or result.get("source") != plan["source"] or result.get("version") != plan["version"]
            or result.get("run_url") != plan["run_url"] or result.get("qualified") is not True
            or result.get("elapsed_seconds", 0) < 3600 or elapsed < 3600
            or result.get("healthy_samples", 0) < 121 or result.get("health_p95_seconds", 3) > 2
            or any(result.get(key) is not True for key in ("durable_operations", "recovery", "daemon_stopped", "home_removed"))
            or set(result.get("apps", {})) != set(APPS)):
        raise ValueError("The full one-hour qualification is incomplete")
    for app in APPS:
        if (result["apps"][app].get("complete") is not True
                or result["apps"][app].get("installed_from_registry") is not True
                or result["apps"][app]["sha"] != plan["apps"][app]):
            raise ValueError("App qualification differs from the frozen source")
    return result


def provenance(folder, proof, plan):
    expected = {f'stable-plan-{os.environ["GITHUB_RUN_ID"]}',
                f'stable-qualification-{os.environ["GITHUB_RUN_ID"]}',
                *(f'stable-bundles-{os.environ["GITHUB_RUN_ID"]}-{target}' for target in TARGETS)}
    rows = api(f'repos/EdgeVector/lastdb/actions/runs/{os.environ["GITHUB_RUN_ID"]}/artifacts?per_page=100')
    retained = [row for row in rows["artifacts"] if row["name"] in expected]
    if len(retained) != len(expected) or {row["name"] for row in retained} != expected:
        raise ValueError("Release artifact uploads are incomplete")
    for row in retained:
        if (row["expired"] or row["size_in_bytes"] <= 0
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", row.get("digest", ""))
                or datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00")) <= datetime.now().astimezone()):
            raise ValueError("Release artifact retention or digest is invalid")
    for path in [*sorted(folder.iterdir()), proof / "qualification.json"]:
        _capture(["gh", "attestation", "verify", str(path), "--repo", "EdgeVector/lastdb",
                  "--signer-workflow", "EdgeVector/lastdb/.github/workflows/stable-release.yml",
                  "--signer-digest", plan["source"], "--source-digest", plan["source"],
                  "--source-ref", "refs/heads/main", "--deny-self-hosted-runners"], timeout=180)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
        stream.write("Verified retained CI artifacts for source " + plan["source"] + "\n\n")
        for row in retained:
            stream.write(f'- [{row["name"]}]({row["archive_download_url"]}) — {row["digest"]}; expires {row["expires_at"]}\n')


def formula_tree(plan, mac_hash):
    parent = api(f"repos/{TAP}/git/ref/heads/main")["object"]["sha"]
    commit = api(f"repos/{TAP}/git/commits/{parent}")
    entries = []
    for name in ("lastdb", "folddb"):
        path = f"Formula/{name}.rb"
        old = base64.b64decode(api(f"repos/{TAP}/contents/{path}?ref={parent}")["content"]).decode()
        previous = re.search(r'^\s*version "(\d+\.\d+\.\d+)"', old, re.M)
        if previous is None or tuple(map(int, previous[1].split("."))) >= tuple(map(int, plan["version"].split("."))):
            raise ValueError("Stable publication would not advance the formula")
        content, versions = re.subn(r'(?m)^(\s*version )"[^"]+"', rf'\g<1>"{plan["version"]}"', old)
        url = f'https://github.com/{TAP}/releases/download/{plan["tag"]}/lastdb-aarch64-apple-darwin.tar.gz'
        content, urls = re.subn(r'(?m)^(\s*url )"[^"]+"', rf'\g<1>"{url}"', content)
        content, hashes = re.subn(r'(?m)^(\s*sha256 )"[0-9a-f]+"', rf'\g<1>"{mac_hash}"', content)
        if (versions, urls, hashes) != (1, 1, 1):
            raise ValueError("Formula changes exceed the three release fields")
        if 'depends_on "openssl@3"' not in content:
            if content.count("  on_macos do") != 1:
                raise ValueError("The native formula has no definite macOS block")
            content = content.replace("  on_macos do", '  depends_on "openssl@3"\n\n  on_macos do', 1)
        entries.append({"path": path, "mode": "100644", "type": "blob", "content": content})
    tree = api(f"repos/{TAP}/git/trees", "POST", {"base_tree": commit["tree"]["sha"], "tree": entries})
    return api(f"repos/{TAP}/git/commits", "POST", {
        "message": f'Promote qualified LastDB {plan["version"]}', "tree": tree["sha"], "parents": [parent]})["sha"]


def merged_promotion(plan, mac_hash):
    rows = api(f'repos/{TAP}/pulls?state=closed&head=EdgeVector:stable/{plan["version"]}&per_page=100')
    matches = [row for row in rows if row.get("merged_at")
               and "Source: " + plan["source"] in row.get("body", "")
               and "Validation: " + plan["run_url"] in row.get("body", "")]
    if not matches:
        return None
    if len(matches) != 1:
        raise ValueError("The stable promotion has ambiguous merged PRs")
    url = f'https://github.com/{TAP}/releases/download/{plan["tag"]}/lastdb-aarch64-apple-darwin.tar.gz'
    for name in ("lastdb", "folddb"):
        row = api(f"repos/{TAP}/contents/Formula/{name}.rb?ref=main")
        text = base64.b64decode(row["content"]).decode()
        for field, expected in (("version", plan["version"]), ("url", url), ("sha256", mac_hash)):
            values = re.findall(rf'^\s*{field} "([^"]+)"', text, re.M)
            if values != [expected]:
                raise ValueError("The merged formula differs from the qualified release")
        if 'depends_on "openssl@3"' not in text:
            raise ValueError("The merged formula lacks the native runtime dependency")
    return matches[0]


def release(plan, folder, hashes, proof):
    marker = f'<!-- lastdb-stable:v1 source={plan["source"]} version={plan["version"]} -->'
    rows = api(f"repos/{TAP}/releases?per_page=100")
    matches = [row for row in rows if row["tag_name"] == plan["tag"]]
    if matches:
        result = matches[0]
        if marker not in result.get("body", ""):
            raise ValueError("The release tag belongs to another publisher or source")
    else:
        result = api(f"repos/{TAP}/releases", "POST", {
            "tag_name": plan["tag"], "name": "LastDB " + plan["version"], "draft": True,
            "prerelease": True, "body": marker + "\n\nQualified for one uninterrupted hour in CI.\n\n"
            + f'Home format {proof["home_format"]}, API {proof["api_version"]}. Older homes require an explicit conversion; this release does not convert or upgrade a primary.\n\n'
            + plan["run_url"]})
    # Existing assets are immutable, including on retry. Never --clobber.
    existing = {row["name"]: row for row in result["assets"]}
    if set(existing) - set(hashes):
        raise ValueError("The release contains unexpected assets")
    for name, checksum in hashes.items():
        row = existing.get(name)
        if row:
            if row.get("digest") != "sha256:" + checksum or row.get("size") != (folder / name).stat().st_size:
                raise ValueError("An existing release asset differs from the qualified bytes")
        else:
            _capture(["gh", "release", "upload", plan["tag"], str(folder / name), "--repo", TAP], timeout=300)
    # A prerelease exposes the exact assets for Homebrew's normal PR checks.
    # It becomes stable only after that PR has actually merged.
    if result["draft"]:
        api(f'repos/{TAP}/releases/{result["id"]}', "PATCH", {"draft": False, "prerelease": True})
    return result


def tap_pr(plan, commit):
    branch = "stable/" + plan["version"]
    matches = api(f"repos/{TAP}/pulls?state=all&head=EdgeVector:{branch}&per_page=100")
    if matches:
        pr = matches[0]
        if not pr.get("merged_at") and pr["state"] != "open":
            raise ValueError("The stable update PR was closed without merging")
    else:
        from nightly_plan import MetadataError
        try:
            existing = api(f"repos/{TAP}/git/ref/heads/{branch}")["object"]["sha"]
        except MetadataError as error:
            if error.http_status != 404:
                raise
            api(f"repos/{TAP}/git/refs", "POST", {"ref": "refs/heads/" + branch, "sha": commit})
        else:
            previous = api(f"repos/{TAP}/git/commits/{existing}")
            proposed = api(f"repos/{TAP}/git/commits/{commit}")
            if previous["tree"]["sha"] != proposed["tree"]["sha"]:
                raise ValueError("The existing stable branch differs; refuse to overwrite it")
        pr = api(f"repos/{TAP}/pulls", "POST", {"head": branch, "base": "main",
                 "title": "LastDB " + plan["version"] + " — qualified stable release",
                 "body": "Promote the matched CLI and daemon after the full one-hour CI qualification and signed app compatibility gate.\n\n"
                         + "Source: " + plan["source"] + "\n\nValidation: " + plan["run_url"]
                         + "\n\nUpdate version, URL, and checksum in each formula, declaring openssl@3 when absent. The primary is unchanged."})
    if pr.get("merged_at"):
        return pr
    proposed = api(f"repos/{TAP}/git/commits/{commit}")
    actual = api(f'repos/{TAP}/git/commits/{pr["head"]["sha"]}')
    if (actual["tree"]["sha"] != proposed["tree"]["sha"]
            or "Source: " + plan["source"] not in pr.get("body", "")
            or "Validation: " + plan["run_url"] not in pr.get("body", "")):
        raise ValueError("The pending Homebrew PR differs from this qualified promotion")
    _capture(["gh", "pr", "merge", str(pr["number"]), "--repo", TAP, "--auto", "--squash",
              "--match-head-commit", pr["head"]["sha"]], timeout=60)
    deadline = time.monotonic() + 1800
    while time.monotonic() < deadline:
        current = api(f'repos/{TAP}/pulls/{pr["number"]}')
        if current.get("merged_at"):
            return current
        if current["state"] != "open":
            raise RuntimeError("The stable formula PR closed without merging")
        print("Waiting for normal Homebrew PR gates: " + current["html_url"], flush=True)
        time.sleep(30)
    raise RuntimeError("Homebrew PR gates are still pending; stable promotion remains incomplete")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--bundles", type=Path, required=True)
    parser.add_argument("--proof", type=Path, required=True)
    args = parser.parse_args()
    plan = load(args.plan)
    if os.environ.get("GITHUB_REF") != "refs/heads/main" or os.environ.get("GITHUB_EVENT_NAME") == "pull_request":
        raise ValueError("Untrusted release invocation")
    proof = qualification(args.proof, plan)
    expected = {f"lastdb-{target}{suffix}" for target in TARGETS
                for suffix in (".tar.gz", ".manifest.json", ".proof.json")}
    if {path.name for path in args.bundles.iterdir()} != expected:
        raise ValueError("The downloaded bundle set differs from the fixed native asset set")
    if proof["bundle_sha256"] != digest(args.bundles / "lastdb-x86_64-unknown-linux-gnu.tar.gz"):
        raise ValueError("Qualification belongs to different executable bytes")
    for target in TARGETS:
        native = validate_bundle(args.bundles, target, plan)
        if (native["proof"]["api_version"] != proof["api_version"]
                or native["proof"]["current_outbox_format_version"] != proof["outbox_format_version"]):
            raise ValueError("The native packages report different interfaces")
    provenance(args.bundles, args.proof, plan)
    wait_gates(plan, wait_seconds=0)
    registry = require_apps(proof)
    metadata = {"schema": "lastdb.stable.release.v1", "plan": plan, "qualification": proof, "registry": registry}
    (args.bundles / "stable.json").write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n")
    hashes = {path.name: digest(path) for path in args.bundles.iterdir()}
    (args.bundles / "SHA256SUMS.txt").write_text("".join(f"{value}  {name}\n" for name, value in sorted(hashes.items())))
    hashes["SHA256SUMS.txt"] = digest(args.bundles / "SHA256SUMS.txt")
    mac_hash = hashes["lastdb-aarch64-apple-darwin.tar.gz"]
    pr = merged_promotion(plan, mac_hash)
    commit = None if pr else formula_tree(plan, mac_hash)
    result = release(plan, args.bundles, hashes, proof)
    pr = pr or tap_pr(plan, commit)
    verified = merged_promotion(plan, mac_hash)
    if verified is None or verified["number"] != pr["number"]:
        raise ValueError("The merged Homebrew promotion could not be verified")
    api(f'repos/{TAP}/releases/{result["id"]}', "PATCH", {"prerelease": False, "make_latest": "true"})
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
        stream.write(f'Published stable {plan["tag"]} after 3600 seconds of qualification.\n\n{pr["html_url"]}\n\n{plan["run_url"]}\n')


if __name__ == "__main__":
    main()
