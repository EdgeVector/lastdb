#!/usr/bin/env python3
"""Assemble, verify, and publish an immutable, attested LastDB nightly prerelease."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from nightly_package import INDEX, MAX_FILE, digest, document, names, prepare, validate
from nightly_plan import MetadataError, REPOSITORY, SHA, _capture, gh_json, load_plan, wait_gates

NEWEST_TARGET = "aarch64-unknown-linux-gnu"

WORKFLOW = f"{REPOSITORY}/.github/workflows/nightly-release.yml"


def release_for_tag(tag):
    try:
        return gh_json(f"repos/{REPOSITORY}/releases/tags/{tag}")
    except MetadataError as error:
        if error.http_status != 404:
            raise
    # The tag endpoint returns published releases only; keep draft discovery bounded.
    matches = []
    for page in range(1, 11):
        rows = gh_json(f"repos/{REPOSITORY}/releases?per_page=100&page={page}")
        if not isinstance(rows, list) or len(rows) > 100 or any(not isinstance(row, dict) for row in rows):
            raise ValueError("The nightly release lookup exceeds its page bound")
        matches.extend(row for row in rows if row.get("tag_name") == tag)
        if len(matches) > 1:
            raise ValueError("The nightly tag has more than one release")
        if len(rows) < 100:
            return matches[0] if matches else None
    raise ValueError("The nightly release lookup exceeds its history bound")


def release_by_id(release_id):
    if type(release_id) is not int or release_id <= 0:
        raise ValueError("The nightly release has no valid numeric identity")
    try:
        release = gh_json(f"repos/{REPOSITORY}/releases/{release_id}")
    except MetadataError as error:
        if error.http_status == 404:
            raise ValueError("The nightly release no longer exists") from error
        raise
    if not isinstance(release, dict) or release.get("id") != release_id:
        raise ValueError("The nightly release identity changed")
    return release


def tag_source(tag):
    result = gh_json(f"repos/{REPOSITORY}/git/ref/tags/{tag}")["object"]
    for _ in range(4):
        if result["type"] == "commit" and SHA.fullmatch(result["sha"]):
            return result["sha"]
        if result["type"] != "tag" or not SHA.fullmatch(result["sha"]):
            break
        result = gh_json(f"repos/{REPOSITORY}/git/tags/{result['sha']}")["object"]
    raise ValueError("The nightly tag does not resolve to one source commit")


def marker(plan):
    return f"<!-- lastdb-nightly:v1 workflow={WORKFLOW} source={plan['source']} tag={plan['tag']} -->"


def owned(release, plan, draft):
    if not isinstance(release, dict):
        raise ValueError("The nightly release metadata is missing")
    if (type(release.get("id")) is not int or release["id"] <= 0
            or release.get("draft") is not draft or release.get("prerelease") is not True
            or release.get("tag_name") != plan["tag"]
            or release.get("author", {}).get("login") != "github-actions[bot]"
            or marker(plan) not in release.get("body", "") or tag_source(plan["tag"]) != plan["source"]):
        raise ValueError("The nightly release is not an unchanged workflow-owned release")


def attest(path, plan):
    _capture(["gh", "attestation", "verify", str(path), "--repo", REPOSITORY,
              "--signer-workflow", WORKFLOW, "--signer-digest", plan["source"],
              "--source-digest", plan["source"], "--source-ref", "refs/heads/main",
              "--deny-self-hosted-runners"], timeout=180)


def asset_rows(release):
    rows = release.get("assets")
    if not isinstance(rows, list) or len(rows) != len(names()):
        raise ValueError("The remote release has an incomplete asset set")
    if {row.get("name") for row in rows} != names():
        raise ValueError("The remote release has unexpected asset names")
    for row in rows:
        limit = MAX_FILE if row["name"].endswith(".tar.gz") else 8192 if row["name"] == "SHA256SUMS.txt" else 1024 ** 2
        if (row.get("state") != "uploaded" or type(row.get("size")) is not int
                or not 0 < row["size"] <= limit
                or not re.fullmatch(r"sha256:[0-9a-f]{64}", row.get("digest", ""))):
            raise ValueError("The remote release lacks complete asset digests")
    return rows


def download(release, folder):
    for row in asset_rows(release):
        _capture(["gh", "release", "download", release["tag_name"], "--repo", REPOSITORY,
                  "--dir", str(folder), "--pattern", row["name"]], timeout=300)
        path = folder / row["name"]
        if path.stat().st_size != row["size"] or "sha256:" + digest(path) != row["digest"]:
            raise ValueError("A downloaded release asset differs from GitHub metadata")


def verify_release(release, plan, draft=False):
    owned(release, plan, draft)
    release = release_by_id(release["id"])
    owned(release, plan, draft)
    with tempfile.TemporaryDirectory(prefix="nightly-verify-", dir=os.environ["RUNNER_TEMP"]) as name:
        folder = Path(name)
        download(release, folder)
        for path in sorted(folder.iterdir()):
            attest(path, plan)
        hashes = validate(folder, plan)
    return {"url": release["html_url"], "tag": plan["tag"], "source": plan["source"], "hashes": hashes}


def latest_nightly():
    for page in range(1, 11):
        rows = gh_json(f"repos/{REPOSITORY}/releases?per_page=100&page={page}")
        if not isinstance(rows, list) or len(rows) > 100:
            raise ValueError("The nightly release list exceeds its page bound")
        for release in rows:
            if (not release.get("draft") and release.get("prerelease")
                    and re.fullmatch(r"nightly-\d{8}-[0-9a-f]{9}", release.get("tag_name", ""))):
                return release
        if len(rows) < 100:
            return None
    raise ValueError("The nightly release list exceeds its history bound")


def prior_plan(release, plan):
    rows = asset_rows(release)
    index = next(row for row in rows if row["name"] == INDEX)
    with tempfile.TemporaryDirectory(prefix="nightly-index-", dir=os.environ["RUNNER_TEMP"]) as name:
        folder = Path(name)
        _capture(["gh", "release", "download", release["tag_name"], "--repo", REPOSITORY,
                  "--dir", name, "--pattern", INDEX], timeout=120)
        if "sha256:" + digest(folder / INDEX) != index["digest"]:
            raise ValueError("The previous release index differs from GitHub metadata")
        value = document(folder / INDEX)
    source = value.get("source_git_oid")
    if not isinstance(source, str) or not SHA.fullmatch(source):
        raise ValueError("The previous nightly source is invalid")
    result = {**plan, "source": source, "version": value.get("version"), "tag": value.get("tag"),
              "epoch": value.get("source_epoch"), "run_url": value.get("run_url")}
    match = re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
                         r"-nightly\.([0-9]{8})\.g([0-9a-f]{9})", result["version"] or "")
    if (result["tag"] != release["tag_name"] or match is None
            or result["tag"] != f"nightly-{match[1]}-{source[:9]}" or match[2] != source[:9]
            or not re.fullmatch(r"https://github\.com/EdgeVector/lastdb/actions/runs/[0-9]+", value.get("run_url", ""))):
        raise ValueError("The previous nightly version or tag differs")
    datetime.strptime(match[1], "%Y%m%d")
    if (type(result["epoch"]) is not int or result["epoch"] < 0
            or str(result["epoch"]) != _capture(["git", "show", "-s", "--format=%ct", source], timeout=20).strip()):
        raise ValueError("The previous nightly source timestamp differs from Git")
    verify_release(release, result)
    return result


def runtime(plan, publishing=False):
    if (os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or os.environ.get("GITHUB_REPOSITORY") != REPOSITORY):
        raise ValueError("Nightly release operations require the canonical hosted workflow")
    if _capture(["git", "rev-parse", "HEAD"], timeout=20).strip() != plan["source"]:
        raise ValueError("The workflow checkout differs from the frozen nightly source")
    if publishing and (not plan["publish"] or plan["event_name"] not in {"schedule", "workflow_dispatch"}
                       or os.environ.get("GITHUB_REF") != "refs/heads/main"
                       or os.environ.get("GITHUB_SHA") != plan["source"]
                       or os.environ.get("GITHUB_EVENT_NAME") != plan["event_name"]):
        raise ValueError("Only a main workflow can publish its own frozen source")


def ancestor(older, newer):
    result = subprocess.run(["git", "merge-base", "--is-ancestor", older, newer],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)
    if result.returncode not in (0, 1):
        raise RuntimeError("The nightly source ancestry is unavailable")
    return result.returncode == 0


def previous(plan):
    release = latest_nightly()
    if release is None:
        return None
    if f"lastdb-{NEWEST_TARGET}.tar.gz" not in {row.get("name") for row in release.get("assets", [])}:
        # Published before this target joined the fixed asset set: its set cannot be verified
        # against today's, and it never blocks a new nightly.
        return None
    _capture(["git", "fetch", "--no-tags", "origin", "+refs/heads/main:refs/remotes/origin/main"], timeout=45)
    value = prior_plan(release, plan)
    if not ancestor(value["source"], "refs/remotes/origin/main"):
        raise ValueError("The previous nightly source is outside accepted main history")
    if value["source"] == plan["source"]:
        return {"skip": True, "reason": "The unchanged source already has a verified nightly", "url": release["html_url"]}
    if ancestor(plan["source"], value["source"]):
        return {"skip": True, "reason": "A newer source already has a verified nightly", "url": release["html_url"]}
    return None


def preflight(plan):
    if not plan["publish"]:
        return {"skip": False, "reason": "PR build and package validation only"}
    runtime(plan, publishing=True)
    release = release_for_tag(plan["tag"])
    if release is not None:
        if release.get("draft"):
            owned(release, plan, draft=True)
        else:
            verified = verify_release(release, plan)
            wait_gates(plan, 0)
            return {"skip": True, "reason": "The immutable nightly already exists and verifies", **verified}
    return previous(plan) or {"skip": False, "reason": "A new accepted source needs a nightly"}


def ensure_tag(plan):
    try:
        source = tag_source(plan["tag"])
    except MetadataError as error:
        if error.http_status != 404:
            raise
        gh_json(f"repos/{REPOSITORY}/git/refs", "POST", {"ref": "refs/tags/" + plan["tag"], "sha": plan["source"]})
        source = tag_source(plan["tag"])
    if source != plan["source"]:
        raise ValueError("The immutable nightly tag already names another source")


def make_draft(plan):
    release = release_for_tag(plan["tag"])
    if release is None:
        body = (marker(plan) + "\n\nLastDB nightly prerelease.\n\n"
                f"Source: https://github.com/{REPOSITORY}/commit/{plan['source']}\n\n"
                f"Version: `{plan['version']}`. Build: {plan['run_url']}.\n\n"
                "Matched CLI and daemon for Apple Silicon and for Ubuntu 24.04 on x64 and arm64. "
                "SHA256SUMS.txt and nightly.json describe the verified files and operational checks. "
                "GitHub artifact attestations identify the main workflow and exact source.\n\n"
                "The macOS programs are Developer ID signed and notarized. "
                "This prerelease does not update Homebrew, the app registry, or installed services.")
        release = gh_json(f"repos/{REPOSITORY}/releases", "POST",
                          {"tag_name": plan["tag"], "target_commitish": plan["source"],
                           "name": "LastDB " + plan["version"], "body": body, "draft": True,
                           "prerelease": True, "make_latest": "false"})
    owned(release, plan, draft=True)
    return release


def refresh_draft(release, plan, folder):
    assets = release.get("assets", [])
    if (not isinstance(assets, list) or len(assets) > len(names())
            or len({asset.get("name") for asset in assets}) != len(assets)
            or any(asset.get("name") not in names() or type(asset.get("id")) is not int
                   or asset["id"] <= 0 for asset in assets)):
        raise ValueError("The workflow draft has assets outside its owned set")
    for asset in assets:
        current = release_by_id(release["id"])
        owned(current, plan, draft=True)
        if current["id"] != release["id"]:
            raise ValueError("The nightly draft identity changed")
        gh_json(f"repos/{REPOSITORY}/releases/assets/{asset['id']}", "DELETE")
    owned(release_by_id(release["id"]), plan, draft=True)
    _capture(["gh", "release", "upload", plan["tag"], "--repo", REPOSITORY,
              *[str(path) for path in sorted(folder.iterdir())]], timeout=600)


def publish(plan, folder):
    runtime(plan, publishing=True)
    for path in sorted(folder.iterdir()):
        attest(path, plan)
    hashes = validate(folder, plan)
    wait_gates(plan, 0)
    stale = previous(plan)
    if stale:
        return stale
    existing = release_for_tag(plan["tag"])
    if existing is not None and not existing.get("draft"):
        return {"skip": True, **verify_release(existing, plan)}
    ensure_tag(plan)
    release = make_draft(plan)
    refresh_draft(release, plan, folder)
    verify_release(release_by_id(release["id"]), plan, draft=True)
    wait_gates(plan, 0)
    stale = previous(plan)
    if stale:
        return stale
    current = release_by_id(release["id"])
    owned(current, plan, draft=True)
    if current["id"] != release["id"]:
        raise ValueError("The nightly draft identity changed before publication")
    gh_json(f"repos/{REPOSITORY}/releases/{release['id']}", "PATCH",
            {"draft": False, "prerelease": True, "make_latest": "false"})
    result = verify_release(release_by_id(release["id"]), plan)
    if result["hashes"] != hashes:
        raise ValueError("The published bytes differ from the attested candidate")
    return {"skip": False, **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "prepare", "publish"))
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--assets", type=Path)
    args = parser.parse_args()
    if args.command != "preflight":
        if args.assets is None:
            parser.error("This command requires --assets")
        directory = args.assets.resolve(strict=True)
        temporary = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
        if args.assets.is_symlink() or directory == temporary or not directory.is_relative_to(temporary):
            raise ValueError("Release assets require their own directory under RUNNER_TEMP")
        args.assets = directory
    plan = load_plan(args.plan)
    runtime(plan)
    if args.command == "preflight":
        result = preflight(plan)
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            output.write("skip=" + str(result["skip"]).lower() + "\n")
    elif args.command == "publish":
        result = publish(plan, args.assets)
    else:
        result = {"source": plan["source"], "hashes": prepare(args.assets, plan)}
    print(json.dumps(result, indent=2, sort_keys=True))
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
            summary.write("\nNightly " + args.command + ":\n\n```json\n" + json.dumps(result, indent=2, sort_keys=True) + "\n```\n")


if __name__ == "__main__":
    main()
