#!/usr/bin/env python3
"""Assemble and publish an immutable, attested LastDB nightly prerelease."""

import argparse
import json
import os
from pathlib import Path
import subprocess

from nightly_assets import names, prepare, validate
from nightly_plan import MetadataError, REPOSITORY, _capture, gh_json, load_plan, wait_gates
from nightly_remote import (attest, latest_nightly, marker, owned, prior_plan,
                            release_by_id, release_for_tag, tag_source, verify_release)


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
    _capture(["git", "fetch", "--no-tags", "origin",
              "+refs/heads/main:refs/remotes/origin/main"], timeout=45)
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
        gh_json(f"repos/{REPOSITORY}/git/refs", "POST",
                {"ref": "refs/tags/" + plan["tag"], "sha": plan["source"]})
        source = tag_source(plan["tag"])
    if source != plan["source"]:
        raise ValueError("The immutable nightly tag already names another source")


def make_draft(plan):
    release = release_for_tag(plan["tag"])
    if release is None:
        body = (marker(plan) + "\n\nLastDB nightly prerelease.\n\n"
                f"Source: https://github.com/{REPOSITORY}/commit/{plan['source']}\n\n"
                f"Version: `{plan['version']}`. Build: {plan['run_url']}.\n\n"
                "Matched CLI and daemon for Apple Silicon and Ubuntu 24.04 x64. "
                "SHA256SUMS.txt and nightly.json describe the verified files and operational checks. "
                "GitHub artifact attestations identify the main workflow and exact source.\n\n"
                "macOS uses ad-hoc signatures, without Developer ID notarization. "
                "This prerelease does not update Homebrew, the app registry, or installed services.")
        release = gh_json(f"repos/{REPOSITORY}/releases", "POST",
                          {"tag_name": plan["tag"], "target_commitish": plan["source"],
                           "name": "LastDB " + plan["version"], "body": body, "draft": True,
                           "prerelease": True, "make_latest": "false"})
    owned(release, plan, draft=True)
    return release


def refresh_draft(release, plan, folder):
    # Only unpublished, marked workflow drafts can change. Public assets never change.
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
    parser.add_argument("command", choices=("preflight", "prepare", "verify", "publish"))
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
    elif args.command == "prepare":
        prepare(args.assets, plan)
        result = {"source": plan["source"], "hashes": validate(args.assets, plan)}
    else:
        result = {"source": plan["source"], "hashes": validate(args.assets, plan)}
    print(json.dumps(result, indent=2, sort_keys=True))
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
            summary.write("\nNightly " + args.command + ":\n\n```json\n" + json.dumps(result, indent=2, sort_keys=True) + "\n```\n")


if __name__ == "__main__":
    main()
