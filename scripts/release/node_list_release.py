"""Build a list entry from a published release. Used by node_list.py."""

from __future__ import annotations

import json
from pathlib import Path
import re
import subprocess
import tempfile

from node_list_rules import (HEX64, ListError, check_entry, fmt, is_int, now_utc,
                             parse_time)

REPOS = {"nightly": "EdgeVector/lastdb", "stable": "EdgeVector/homebrew-lastdb"}
MAX_MANIFEST = 1024 ** 2
ROLLOUT_START = {"nightly": 100, "stable": 10}
NIGHTLY_TAG = re.compile(r"nightly-\d{8}-[0-9a-f]{9}\Z")
STABLE_TAG = re.compile(r"v\d+\.\d+\.\d+\Z")


def gh_json(path):
    done = subprocess.run(["gh", "api", path], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(done.stdout)


def channel_of_tag(tag):
    if NIGHTLY_TAG.fullmatch(tag):
        return "nightly"
    if STABLE_TAG.fullmatch(tag):
        return "stable"
    raise ListError(f"Tag {tag!r} is neither a nightly nor a stable release tag")


def find_release(args):
    if args.tag:
        channel = channel_of_tag(args.tag)
        release = gh_json(f"repos/{REPOS[channel]}/releases/tags/{args.tag}")
        return channel, release
    channel = args.latest
    rows = gh_json(f"repos/{REPOS[channel]}/releases?per_page=100")
    pattern = NIGHTLY_TAG if channel == "nightly" else STABLE_TAG
    rows = [row for row in rows if not row.get("draft") and pattern.fullmatch(row.get("tag_name", ""))
            and row.get("published_at")]
    if not rows:
        raise ListError(f"No published {channel} release was found")
    return channel, max(rows, key=lambda row: row["published_at"])


def download_manifest(repo, tag, name):
    with tempfile.TemporaryDirectory() as folder:
        subprocess.run(["gh", "release", "download", tag, "--repo", repo, "--pattern", name,
                        "--dir", folder], check=True, capture_output=True, text=True, timeout=180)
        path = Path(folder) / name
        if path.is_symlink() or not 0 < path.stat().st_size <= MAX_MANIFEST:
            raise ListError(f"{name} has an invalid size")
        return json.loads(path.read_text())


def build_entry(channel, repo, release, target):
    """The list entry for one target, or None when the release has no asset for it."""
    tag = release["tag_name"]
    if release.get("draft"):
        raise ListError(f"Release {tag} is a draft")
    manifest_name, archive_name = f"lastdb-{target}.manifest.json", f"lastdb-{target}.tar.gz"
    assets = {row["name"]: row for row in release.get("assets", [])}
    if manifest_name not in assets:
        return None
    if archive_name not in assets:
        raise ListError(f"Release {tag} has {manifest_name} but not {archive_name}")
    manifest = download_manifest(repo, tag, manifest_name)
    artifact = manifest.get("artifact", {})
    if (manifest.get("schema") != "lastdb.mini.bundle.v1" or manifest.get("target") != target
            or artifact.get("file") != archive_name or not HEX64.fullmatch(str(artifact.get("sha256")))
            or not is_int(artifact.get("size")) or artifact["size"] <= 0):
        raise ListError(f"The {target} manifest of {tag} differs from lastdb.mini.bundle.v1")
    row = assets[archive_name]
    if row.get("size") != artifact["size"]:
        raise ListError("The archive size differs from its manifest")
    if row.get("digest") and row["digest"] != "sha256:" + artifact["sha256"]:
        raise ListError("The archive digest on GitHub differs from its manifest")
    binaries = {item.get("name"): item for item in manifest.get("binaries", [])}
    if set(binaries) != {"lastdb", "lastdbd"} or len(manifest["binaries"]) != 2:
        raise ListError("The manifest must name exactly lastdb and lastdbd")
    entry = {
        "version": manifest["version"],
        "source_git_oid": manifest["source_git_oid"],
        "published_at": fmt(parse_time(release["published_at"], "release published_at")),
        "archive_url": f"https://github.com/{repo}/releases/download/{tag}/{archive_name}",
        "archive_sha256": artifact["sha256"],
        "archive_bytes": artifact["size"],
        "binaries": [{"name": name, "sha256": binaries[name].get("sha256"), "size": binaries[name].get("size")}
                     for name in ("lastdb", "lastdbd")],
        "signing": manifest.get("signing"),
        "rollout": {"percent": ROLLOUT_START[channel], "started_at": fmt(now_utc())},
        "status": "active",
    }
    if entry["signing"] == "ad-hoc":
        # The contract has no place for an ad-hoc build, so that target's list does not change.
        print(f"::warning::{tag} {target} is ad-hoc signed; only developer-id releases are listed")
        return None
    check_entry(entry, channel, target)
    return entry
