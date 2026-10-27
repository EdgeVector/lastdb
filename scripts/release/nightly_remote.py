"""Read and verify immutable nightly releases through the GitHub CLI."""

import os
from datetime import datetime
from pathlib import Path
import re
import tempfile

from nightly_assets import INDEX, MAX_FILE, digest, document, names, validate
from nightly_plan import MetadataError, REPOSITORY, SHA, _capture, gh_json


WORKFLOW = f"{REPOSITORY}/.github/workflows/nightly-release.yml"


def release_for_tag(tag):
    try:
        return gh_json(f"repos/{REPOSITORY}/releases/tags/{tag}")
    except MetadataError as error:
        if error.http_status != 404:
            raise
    # The tag endpoint returns published releases only. A writer can also see
    # drafts in the authenticated list; keep retry discovery bounded.
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
