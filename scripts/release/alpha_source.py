#!/usr/bin/env python3
"""Move the public alpha tag after a green main recovery and a settled format.

The tag moves only when Automatic DEV backup and recovery passed on a main
push, and the tree no longer contains LegacyUtf8V1. A later green recovery
moves the tag again. An immutable alpha-<date>-<sha> tag is never moved.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import re
import subprocess
import tempfile


REPOSITORY = "EdgeVector/lastdb"
LIVE_JOB = "Automatic DEV backup and recovery"
RECOVERY_WORKFLOW = "Cloud backup recovery"
NIGHTLY_WORKFLOW = "Nightly LastDB release"
SHA = re.compile(r"[0-9a-f]{40}\Z")
MAX_BYTES = 2 * 1024 * 1024


class AlphaError(RuntimeError):
    pass


def git(*args):
    result = subprocess.run(["git", *args], check=False, capture_output=True)
    if result.returncode != 0:
        raise AlphaError(f"git {' '.join(args)} failed with exit {result.returncode}")
    return result.stdout.decode("utf-8").strip()


def gh_json(path):
    result = subprocess.run(["gh", "api", path], check=False, capture_output=True)
    if result.returncode != 0 or len(result.stdout) > MAX_BYTES:
        raise AlphaError(f"GitHub metadata failed with exit {result.returncode}")
    return json.loads(result.stdout)


def gh_send(method, path, payload):
    with tempfile.NamedTemporaryFile("w", delete=False) as handle:
        json.dump(payload, handle)
        name = handle.name
    try:
        result = subprocess.run(
            ["gh", "api", "--method", method, "--input", name, path],
            check=False, capture_output=True)
    finally:
        os.unlink(name)
    if result.returncode != 0:
        raise AlphaError(f"GitHub {method} failed with exit {result.returncode}")


def load_event():
    path = os.environ.get("GITHUB_EVENT_PATH")
    if not path:
        raise AlphaError("The alpha workflow has no event")
    raw = Path(path).read_bytes()
    if len(raw) > MAX_BYTES:
        raise AlphaError("The alpha event is too large")
    event = json.loads(raw)
    run = event.get("workflow_run")
    if not isinstance(run, dict):
        raise AlphaError("The alpha event is not a workflow run")
    return run


def tag_commit(name):
    result = subprocess.run(
        ["git", "rev-parse", f"{name}^{{}}"], check=False, capture_output=True)
    if result.returncode != 0:
        return None
    value = result.stdout.decode("utf-8").strip()
    if SHA.fullmatch(value) is None:
        raise AlphaError("A tag does not name a commit")
    return value


def fetch_tag(name):
    subprocess.run(
        ["git", "fetch", "--no-tags", "origin", "tag", name], check=False, capture_output=True)


def require_run(run):
    head = run.get("head_repository") or {}
    if (os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or head.get("full_name") != REPOSITORY):
        raise AlphaError("The alpha workflow is outside EdgeVector/lastdb")
    sha = run.get("head_sha")
    if not isinstance(sha, str) or SHA.fullmatch(sha) is None:
        raise AlphaError("The recovery commit is invalid")
    if git("rev-parse", "HEAD") != sha:
        raise AlphaError("The checkout differs from the recovery commit")
    return sha


def format_settled():
    result = subprocess.run(
        ["git", "grep", "-I", "-n", "LegacyUtf8V1", "--", "*.rs"],
        check=False, capture_output=True)
    if result.returncode == 1:
        return True
    if result.returncode == 0:
        return False
    raise AlphaError(f"format search failed with exit {result.returncode}")


def live_recovery(run):
    if (run.get("name") != RECOVERY_WORKFLOW or run.get("event") != "push"
            or run.get("head_branch") != "main" or run.get("conclusion") != "success"):
        return False
    run_id = run.get("id")
    if type(run_id) is not int:
        raise AlphaError("The recovery run id is invalid")
    data = gh_json(f"repos/{REPOSITORY}/actions/runs/{run_id}/jobs?per_page=100")
    jobs = data.get("jobs")
    if not isinstance(jobs, list):
        raise AlphaError("The recovery jobs are missing")
    return any(job.get("name") == LIVE_JOB and job.get("conclusion") == "success"
               for job in jobs)


def on_main(sha):
    git("fetch", "origin", "main")
    ancestor = subprocess.run(
        ["git", "merge-base", "--is-ancestor", sha, "origin/main"], check=False)
    if ancestor.returncode != 0:
        raise AlphaError("The recovery commit is not on main")
    return git("rev-parse", "origin/main") == sha


def nightly_url(sha):
    releases = gh_json(f"repos/{REPOSITORY}/releases?per_page=30")
    if not isinstance(releases, list):
        return None
    prefix = f"https://github.com/{REPOSITORY}/releases/"
    for release in releases:
        tag = release.get("tag_name") or ""
        if not tag.endswith(sha[:9]) or not tag.startswith("nightly-"):
            continue
        url = release.get("html_url")
        if not isinstance(url, str) or not url.startswith(prefix):
            continue
        if release.get("target_commitish") == sha:
            return url
        fetch_tag(tag)
        if tag_commit(tag) == sha:
            return url
    return None


def notes(sha, run_id, archive):
    lines = [
        f"Alpha source {sha}",
        "",
        "Automatic DEV backup and recovery passed.",
        f"Run: https://github.com/{REPOSITORY}/actions/runs/{run_id}",
        "",
    ]
    if archive:
        lines.extend([
            "Apple Silicon and Linux x64 archives:",
            archive,
            "",
        ])
    lines.extend([
        "Local use needs no account.",
        "Backup is optional.",
        "One machine writes. A second machine restores a backup.",
        "",
        "Connect with lastdb cloud backup-v2 connect.",
        "Turn backup on with lastdb cloud backup-v2 enable-automatic.",
        "Keep the recovery phrase. The phrase opens a restore.",
        "",
    ])
    return "\n".join(lines)


def commit_date(sha):
    epoch = int(git("show", "-s", "--format=%ct", sha))
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%Y%m%d")


def put_tag(name, sha, message, move):
    fetch_tag(name)
    have = tag_commit(name)
    if have == sha:
        return
    if have is not None and not move:
        raise AlphaError("An immutable alpha tag already names another commit")
    git("tag", "-a", name, "-f", sha, "-m", message)
    ref = f"refs/tags/{name}"
    if move:
        git("push", "--force", "origin", ref)
    else:
        git("push", "origin", ref)


def put_release(sha, body):
    view = subprocess.run(
        ["gh", "api", f"repos/{REPOSITORY}/releases/tags/alpha"],
        check=False, capture_output=True)
    payload = {
        "tag_name": "alpha",
        "target_commitish": sha,
        "name": "LastDB alpha",
        "body": body,
        "draft": False,
        "prerelease": True,
        "make_latest": "false",
    }
    if view.returncode == 0:
        release = json.loads(view.stdout)
        release_id = release.get("id")
        if type(release_id) is not int:
            raise AlphaError("The alpha release id is invalid")
        gh_send("PATCH", f"repos/{REPOSITORY}/releases/{release_id}", payload)
        return
    gh_send("POST", f"repos/{REPOSITORY}/releases", payload)


def dispatch_nightly(sha):
    data = gh_json(
        f"repos/{REPOSITORY}/actions/workflows/nightly-release.yml/runs?per_page=5")
    runs = data.get("workflow_runs")
    if not isinstance(runs, list):
        raise AlphaError("The nightly runs are missing")
    for run in runs:
        if run.get("head_sha") == sha and run.get("conclusion") in (None, "success"):
            return
    result = subprocess.run(
        ["gh", "workflow", "run", "nightly-release.yml", "--ref", "main"], check=False)
    if result.returncode != 0:
        raise AlphaError(f"nightly dispatch failed with exit {result.returncode}")


def move_from_recovery(run, sha):
    if not format_settled():
        print("alpha tag unchanged: disk format is still open")
        return
    if not live_recovery(run):
        print("alpha tag unchanged: live recovery did not pass")
        return
    tip = on_main(sha)
    run_id = run["id"]
    message = f"Alpha source {sha}. Recovery run {run_id}."
    put_tag("alpha", sha, message, move=True)
    frozen = f"alpha-{commit_date(sha)}-{sha[:9]}"
    put_tag(frozen, sha, message, move=False)
    archive = nightly_url(sha)
    put_release(sha, notes(sha, run_id, archive))
    if tip and archive is None:
        dispatch_nightly(sha)
    print(f"alpha tag names {sha}")


def link_from_nightly(run, sha):
    if (run.get("name") != NIGHTLY_WORKFLOW or run.get("conclusion") != "success"
            or run.get("head_branch") != "main"):
        print("alpha release unchanged: nightly did not publish main")
        return
    fetch_tag("alpha")
    if tag_commit("alpha") != sha:
        print("alpha release unchanged: nightly commit is not the alpha tag")
        return
    archive = nightly_url(sha)
    if archive is None:
        print("alpha release unchanged: nightly archive is not listed yet")
        return
    put_release(sha, notes(sha, run["id"], archive))
    print(f"alpha release links the archive for {sha}")


def main():
    run = load_event()
    sha = require_run(run)
    name = run.get("name")
    if name == RECOVERY_WORKFLOW:
        move_from_recovery(run, sha)
    elif name == NIGHTLY_WORKFLOW:
        link_from_nightly(run, sha)
    else:
        print("alpha tag unchanged: unrelated workflow")


if __name__ == "__main__":
    main()
