#!/usr/bin/env python3
"""Dispatch People DEV sharing against a LastDB SHA and wait for the result.

People owns the recipes and the DEV publisher secrets
(EdgeVector/people `.github/workflows/dev-sharing.yml`). This gate starts
that workflow on people main with `lastdb_source_sha` and fails when any
sharing leg fails. It uses the same `secrets.GH_PAT` cross-repo pattern as
`notify-schema-infra.yml`.

Correlation is the People `run-name` only: exact
`DEV sharing (<40-hex SHA>)` on `head_branch=main`. There is no
“newest dispatch” fallback and no adoption of a concurrent run for a
different LastDB SHA or ref (same class of failure as exemem deploy
prove watching People 45068459500 / 6f9dbbeb5 after a cancel redispatch).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

PEOPLE = "EdgeVector/people"
WORKFLOW = "dev-sharing.yml"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHARING_LEGS = (
    "people-share in DEV",
    "people-circles in DEV",
    "people-scale in DEV",
    "people-prepare in DEV",
    "people-link in DEV",
    "people-direct in DEV",
)


def gh_api(path: str, method: str = "GET", body: dict | None = None) -> subprocess.CompletedProcess:
    cmd = ["gh", "api", "--method", method, path]
    if body is None:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    return subprocess.run(
        cmd + ["--input", "-"],
        input=json.dumps(body),
        capture_output=True,
        text=True,
        timeout=120,
    )


def require_token() -> None:
    if os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN"):
        return
    print(
        "people-dev-sharing-gate: GH_TOKEN is empty.\n"
        "Set secrets.GH_PAT on EdgeVector/lastdb with Actions write on "
        f"{PEOPLE} (same cross-repo pattern as notify-schema-infra.yml).",
        file=sys.stderr,
    )
    sys.exit(2)


def dispatch(sha: str, mode: str) -> None:
    # workflow_dispatch returns 204 with an empty body.
    # Only mode + lastdb_source_sha: leave lastdb_release and relay_url unset.
    result = gh_api(
        f"repos/{PEOPLE}/actions/workflows/{WORKFLOW}/dispatches",
        method="POST",
        body={
            "ref": "main",
            "inputs": {
                "lastdb_source_sha": sha,
                "mode": mode,
            },
        },
    )
    if result.returncode == 0:
        print(f"people-dev-sharing-gate: dispatched {WORKFLOW} for {sha}", flush=True)
        return
    detail = (result.stderr or result.stdout or "").strip()[:500]
    print(
        "people-dev-sharing-gate: FAIL dispatching People DEV sharing "
        f"(exit {result.returncode}): {detail}\n"
        "Need secrets.GH_PAT on EdgeVector/lastdb with permission to "
        f"workflow_dispatch {PEOPLE} `{WORKFLOW}` (Actions: write on that "
        "repository). Fine-grained: Actions write + Contents read on people. "
        "Classic: the `workflow` scope plus repo access.",
        file=sys.stderr,
    )
    sys.exit(1)


def list_runs() -> list[dict]:
    result = gh_api(
        f"repos/{PEOPLE}/actions/workflows/{WORKFLOW}/runs"
        "?event=workflow_dispatch&per_page=30"
    )
    if result.returncode != 0:
        print(
            f"people-dev-sharing-gate: run list failed: {(result.stderr or '')[:200]}",
            flush=True,
        )
        return []
    return json.loads(result.stdout).get("workflow_runs", [])


def parse_created(run: dict) -> datetime | None:
    created = run.get("created_at") or ""
    try:
        return datetime.fromisoformat(created.replace("Z", "+00:00"))
    except ValueError:
        return None


def expected_run_name(sha: str) -> str:
    return f"DEV sharing ({sha.lower()})"


def run_identity_matches(run: dict, sha: str) -> bool:
    """True only for people main workflow_dispatch of this exact LastDB SHA."""
    if (run.get("head_branch") or "") != "main":
        return False
    title = (run.get("display_title") or "").strip().lower()
    return title == expected_run_name(sha)


def find_run(sha: str, not_before: datetime) -> dict | None:
    """Return the run whose run-name is exactly DEV sharing (<sha>) on main."""
    floor = not_before - timedelta(seconds=5)
    matches = []
    for run in list_runs():
        created_at = parse_created(run)
        if created_at is None or created_at < floor:
            continue
        if not run_identity_matches(run, sha):
            continue
        matches.append(run)
    if not matches:
        return None
    return max(matches, key=lambda run: run["id"])


def job_rows(run_id: int) -> list[dict]:
    result = gh_api(f"repos/{PEOPLE}/actions/runs/{run_id}/jobs?per_page=50")
    if result.returncode != 0:
        return []
    return json.loads(result.stdout).get("jobs", [])


def print_legs(run_id: int, html_url: str) -> None:
    jobs = {job.get("name"): job for job in job_rows(run_id)}
    print(f"people-dev-sharing-gate: run {html_url}", flush=True)
    for name in SHARING_LEGS:
        job = jobs.get(name)
        if job is None:
            print(f"  - {name}: absent", flush=True)
            continue
        print(
            f"  - {name}: {job.get('conclusion') or job.get('status')}",
            flush=True,
        )


def wait_for_run(sha: str, not_before: datetime, wait_seconds: int) -> dict:
    deadline = time.monotonic() + wait_seconds
    run = None
    while time.monotonic() < deadline:
        run = find_run(sha, not_before)
        if run is not None:
            break
        print(
            "people-dev-sharing-gate: waiting for a workflow_dispatch whose "
            f"run-name is exactly {expected_run_name(sha)!r} on main",
            flush=True,
        )
        time.sleep(15)
    if run is None:
        print(
            f"people-dev-sharing-gate: FAIL no People DEV sharing run named "
            f"{expected_run_name(sha)!r} on main appeared within {wait_seconds}s. "
            "People main must embed lastdb_source_sha in run-name (people #37). "
            "There is no newest-dispatch fallback.",
            file=sys.stderr,
        )
        sys.exit(1)
    run_id = run["id"]
    html_url = run.get("html_url", "")
    title = run.get("display_title", "")
    print(
        f"people-dev-sharing-gate: watching run {run_id} ({title}) {html_url}",
        flush=True,
    )
    while time.monotonic() < deadline:
        result = gh_api(f"repos/{PEOPLE}/actions/runs/{run_id}")
        if result.returncode != 0:
            print(
                f"people-dev-sharing-gate: run query failed, retrying: "
                f"{(result.stderr or '')[:200]}",
                flush=True,
            )
            time.sleep(30)
            continue
        run = json.loads(result.stdout)
        # Re-check identity so a watched id can never authorize another SHA/ref.
        if not run_identity_matches(run, sha):
            print(
                f"people-dev-sharing-gate: FAIL watched run {run_id} identity "
                f"title={run.get('display_title')!r} head_branch={run.get('head_branch')!r} "
                f"does not match {expected_run_name(sha)!r} on main",
                file=sys.stderr,
            )
            sys.exit(1)
        status = run.get("status")
        if status == "completed":
            return run
        print(f"people-dev-sharing-gate: run status {status}", flush=True)
        time.sleep(30)
    print_legs(run_id, html_url)
    print(
        f"people-dev-sharing-gate: FAIL run {run_id} did not finish within "
        f"{wait_seconds}s: {html_url}",
        file=sys.stderr,
    )
    sys.exit(1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sha", required=True, help="40-hex LastDB commit to prove")
    parser.add_argument("--mode", default="all", help="People sharing mode (default all)")
    parser.add_argument("--wait-seconds", type=int, default=7200)
    args = parser.parse_args()
    sha = args.sha.strip().lower()
    if not SHA_RE.match(sha):
        print(
            f"people-dev-sharing-gate: FAIL sha must be 40 hex, got {args.sha!r}",
            file=sys.stderr,
        )
        return 2
    require_token()
    not_before = datetime.now(timezone.utc)
    dispatch(sha, args.mode)
    run = wait_for_run(sha, not_before, args.wait_seconds)
    if not run_identity_matches(run, sha):
        print(
            f"people-dev-sharing-gate: FAIL completed run {run.get('id')} identity "
            f"title={run.get('display_title')!r} head_branch={run.get('head_branch')!r} "
            f"does not match {expected_run_name(sha)!r} on main",
            file=sys.stderr,
        )
        return 1
    print_legs(run["id"], run.get("html_url", ""))
    conclusion = run.get("conclusion")
    if conclusion == "success":
        print(
            f"people-dev-sharing-gate: OK {run.get('html_url')} "
            f"identity={expected_run_name(sha)}",
        )
        return 0
    print(
        f"people-dev-sharing-gate: FAIL People DEV sharing concluded "
        f"{conclusion}: {run.get('html_url')}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
