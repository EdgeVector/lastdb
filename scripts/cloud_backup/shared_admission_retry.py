#!/usr/bin/env python3
"""Retry one exact-main sharing run only if it failed before all cloud jobs."""

import json
import os
from pathlib import Path
import re
import subprocess


REPOSITORY = "EdgeVector/lastdb"
WORKFLOW = ".github/workflows/cloud-shared-daily.yml"


def require(value, message):
    if not value:
        raise RuntimeError(message)


def api(path, method="GET"):
    result = subprocess.run(
        ["gh", "api", "--method", method, f"repos/{REPOSITORY}/{path}"],
        stdin=subprocess.DEVNULL, capture_output=True, timeout=40, check=False,
    )
    require(result.returncode == 0, "sharing admission GitHub request failed")
    require(len(result.stdout) <= 2 * 1024 * 1024, "sharing admission GitHub response too large")
    return json.loads(result.stdout) if result.stdout.strip() else None


def successful_gates(source):
    for name in ("ci-required", "benchmark-complete"):
        checks = api(f"commits/{source}/check-runs?check_name={name}&per_page=100")["check_runs"]
        matches = [check for check in checks if check["name"] == name
                   and check["head_sha"] == source and check["app"]["slug"] == "github-actions"]
        latest = max(matches, key=lambda check: check["id"], default=None)
        if latest is None or latest["status"] != "completed" or latest["conclusion"] != "success":
            return False
    return True


def admission_only_failure(run):
    if run["status"] != "completed" or run["conclusion"] not in ("failure", "timed_out"):
        return False
    # One automatic retry is enough once both source gates have already passed.
    if run["run_attempt"] != 1:
        return False
    response = api(f"actions/runs/{run['id']}/jobs?filter=latest&per_page=100")
    jobs = response["jobs"]
    require(response["total_count"] == len(jobs), "sharing admission job discovery incomplete")
    admission = [job for job in jobs if job["name"] == "admission"]
    cleanup = [job for job in jobs if job["name"] == "Retire owned shared DEV scopes"]
    if len(admission) != 1 or admission[0]["conclusion"] not in ("failure", "timed_out"):
        return False
    if len(cleanup) != 1 or cleanup[0]["conclusion"] != "skipped":
        return False
    # The aggregate only reads artifacts. Every other job must have been skipped,
    # including unknown future jobs; never rerun cloud writes or failed cases.
    return all(job["conclusion"] == "skipped" for job in jobs
               if job["name"] not in ("admission", "Shared daily complete"))


def main():
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_EVENT_NAME") == "workflow_run"
            and os.environ.get("GITHUB_REF") == "refs/heads/main",
            "sharing admission retry requires accepted-main workflow_run")
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())["workflow_run"]
    source = event["head_sha"]
    require(re.fullmatch(r"[0-9a-f]{40}", source)
            and event["event"] == "push" and event["head_branch"] == "main"
            and event["head_repository"]["full_name"] == REPOSITORY
            and ((event["name"] in ("CI required", "LastDB benchmarks")
                  and event["conclusion"] == "success")
                 or (event["name"] == "Daily shared DEV operations"
                     and event["conclusion"] in ("failure", "timed_out"))),
            "sharing admission retry source is not an accepted main push")
    if not successful_gates(source):
        print("Both exact-source gates have not passed; no retry.")
        return
    response = api(f"actions/workflows/cloud-shared-daily.yml/runs?event=push&head_sha={source}&per_page=100")
    runs = response["workflow_runs"]
    require(response["total_count"] == len(runs), "sharing admission run discovery incomplete")
    matching = [run for run in runs if run["head_sha"] == source and run["head_branch"] == "main"
                and run["event"] == "push" and run["path"] == WORKFLOW]
    require(len(matching) <= 1, "sharing admission push run is ambiguous")
    if not matching or not admission_only_failure(matching[0]):
        print("No failed pre-cloud admission needs a retry.")
        return
    run = matching[0]
    require(isinstance(run["id"], int) and run["id"] > 0, "sharing admission run ID invalid")
    api(f"actions/runs/{run['id']}/rerun", "POST")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"Retried exact-source pre-cloud admission for `{source}`: "
                      f"https://github.com/{REPOSITORY}/actions/runs/{run['id']}\n")
    print("Requested one complete rerun after both exact-source gates passed.")


if __name__ == "__main__":
    main()
