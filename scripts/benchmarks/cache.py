#!/usr/bin/env python3
"""Wait only for a trusted main build that can publish the exact baseline."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.parse import urlencode


def github(path, query):
    endpoint = f"repos/{os.environ['GITHUB_REPOSITORY']}/{path}?{urlencode(query)}"
    result = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError("GitHub cache availability could not be read")
    return json.loads(result.stdout)


def producer_pending(revision):
    runs = github("actions/workflows/bench.yml/runs",
                  {"event": "push", "branch": "main", "head_sha": revision, "per_page": 20})
    for run in runs["workflow_runs"]:
        if (run["head_sha"] != revision or run["event"] != "push"
                or run["head_branch"] != "main" or run["status"] == "completed"):
            continue
        jobs = github(f"actions/runs/{run['id']}/jobs", {"per_page": 100})
        for job in jobs["jobs"]:
            for step in job["steps"]:
                if step["name"] == "Save the accepted candidate for future baselines":
                    return step["status"] != "completed"
        # The run can be queued without jobs, or between plan and measurement.
        return True
    return False


def cache_available(key):
    caches = github("actions/caches", {"key": key, "ref": "refs/heads/main", "per_page": 100})
    return any(cache["key"] == key and cache["ref"] == "refs/heads/main"
               and cache["size_in_bytes"] > 0 for cache in caches["actions_caches"])


def wait(args):
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        raise ValueError("The baseline revision must be a complete SHA")
    deadline = time.monotonic() + args.timeout
    ready, reason = False, "no active main producer"
    try:
        while True:
            ready = cache_available(args.key)
            if ready:
                reason = "exact main cache is available"
                break
            if not producer_pending(args.revision):
                # The upload can finish between the cache read and the job read.
                ready = cache_available(args.key)
                reason = "exact main cache is available" if ready else "no pending compatible main cache upload"
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                reason = "main cache wait reached its ten-minute limit"
                break
            print("Exact baseline cache is pending; the trusted main build is active.", flush=True)
            time.sleep(min(15, remaining))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError):
        reason = "cache lookup unavailable; build the baseline from source"
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as stream:
        stream.write(f"ready={str(ready).lower()}\n")
    print(reason, flush=True)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\nBaseline cache: {reason}.\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--timeout", type=int, choices=range(0, 601), default=600)
    wait(parser.parse_args())


if __name__ == "__main__":
    main()
