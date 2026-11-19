#!/usr/bin/env python3
"""Hold ci-required until the installed-app proof passes for a surface change.

A pull request that changes the socket surface file or an interface version
starts the stable release workflow, whose `app-compatibility` job runs the apps
from every supported signed list against the PR build. That workflow is not a
required check, so this job waits for that exact result on the PR head commit
and fails when it fails, is cancelled, or never appears.

A pull request that leaves those files unchanged, and every non-PR event,
passes at once.
"""

import argparse
import json
import subprocess
import sys
import time

REPOSITORY = "EdgeVector/lastdb"
ACTIONS_APP_ID = 15368
CHECK = "app-compatibility"
# The same paths that start stable-release.yml for a surface or version change.
TRIGGERS = ("lastdb_node/api-surface.json", "lastdb_node/src/version.rs")


def changed(base, head):
    names = subprocess.run(["git", "diff", "--name-only", f"{base}...{head}", "--", *TRIGGERS],
                           capture_output=True, text=True, check=True, timeout=60).stdout.split()
    return sorted(names)


def latest_check(head):
    shown = subprocess.run(
        ["gh", "api", f"repos/{REPOSITORY}/commits/{head}/check-runs?check_name={CHECK}&filter=latest"],
        capture_output=True, text=True, timeout=60)
    if shown.returncode != 0:
        print(f"installed-app-gate: check query failed, will retry: {shown.stderr.strip()[:200]}", flush=True)
        return None
    runs = [run for run in json.loads(shown.stdout).get("check_runs", [])
            if run.get("name") == CHECK and run.get("app", {}).get("id") == ACTIONS_APP_ID]
    return max(runs, key=lambda run: run["id"]) if runs else None


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", default="", help="PR base commit; empty for a non-PR event")
    parser.add_argument("--head", default="", help="PR head commit; empty for a non-PR event")
    parser.add_argument("--wait-seconds", type=int, default=3000)
    args = parser.parse_args()

    if not args.base or not args.head:
        print("installed-app-gate: not a pull request; nothing to wait for")
        return 0
    names = changed(args.base, args.head)
    if not names:
        print("installed-app-gate: the socket surface and interface versions are unchanged; nothing to wait for")
        return 0
    print(f"installed-app-gate: {', '.join(names)} changed; waiting for {CHECK} on {args.head}", flush=True)
    deadline = time.monotonic() + args.wait_seconds
    while True:
        check = latest_check(args.head)
        if check is not None and check.get("status") == "completed":
            if check.get("conclusion") == "success":
                print(f"installed-app-gate: OK {check['html_url']}")
                return 0
            print(f"installed-app-gate: FAIL {CHECK} concluded {check.get('conclusion')}: "
                  f"{check['html_url']}\nThe apps in a supported signed list do not work against this "
                  "build. Keep the app API compatible, or raise MIN_SUPPORTED_APP_API_VERSION.",
                  file=sys.stderr)
            return 1
        if time.monotonic() >= deadline:
            print(f"installed-app-gate: FAIL {CHECK} did not complete on {args.head} within "
                  f"{args.wait_seconds}s. The stable release workflow runs it for a same-repository "
                  "pull request; re-run that workflow, then this job.", file=sys.stderr)
            return 1
        state = "absent" if check is None else check.get("status")
        print(f"installed-app-gate: {CHECK} is {state}", flush=True)
        time.sleep(30)


if __name__ == "__main__":
    sys.exit(main())
