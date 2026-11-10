#!/usr/bin/env python3
"""Require successful source gates before the daily workflow receives DEV rights."""

import json
import os
import time

from shared_daily_io import CASES, MANUAL_CASES, github
from shared_origin import owner
from support import OperationError, require


def wait_for_gates(source):
    # Daily work can start while main CI is still running. Never bypass its gates.
    budget = 480 if os.environ["SELECTED_OPERATION"] == "cleanup" else 3600
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        waiting = []
        for name in ("ci-required", "benchmark-complete"):
            checks = github(f"commits/{source}/check-runs?check_name={name}&per_page=100")["check_runs"]
            matching = [c for c in checks if c["name"] == name and c["head_sha"] == source
                        and c["app"]["slug"] == "github-actions"]
            latest = max(matching, key=lambda check: check["id"], default=None)
            if latest is None or latest["status"] != "completed":
                waiting.append(name)
            else:
                require(latest["conclusion"] == "success", "shared_daily_accepted_source_gate_failed_" + name)
        if not waiting:
            return
        remaining = deadline - time.monotonic()
        if remaining > 0:
            print("Waiting for exact-source gates: " + ", ".join(waiting), flush=True)
            time.sleep(min(30, remaining))
    raise OperationError("shared_daily_accepted_source_gate_deadline")


def main():
    identity = owner(os.environ.get("GITHUB_SHA", ""))
    selected = os.environ["SELECTED_OPERATION"]
    require(selected in ("suite", "cleanup", *MANUAL_CASES), "shared_daily_operation_selection")
    if selected in MANUAL_CASES:
        require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch",
                "shared_daily_manual_dispatch")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"## Shared DEV source admission\n\nOperation `{selected}`; "
                      f"LastDB source `{identity['source']}`.\n\n")
        try:
            wait_for_gates(identity["source"])
        except OperationError as error:
            summary.write(f"Admission blocked: `{error}`. No cloud operations were admitted.\n")
            raise
        summary.write("Both exact-source CI gates passed.\n")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write("operation=" + selected + "\n")
        cases = (selected,) if selected in MANUAL_CASES else CASES
        output.write("cases=" + json.dumps(cases) + "\n")
    print("Accepted source gates passed")


if __name__ == "__main__":
    try:
        main()
    except OperationError as error:
        raise SystemExit(str(error)) from None
