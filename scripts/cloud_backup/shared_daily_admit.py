#!/usr/bin/env python3
"""Require successful source gates before the daily workflow receives DEV rights."""

import json
import os
import time

from shared_daily_io import CASES, MANUAL_CASES, github
from shared_origin import owner
from support import OperationError, require


def wait_for_gates(source):
    deadline = time.monotonic() + 480
    while time.monotonic() < deadline:
        checks = github(f"commits/{source}/check-runs?per_page=100")["check_runs"]
        waiting = []
        for name in ("ci-required", "benchmark-complete"):
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
            time.sleep(min(15, remaining))
    raise OperationError("shared_daily_accepted_source_gate_deadline")


def main():
    identity = owner(os.environ.get("GITHUB_SHA", ""))
    selected = os.environ["SELECTED_OPERATION"]
    require(selected in ("suite", "cleanup", *MANUAL_CASES), "shared_daily_operation_selection")
    if selected in MANUAL_CASES:
        require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch",
                "shared_daily_manual_dispatch")
    wait_for_gates(identity["source"])
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
