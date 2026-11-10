#!/usr/bin/env python3
"""Wait for already-fenced owned scopes without weakening the cap or drain."""

import argparse
import math
import os
import time

import shared_daily_io as io
import shared_daily_scopes as scopes
from shared_origin import owner
from support import OperationError, require


MAX_WAIT_SECONDS = io.GRACE_MS // 1000 + 60


def slots(operation):
    require(operation in ("suite", "automatic", "cleanup", *io.MANUAL_CASES),
            "shared_daily_capacity_operation")
    return len(io.CASES) if operation == "suite" else int(operation != "cleanup")


def capacity(operation, check):
    io.admission()
    rows = scopes.directory()
    needed = max(0, len(rows) + slots(operation) - io.MAX_SCOPES)
    if check:
        require(needed == 0, "shared_daily_capacity_still_full")
        print("Owned scope capacity confirmed for " + operation)
        return
    wait = 0
    if needed:
        now = io.now_ms()
        eligible = []
        for row in rows:
            value = io.body(row)
            if value["phase"] != "fenced":
                continue
            fenced = value["fenced_at_ms"]
            require(type(fenced) is int and 0 < fenced <= now,
                    "shared_daily_capacity_fence_clock")
            eligible.append(max(0, fenced + io.GRACE_MS - now))
        require(len(eligible) >= needed, "shared_daily_capacity_not_enough_fenced_scopes")
        # Wait for enough scopes for the entire serialized matrix, not just its first case.
        wait = math.ceil(sorted(eligible)[needed - 1] / 1000) + 15
        require(0 < wait <= MAX_WAIT_SECONDS, "shared_daily_capacity_wait_bound")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"wait_seconds={wait}\n")
    print(f"Owned scopes: {len(rows)}; required slots: {slots(operation)}; drain wait: {wait}s")


def wait_without_credentials(seconds):
    owner(os.environ.get("GITHUB_SHA", ""))
    require(0 < seconds <= MAX_WAIT_SECONDS, "shared_daily_capacity_wait_bound")
    require(not any(os.environ.get(name) for name in (
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN",
        "AWS_PROFILE", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI", "GH_TOKEN", "GITHUB_TOKEN")),
        "shared_daily_capacity_wait_has_credentials")
    for name in list(os.environ):
        if name.startswith("AWS_") or name.startswith("ACTIONS_ID_TOKEN_"):
            os.environ.pop(name)
    # No calls to cloud services occur here; the next step obtains a new role session.
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        time.sleep(min(30, max(0, deadline - time.monotonic())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--operation")
    mode.add_argument("--wait-seconds", type=int)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.wait_seconds is not None:
        require(not args.check, "shared_daily_capacity_wait_arguments")
        wait_without_credentials(args.wait_seconds)
    else:
        capacity(args.operation, args.check)


if __name__ == "__main__":
    try:
        main()
    except OperationError as error:
        raise SystemExit(str(error)) from None
