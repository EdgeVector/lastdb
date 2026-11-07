#!/usr/bin/env python3
"""Require successful source gates before the daily workflow receives DEV rights."""

import os
from shared_daily_io import github
from shared_origin import owner
from support import decode_json, require


identity = owner(os.environ.get("GITHUB_SHA", ""))
checks = github(f"commits/{identity['source']}/check-runs?per_page=100")["check_runs"]
for name in ("ci-required", "benchmark-complete"):
    require(any(c["name"] == name and c["conclusion"] == "success"
                and c["head_sha"] == identity["source"] and c["app"]["slug"] == "github-actions"
                for c in checks), "shared_daily_accepted_source_gate_" + name)
with open(os.environ["GITHUB_OUTPUT"], "a") as output:
    output.write("operation=" + os.environ["SELECTED_OPERATION"] + "\n")
print("Accepted source gates passed")
