#!/usr/bin/env python3
"""Require completed jobs and exact-source artifacts for the daily result."""

import argparse
import json
import os
from pathlib import Path
import sys

from shared_origin import owner
from support import OperationError, require


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--operation", choices=("suite", "cleanup"), required=True)
    parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    identity = owner(os.environ.get("GITHUB_SHA", ""))
    expected = ["cleanup"] + (["lifecycle", "conflicts", "retention"] if args.operation == "suite" else [])
    require(os.environ["ADMISSION_RESULT"] == "success" and os.environ["CLEANUP_RESULT"] == "success",
            "shared_daily_required_control_job")
    if args.operation == "suite":
        require(os.environ["BUILD_RESULT"] == "success" and os.environ["CASES_RESULT"] == "success",
                "shared_daily_required_product_jobs")
    rows, binaries = [], []
    for case in expected:
        path = args.directory / f"shared-daily-{case}.json"
        require(path.is_file() and path.stat().st_size <= 1024 * 1024, "shared_daily_result_artifact")
        result = json.loads(path.read_text())
        require(result["result"] == "passed" and result["source"] == identity["source"]
                and result["owner"] == identity, "shared_daily_result_identity")
        if case != "cleanup":
            require(result["case"] == case and result["phase"] == "fenced", "shared_daily_case_retirement")
            binaries.append(result["proof"]["binaries"])
        rows.append(case)
    require(not binaries or all(value == binaries[0] for value in binaries), "shared_daily_one_binary_pair")
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
        summary.write(f"## Shared DEV daily result\n\nSource `{identity['source']}`.\n\n")
        summary.write("| Operation | Result |\n| --- | --- |\n")
        for case in rows:
            summary.write(f"| {case} | passed |\n")
        summary.write("\nPhysical cleanup follows the one-hour drain for fenced scopes; "
                      "the ownership ledger and scope cap remain enforced.\n")
    print(json.dumps({"result": "passed", "source": identity["source"], "operations": rows}))


if __name__ == "__main__":
    try:
        main()
    except OperationError as error:
        raise SystemExit(str(error)) from None
