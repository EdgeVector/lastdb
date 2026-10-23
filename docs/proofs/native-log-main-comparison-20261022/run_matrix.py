#!/usr/bin/env python3
"""Run the pinned real-daemon comparison sequentially, without compiler overlap."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    plan = json.loads((root/"plan.json").read_text())
    builds = json.loads((root/"builds.json").read_text())
    for mode in ("main", "candidate"):
        binary = args.scratch/mode
        if hashlib.sha256(binary.read_bytes()).hexdigest() != builds[mode]["sha256"]:
            raise RuntimeError("the pinned executable hash differs from the build receipt")
    outcomes = []
    for position, run in enumerate(plan["runs"]):
        command = [sys.executable, str(root/"capture.py"), "--binary", str(args.scratch/run["mode"]),
                   "--mode", run["mode"], "--source-commit", plan[run["mode"]+"_commit"],
                   "--home", str(args.scratch/("h"+str(position))), "--output", str(root/run["name"]),
                   "--count", str(plan["count_per_run"]), "--clients", str(run["clients"]),
                   "--batch-size", str(run["records_per_batch"])]
        print(json.dumps({"starting":run["name"],"position":position+1,"total":len(plan["runs"])}),flush=True)
        completed = subprocess.run(command, check=False)
        outcomes.append({"run":run["name"],"exit_code":completed.returncode,"command":command})
        (root/"run-outcomes.json").write_text(json.dumps(outcomes,indent=2)+"\n")
        if completed.returncode:
            raise RuntimeError("the real-daemon run failed; preserve its receipts before any retry")
    print("All pinned comparison runs completed.",flush=True)


if __name__ == "__main__":
    main()
