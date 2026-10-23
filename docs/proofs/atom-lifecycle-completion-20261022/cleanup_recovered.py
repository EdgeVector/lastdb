#!/usr/bin/env python3
"""Remove the failed generated home only after its separate recovery succeeds."""
import argparse
import json
from pathlib import Path
import shutil
import time

from recover_run import cleanup, owned_root


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    args = parser.parse_args()
    run = args.run.resolve(strict=True)
    root, ownership = owned_root(run)
    original = cleanup.load(run / "summary.json")
    recovery = cleanup.load(run / "recovery" / "summary.json")
    if (original.get("complete") is not False or recovery.get("complete") is not True
            or recovery.get("original_run_complete") is not False
            or recovery.get("all_owned_processes_stopped") is not True):
        raise ValueError("The failed capture needs a separate successful stopped recovery")
    stopped = cleanup.owned_processes(run / "recovery")
    cleanup.no_active_sockets(root)
    receipt = {"scratch": str(root), "ownership": ownership,
               "pre_cleanup": cleanup.inventory(root), "at_unix_ns": time.time_ns(),
               "recovery_stopped_pids": stopped, "original_run_complete": False,
               "recovery_complete": True}
    target = run / "cleanup.json"
    target.write_text(json.dumps({**receipt, "removed": False}, indent=2, sort_keys=True) + "\n")
    shutil.rmtree(root)
    if root.exists():
        raise RuntimeError("The exact owned scratch remains after cleanup")
    target.write_text(json.dumps({**receipt, "removed": True}, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"removed": True, "scratch": str(root), "receipt": str(target)}))


if __name__ == "__main__":
    main()
