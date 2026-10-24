#!/usr/bin/env python3
"""Reserve build and generated-home space on a GitHub benchmark runner."""

import argparse
import os
from pathlib import Path
import shutil


GIB = 1024 ** 3
# The 100,000-record nightly reached 29.8 GiB in run 40082466885.
# Homes run one at a time and are removed before the next version starts.
# Reserve 48 GiB for nightly growth, plus 32 GiB for two fresh build trees.
HOME_GIB = {"standard": 16, "nightly": 48, "batch-insert": 16}
BUILD_GIB = 32


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=HOME_GIB, required=True)
    parser.add_argument("--phase", choices=("build", "measure", "finish"), required=True)
    parser.add_argument("--require", action="store_true")
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        parser.error("Disk reservations require a GitHub Actions runner")
    paths = {Path(os.environ[name]).resolve(strict=True)
             for name in ("GITHUB_WORKSPACE", "RUNNER_TEMP")}
    free = min(shutil.disk_usage(path).free for path in paths)
    required_gib = 0 if args.phase == "finish" else HOME_GIB[args.profile]
    if args.phase == "build":
        required_gib += BUILD_GIB
    cleanup = free < required_gib * GIB
    message = (f"Disk before {args.phase} ({args.profile}): {free / GIB:.1f} GiB free; "
               f"{required_gib} GiB required. Cleanup required: {str(cleanup).lower()}.")
    print(message, flush=True)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\n{message}\n")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"cleanup-required={str(cleanup).lower()}\n")
    if args.require and cleanup:
        raise SystemExit("Insufficient disk space after cleanup; refuse an undersized benchmark runner")


if __name__ == "__main__":
    main()
