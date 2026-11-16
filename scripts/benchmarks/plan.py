#!/usr/bin/env python3
"""Select exact benchmark revisions and reserve build/corpus disk space."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


DOCUMENT_FILES = {".gitignore", ".gitattributes", ".github/CODEOWNERS", ".last-stack/pr-venue"}
UNBUILDABLE_REPLAY_MERGE = "84fc27b29c1d9036948ca3c28a30997f72d590de"
LAST_BUILDABLE_REPLAY_BASE = "b175c9e6aa88ab19d0ff7d9a8764c2ffa6917806"
GIB = 1024 ** 3
# The 100,000-record nightly reached 29.8 GiB in run 40082466885.
# Homes run one at a time and are removed before the next version starts.
# Reserve 48 GiB for nightly growth, plus 32 GiB for two fresh build trees.
HOME_GIB = {"standard": 16, "nightly": 48, "batch-insert": 16}
BUILD_GIB = 32


def git(*args):
    return subprocess.check_output(["git", *args])


def revision(value):
    if not re.fullmatch(r"[0-9a-f]{40}", value):
        raise ValueError("Each source revision must be a full 40-character Git SHA")
    exists = subprocess.run(["git", "cat-file", "-e", value + "^{commit}"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if exists.returncode:
        subprocess.run(["git", "fetch", "--no-tags", "origin", value], check=True)
    return git("rev-parse", value + "^{commit}").decode().strip()


def disk_budget(argv):
    parser = argparse.ArgumentParser(prog="plan.py disk", description=__doc__)
    parser.add_argument("--profile", choices=HOME_GIB, required=True)
    parser.add_argument("--phase", choices=("build", "measure", "finish"), required=True)
    parser.add_argument("--require", action="store_true")
    args = parser.parse_args(argv)
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
    return 0


def main():
    if sys.argv[1:2] == ["disk"]:
        return disk_budget(sys.argv[2:])
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default=os.environ.get("BASE_SHA", ""))
    parser.add_argument("--head", default=os.environ.get("HEAD_SHA", ""))
    parser.add_argument("--always", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--github-output", type=Path, default=os.environ.get("GITHUB_OUTPUT"))
    args = parser.parse_args()
    candidate = revision(args.head)
    base = args.base
    if not base or set(base) == {"0"}:
        base = git("rev-parse", candidate + "^").decode().strip()
    baseline = revision(base)
    requested_baseline = baseline
    event = os.environ.get("GITHUB_EVENT_NAME")
    if event == "pull_request":
        parents = git("rev-list", "--parents", "-n", "1", candidate).decode().split()
        if len(parents) != 3:
            raise ValueError("A PR comparison requires its exact two-parent candidate merge commit")
        baseline = revision(parents[1])
    if baseline == candidate:
        raise ValueError("Baseline and candidate revisions must differ; preserve the fresh candidate program")
    merge_base = git("merge-base", baseline, candidate).decode().strip()
    paths = git("diff", "--no-renames", "--name-only", "-z", merge_base, candidate)
    paths = [os.fsdecode(path) for path in paths.split(b"\0") if path]
    product = [path for path in paths
               if path not in DOCUMENT_FILES and Path(path).suffix.lower() not in {".md", ".rst"}]
    required = args.always or bool(product) or not paths
    reason = ("product or benchmark configuration change" if product else
              "scheduled, main, manual, or empty change" if required else "documentation-only change")
    if event in {"pull_request", "merge_group", "push"} and baseline == UNBUILDABLE_REPLAY_MERGE:
        repaired_base = revision(LAST_BUILDABLE_REPLAY_BASE)
        if git("rev-parse", baseline + "^").decode().strip() != repaired_base:
            raise ValueError("The replay repair benchmark base is not the direct accepted parent")
        reason += f". Measuring against accepted {repaired_base} because {baseline} fails to build"
        baseline = repaired_base
    result = {"required": str(required).lower(), "baseline": baseline, "candidate": candidate,
              "requested_baseline": requested_baseline,
              "driver": git("rev-parse", "HEAD").decode().strip(),
              "reason": reason}
    args.output.write_text(json.dumps({**result, "changed_paths": paths}, indent=2) + "\n")
    if args.github_output:
        with args.github_output.open("a") as stream:
            for key, value in result.items():
                stream.write(f"{key}={value}\n")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
