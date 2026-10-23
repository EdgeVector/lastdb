#!/usr/bin/env python3
"""Select exact source revisions and fail closed for non-documentation changes."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess


DOCUMENT_FILES = {".gitignore", ".gitattributes", ".github/CODEOWNERS", ".last-stack/pr-venue"}


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


def main():
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
    merge_base = git("merge-base", baseline, candidate).decode().strip()
    paths = git("diff", "--no-renames", "--name-only", "-z", merge_base, candidate)
    paths = [os.fsdecode(path) for path in paths.split(b"\0") if path]
    product = [path for path in paths
               if path not in DOCUMENT_FILES and Path(path).suffix.lower() not in {".md", ".rst"}]
    required = args.always or bool(product) or not paths
    result = {"required": str(required).lower(), "baseline": baseline, "candidate": candidate,
              "driver": git("rev-parse", "HEAD").decode().strip(),
              "reason": "product or benchmark configuration change" if product else
                        "scheduled, main, manual, or empty change" if required else "documentation-only change"}
    args.output.write_text(json.dumps({**result, "changed_paths": paths}, indent=2) + "\n")
    if args.github_output:
        with args.github_output.open("a") as stream:
            for key, value in result.items():
                stream.write(f"{key}={value}\n")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
