#!/usr/bin/env python3
"""Add physical command binaries from an already recorded clean source build."""

import argparse
import json
import os
from pathlib import Path
import sys

sys.dont_write_bytecode = True
import build_variant


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "after"), required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the standard external host Cargo lock")
    repo = args.repo.resolve(strict=True)
    if repo.parent != build_variant.DEV or args.repo.is_symlink():
        raise ValueError("Use the exact source-only DEV worktree")
    build_variant.frozen(repo, args.revision)
    scratch, ownership = build_variant.owner(args.scratch)
    prior = json.loads((build_variant.ROOT / f"{args.variant}-build.json").read_text())
    if (prior["revision"] != args.revision or prior["build_scratch_owner"] != ownership
            or prior["product_source_clean"] is not True):
        raise ValueError("The physical tools must use the exact preceding clean product source")
    environment = dict(os.environ)
    for name in ("FOLDDB_BUILD_VERSION_OVERRIDE", "GITHUB_REF_NAME"):
        environment.pop(name, None)
    target = Path(prior["build_environment"]["CARGO_TARGET_DIR"])
    environment["CARGO_TARGET_DIR"] = str(target)
    shared = {name: value for name, value in prior.items()
              if name not in ("binary", "binary_sha256", "version")}
    shared["disk_driver_sha256"] = build_variant.digest(Path(__file__))
    build_variant.build_disk(repo, args, scratch, environment, target, shared)
    print(json.dumps({"physical_tools_ready": True, "variant": args.variant,
                      "revision": args.revision, "scratch": str(scratch)}), flush=True)


if __name__ == "__main__":
    main()
