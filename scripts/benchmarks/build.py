#!/usr/bin/env python3
"""Reuse an exact benchmark baseline build; always build the candidate afresh."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import time

from node import file_hash, save


COMMAND = ["cargo", "build", "--locked", "--profile", "dev-release",
           "-p", "lastdb_node", "--bin", "lastdbd", "--bin", "lastdb"]


def native_context():
    release = platform.freedesktop_os_release()
    commands = {name: subprocess.check_output([name, "--version"], text=True).splitlines()[0]
                for name in ("cc", "c++", "ld", "ar", "cmake", "pkg-config", "ldd")}
    packages = subprocess.check_output(
        ["dpkg-query", "--show", "--showformat=${Package}=${Version}\n",
         "libc6", "libc6-dev", "libgcc-s1", "libstdc++6", "libssl-dev", "zlib1g-dev"], text=True)
    return {"os": {key: release[key] for key in ("ID", "VERSION_ID")},
            "tools": commands, "packages": sorted(packages.splitlines())}


def context(output):
    driver = Path(__file__).resolve().parents[2]
    settings = ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET",
                "CC", "CXX", "CFLAGS", "CXXFLAGS", "CPPFLAGS", "LDFLAGS")
    inputs = {
        "format_version": 2,
        "command": COMMAND,
        "rustc": subprocess.check_output(["rustc", "--version", "--verbose"], text=True).strip(),
        "cargo": subprocess.check_output(["cargo", "--version"], text=True).strip(),
        # Hosted image labels change for unrelated preinstalled software.
        # Reuse requires the actual native build environment instead.
        "native": native_context(),
        "architecture": os.uname().machine,
        "flags": {name: os.environ.get(name, "") for name in settings},
        "profiles": {name: value for name, value in os.environ.items()
                     if name.startswith("CARGO_PROFILE_")},
        "recipe_files": {name: file_hash(driver / name)
                         for name in ("scripts/benchmarks/build.py", ".github/workflows/bench-build.yml",
                                      ".github/workflows/bench-run.yml")},
    }
    fingerprint = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
    save(output, {"fingerprint": fingerprint, "inputs": inputs,
                  "runner_image": {"version": os.environ["ImageVersion"], "os": os.environ["ImageOS"]}})
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as stream:
        stream.write(f"fingerprint={fingerprint}\n")


def verify(directory, revision, fingerprint):
    binary, metadata_path = directory / "lastdbd", directory / "build.json"
    cli = directory / "lastdb"
    if any(path.is_symlink() for path in (directory, binary, cli, metadata_path)):
        raise ValueError("The cached build contains an unexpected symlink")
    metadata = json.loads(metadata_path.read_text())
    if (metadata["revision"] != revision or metadata["context"] != fingerprint
            or metadata["sha256"] != file_hash(binary)
            or metadata["cli_sha256"] != file_hash(cli)):
        raise ValueError("The cached build identity or checksum differs")
    version = subprocess.check_output([str(binary), "--version"], text=True, timeout=10).strip()
    if f"bench-{revision}" not in version or metadata["version"] != version:
        raise ValueError("The cached executable reports another revision")
    cli_version = subprocess.check_output([str(cli), "--version"], text=True, timeout=10).strip()
    if f"bench-{revision}" not in cli_version or metadata["cli_version"] != cli_version:
        raise ValueError("The cached conversion CLI reports another revision")
    return metadata


def compile_binary(args, directory, fingerprint):
    environment = os.environ.copy()
    target = Path(os.environ["RUNNER_TEMP"]) / f"bench-target-{args.label}"
    environment.update(CARGO_TARGET_DIR=str(target), FOLDDB_BUILD_VERSION_OVERRIDE=f"bench-{args.revision}")
    # No compiled target directory enters the cache. Each build starts in a fresh job.
    if target.exists():
        raise ValueError("The build target already exists; refuse stale build inputs")
    log_path = args.output / f"build-{args.label}.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(COMMAND, cwd=args.source, env=environment,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            log.write(line)
            print(line, end="", flush=True)
        if process.wait():
            raise RuntimeError(f"The {args.label} build failed; inspect {log_path.name}")
    if any(path.is_symlink() for path in (directory, directory / "lastdbd", directory / "lastdb",
                                        directory / "build.json")):
        raise ValueError("Refuse to replace a cached build through a symlink")
    directory.mkdir(parents=True, exist_ok=True)
    binary = directory / "lastdbd"
    shutil.copy2(target / "dev-release" / "lastdbd", binary)
    cli = directory / "lastdb"
    shutil.copy2(target / "dev-release" / "lastdb", cli)
    version = subprocess.check_output([str(binary), "--version"], text=True, timeout=10).strip()
    metadata = {"revision": args.revision, "context": fingerprint,
                "sha256": file_hash(binary), "version": version,
                "cli_sha256": file_hash(cli),
                "cli_version": subprocess.check_output([str(cli), "--version"], text=True,
                                                        timeout=10).strip(),
                "runner_image": json.loads(args.context.read_text())["runner_image"],
                "producer": f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}"
                            f"/actions/runs/{os.environ['GITHUB_RUN_ID']}"}
    save(directory / "build.json", metadata)
    return verify(directory, args.revision, fingerprint)


def build(args):
    started = time.monotonic()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        raise ValueError("The source revision must be a complete commit SHA")
    actual = subprocess.check_output(["git", "-C", str(args.source), "rev-parse", "HEAD"], text=True).strip()
    if actual != args.revision:
        raise ValueError("The source checkout differs from the selected revision")
    fingerprint = json.loads(args.context.read_text())["fingerprint"]
    if not re.fullmatch(r"[0-9a-f]{64}", fingerprint):
        raise ValueError("The build context fingerprint is invalid")
    args.output.mkdir(parents=True, exist_ok=True)
    directory = args.cache / args.revision
    metadata, reused = None, False
    if args.reuse:
        if args.label != "baseline":
            raise ValueError("The candidate must build from its current source")
        try:
            metadata = verify(directory, args.revision, fingerprint)
            reused = True
        except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as failure:
            print(f"::warning title=Baseline cache rejected::{failure}. Build from source instead.", flush=True)
    if metadata is None:
        metadata = compile_binary(args, directory, fingerprint)
    mode = "reused" if reused else "built from source"
    details = {"mode": mode, "prepare_seconds": round(time.monotonic() - started, 3), **metadata}
    if reused:
        (args.output / "build-baseline.log").write_text(json.dumps(details, indent=2) + "\n")
    save(args.output / f"build-{args.label}.json", details)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\n{args.label.capitalize()} program: **{mode}**  \n"
                      f"Source: {args.revision}  \nSHA-256: {metadata['sha256']}  \n"
                      f"Build: {metadata['producer']}  \n"
                      f"Prepare seconds: {details['prepare_seconds']}\n")
    print(json.dumps(details), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    identify = commands.add_parser("context")
    identify.add_argument("--output", type=Path, required=True)
    compile_one = commands.add_parser("build")
    for name in ("source", "cache", "context", "output"):
        compile_one.add_argument(f"--{name}", type=Path, required=True)
    compile_one.add_argument("--revision", required=True)
    compile_one.add_argument("--label", choices=("baseline", "candidate"), required=True)
    compile_one.add_argument("--reuse", action="store_true")
    args = parser.parse_args()
    if args.command == "context":
        context(args.output)
    else:
        build(args)


if __name__ == "__main__":
    main()
