#!/usr/bin/env python3
"""Build, cache-await, and transfer verified benchmark programs across CI jobs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tarfile
import time
from urllib.parse import urlencode

from node import file_hash, save
from harness import clean_local_cargo_packages


COMMAND = ["cargo", "build", "--locked", "--profile", "dev-release",
           "-p", "lastdb_node", "--bin", "lastdbd", "--bin", "lastdb"]
FILES = ("lastdbd", "lastdb", "build.json")


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
                         for name in ("scripts/benchmarks/build.py", "scripts/harness.py",
                                      ".github/workflows/bench-build.yml",
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
        raise ValueError("The cached CLI reports another revision")
    return metadata


def compile_binary(args, directory, fingerprint):
    environment = os.environ.copy()
    target = Path(os.environ["RUNNER_TEMP"]) / f"bench-target-{args.label}"
    environment.update(CARGO_TARGET_DIR=str(target), FOLDDB_BUILD_VERSION_OVERRIDE=f"bench-{args.revision}")
    dependencies = clean_local_cargo_packages(
        target,
        profile="dev-release",
        source=args.source,
        environment=environment,
        verify_binaries=("lastdb", "lastdbd"),
    )
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
                "dependency_target": dependencies,
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
        if not reused:
            dependencies = metadata["dependency_target"]
            summary.write(f"Dependency target present: {dependencies['target_preexisted']}. "
                          f"Local packages cleaned: {len(dependencies['cleaned_path_packages'])}. "
                          f"Cleanup seconds: {dependencies['clean_seconds']}.\n")
    print(json.dumps(details), flush=True)


def github(path, query):
    endpoint = f"repos/{os.environ['GITHUB_REPOSITORY']}/{path}?{urlencode(query)}"
    result = subprocess.run(["gh", "api", endpoint], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError("GitHub cache availability could not be read")
    return json.loads(result.stdout)


def producer_pending(revision):
    runs = github("actions/workflows/bench.yml/runs",
                  {"event": "push", "branch": "main", "head_sha": revision, "per_page": 20})
    for run in runs["workflow_runs"]:
        if (run["head_sha"] != revision or run["event"] != "push"
                or run["head_branch"] != "main" or run["status"] == "completed"):
            continue
        jobs = github(f"actions/runs/{run['id']}/jobs", {"per_page": 100})
        for job in jobs["jobs"]:
            for step in job["steps"]:
                if step["name"] == "Save the accepted candidate for future baselines":
                    return step["status"] != "completed"
        return True
    return False


def cache_available(key):
    caches = github("actions/caches", {"key": key, "ref": "refs/heads/main", "per_page": 100})
    return any(cache["key"] == key and cache["ref"] == "refs/heads/main"
               and cache["size_in_bytes"] > 0 for cache in caches["actions_caches"])


def wait_cache(args):
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        raise ValueError("The baseline revision must be a complete SHA")
    deadline = time.monotonic() + args.timeout
    ready, reason = False, "no active main producer"
    try:
        while True:
            ready = cache_available(args.key)
            if ready:
                reason = "exact main cache is available"
                break
            if not producer_pending(args.revision):
                ready = cache_available(args.key)
                reason = "exact main cache is available" if ready else "no pending compatible main cache upload"
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                reason = "main cache wait reached its ten-minute limit"
                break
            print("Exact baseline cache is pending; the trusted main build is active.", flush=True)
            time.sleep(min(15, remaining))
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, subprocess.SubprocessError):
        reason = "cache lookup unavailable; build the baseline from source"
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as stream:
        stream.write(f"ready={str(ready).lower()}\n")
    print(reason, flush=True)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\nBaseline cache: {reason}.\n")


def configuration():
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        raise ValueError("Program transfer requires a GitHub-hosted Actions runner")
    revisions = {label: os.environ[f"{label.upper()}_SHA"] for label in ("baseline", "candidate")}
    fingerprint = os.environ["BUILD_CONTEXT"]
    if (any(not re.fullmatch(r"[0-9a-f]{40}", revision) for revision in revisions.values())
            or not re.fullmatch(r"[0-9a-f]{64}", fingerprint)):
        raise ValueError("Program transfer requires complete source and build identities")
    return revisions, fingerprint


def pack(args, revisions, fingerprint):
    if args.archive.exists():
        raise ValueError("Refuse to replace an existing program bundle")
    for revision in set(revisions.values()):
        verify(args.root / revision, revision, fingerprint)
    with tarfile.open(args.archive, "w:gz") as archive:
        for revision in sorted(set(revisions.values())):
            for name in FILES:
                archive.add(args.root / revision / name, arcname=f"{revision}/{name}", recursive=False)
    digest = file_hash(args.archive)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"sha256={digest}\n")
    print(f"Program bundle SHA-256: {digest}", flush=True)


def verify_context(archive, fingerprint):
    ctx = json.loads((archive.parent / "build-context.json").read_text())
    actual = hashlib.sha256(json.dumps(ctx["inputs"], sort_keys=True).encode()).hexdigest()
    if ctx["fingerprint"] != fingerprint or actual != fingerprint:
        raise ValueError("The downloaded build context differs from the producer output")
    inputs = ctx["inputs"]
    consumer = native_context()
    if inputs["native"]["os"] != consumer["os"] or inputs["architecture"] != platform.machine():
        raise ValueError("The measurement runner has another OS or architecture")
    driver = Path(__file__).resolve().parents[2]
    if any(file_hash(driver / name) != digest for name, digest in inputs["recipe_files"].items()):
        raise ValueError("The measurement driver differs from the verified build recipe")
    return consumer


def runtime_dependencies(programs):
    dependencies = {}
    for program in programs:
        result = subprocess.run(["ldd", "-r", str(program)], text=True, capture_output=True,
                                check=True, timeout=30)
        output = result.stdout + result.stderr
        if "not found" in output or "undefined symbol" in output:
            raise ValueError(f"The measurement runner cannot load {program.name}: {output}")
        dependencies[str(program)] = output
    return dependencies


def unpack(args, revisions, fingerprint):
    digest = os.environ["BUNDLE_SHA256"]
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or file_hash(args.archive) != digest:
        raise ValueError("The downloaded program archive differs from this run's producer")
    consumer = verify_context(args.archive, fingerprint)
    if args.root.exists():
        raise ValueError("Program transfer requires a fresh destination")
    expected = {f"{revision}/{name}" for revision in revisions.values() for name in FILES}
    with tarfile.open(args.archive, "r:gz") as archive:
        members = archive.getmembers()
        if {member.name for member in members} != expected or len(members) != len(expected):
            raise ValueError("The archive contains missing, duplicate, or unexpected program files")
        for member in members:
            if not member.isfile() or not 0 < member.size <= 512 * 2**20:
                raise ValueError("The program archive contains an invalid file")
        args.root.mkdir(parents=True, exist_ok=False)
        for member in members:
            target = args.root / member.name
            target.parent.mkdir(exist_ok=True)
            with archive.extractfile(member) as source, target.open("xb") as destination:
                while chunk := source.read(2**20):
                    destination.write(chunk)
            target.chmod(0o600 if target.name == "build.json" else 0o700)
    dependencies = runtime_dependencies([args.root / revision / name
                                         for revision in sorted(set(revisions.values()))
                                         for name in ("lastdbd", "lastdb")])
    programs = {label: verify(args.root / revision, revision, fingerprint)
                for label, revision in revisions.items()}
    receipt = {"bundle_sha256": digest, "context": fingerprint, "programs": programs,
               "consumer_run": os.environ["GITHUB_RUN_ID"],
               "consumer_image": os.environ["ImageVersion"], "consumer_native": consumer,
               "runtime_dependencies": dependencies}
    save(Path(os.environ["BENCH_OUTPUT"]) / "program-transfer.json", receipt)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\nVerified shared CI programs: `{digest}`  \n")
        for label, program in programs.items():
            summary.write(f"{label}: `{program['revision']}` / `{program['sha256']}`  \n")
    print("Verified both daemon and CLI identities, hashes, versions, recipe, and runtime dependencies.", flush=True)


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
    for name in ("pack", "unpack"):
        bundle = commands.add_parser(name)
        bundle.add_argument("--root", type=Path, required=True)
        bundle.add_argument("--archive", type=Path, required=True)
    cache_cmd = commands.add_parser("cache")
    cache_cmd.add_argument("--revision", required=True)
    cache_cmd.add_argument("--key", required=True)
    cache_cmd.add_argument("--timeout", type=int, choices=range(0, 601), default=600)
    args = parser.parse_args()
    if args.command == "context":
        context(args.output)
    elif args.command == "build":
        build(args)
    elif args.command == "cache":
        wait_cache(args)
    else:
        revisions, fingerprint = configuration()
        {"pack": pack, "unpack": unpack}[args.command](args, revisions, fingerprint)


if __name__ == "__main__":
    main()
