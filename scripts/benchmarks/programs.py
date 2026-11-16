#!/usr/bin/env python3
"""Await baseline caches and transfer verified programs between CI jobs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import tarfile
import time
from urllib.parse import urlencode

from build import native_context, verify
from node import file_hash, save


FILES = ("lastdbd", "lastdb", "build.json")


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
    context = json.loads((archive.parent / "build-context.json").read_text())
    actual = hashlib.sha256(json.dumps(context["inputs"], sort_keys=True).encode()).hexdigest()
    if context["fingerprint"] != fingerprint or actual != fingerprint:
        raise ValueError("The downloaded build context differs from the producer output")
    inputs = context["inputs"]
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
    for name in ("pack", "unpack"):
        bundle = commands.add_parser(name)
        bundle.add_argument("--root", type=Path, required=True)
        bundle.add_argument("--archive", type=Path, required=True)
    cache_cmd = commands.add_parser("cache")
    cache_cmd.add_argument("--revision", required=True)
    cache_cmd.add_argument("--key", required=True)
    cache_cmd.add_argument("--timeout", type=int, choices=range(0, 601), default=600)
    args = parser.parse_args()
    if args.command == "cache":
        wait_cache(args)
        return
    revisions, fingerprint = configuration()
    {"pack": pack, "unpack": unpack}[args.command](args, revisions, fingerprint)


if __name__ == "__main__":
    main()
