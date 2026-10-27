#!/usr/bin/env python3
"""Transfer exact, verified programs between jobs in one hosted CI run."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import tarfile

from build import native_context, verify
from node import file_hash, save


FILES = ("lastdbd", "lastdb", "build.json")


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
    if inputs["native"] != native_context() or inputs["architecture"] != platform.machine():
        raise ValueError("The measurement runner is incompatible with the build runner")
    driver = Path(__file__).resolve().parents[2]
    if any(file_hash(driver / name) != digest for name, digest in inputs["recipe_files"].items()):
        raise ValueError("The measurement driver differs from the verified build recipe")


def unpack(args, revisions, fingerprint):
    digest = os.environ["BUNDLE_SHA256"]
    if not re.fullmatch(r"[0-9a-f]{64}", digest) or file_hash(args.archive) != digest:
        raise ValueError("The downloaded program archive differs from this run's producer")
    verify_context(args.archive, fingerprint)
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
    programs = {label: verify(args.root / revision, revision, fingerprint)
                for label, revision in revisions.items()}
    receipt = {"bundle_sha256": digest, "context": fingerprint, "programs": programs,
               "consumer_run": os.environ["GITHUB_RUN_ID"],
               "consumer_image": os.environ["ImageVersion"]}
    save(Path(os.environ["BENCH_OUTPUT"]) / "program-transfer.json", receipt)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"\nVerified shared CI programs: `{digest}`  \n")
        for label, program in programs.items():
            summary.write(f"{label}: `{program['revision']}` / `{program['sha256']}`  \n")
    print("Verified both daemon and CLI identities, hashes, versions, recipe, and native compatibility.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("pack", "unpack"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    args = parser.parse_args()
    revisions, fingerprint = configuration()
    {"pack": pack, "unpack": unpack}[args.command](args, revisions, fingerprint)


if __name__ == "__main__":
    main()
