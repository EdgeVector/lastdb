"""Validate the matched native bundles and the release-only asset set."""

import hashlib
import json
from pathlib import Path
import re
import tarfile


TARGETS = ("aarch64-apple-darwin", "x86_64-unknown-linux-gnu")
INDEX = "nightly.json"
SUMS = "SHA256SUMS.txt"
MAX_FILE = 512 * 1024 ** 2
HEX = re.compile(r"[0-9a-f]{64}\Z")


def digest(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= MAX_FILE:
        raise ValueError(f"Invalid release asset: {path.name}")
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def document(path):
    path = Path(path)
    if path.is_symlink() or not 0 < path.stat().st_size <= 1024 ** 2:
        raise ValueError(f"Invalid release metadata: {path.name}")
    return json.loads(path.read_text())


def names():
    return {f"lastdb-{target}{suffix}" for target in TARGETS
            for suffix in (".tar.gz", ".manifest.json", ".proof.json")} | {INDEX, SUMS}


def validate_bundle(folder, target, plan):
    prefix = f"lastdb-{target}"
    bundle = folder / f"{prefix}.tar.gz"
    manifest = document(folder / f"{prefix}.manifest.json")
    if (manifest.get("schema") != "lastdb.mini.bundle.v1" or manifest.get("target") != target
            or manifest.get("source_git_oid") != plan["source"] or manifest.get("version") != plan["version"]
            or manifest["artifact"]["file"] != bundle.name
            or type(manifest["artifact"].get("size")) is not int
            or manifest["artifact"]["size"] != bundle.stat().st_size
            or manifest["artifact"]["sha256"] != digest(bundle)):
        raise ValueError(f"The {target} bundle manifest differs from its source or bytes")
    binaries = manifest["binaries"]
    if len(binaries) != 2 or {item["name"] for item in binaries} != {"lastdb", "lastdbd"}:
        raise ValueError("The release requires one matched CLI and daemon")
    expected = {item["name"]: item for item in binaries}
    with tarfile.open(bundle, "r:gz") as archive:
        members = []
        for member in archive:
            if (len(members) == 2 or member.name not in expected or not member.isfile()
                    or not 0 < member.size <= MAX_FILE):
                raise ValueError("The archive exceeds the executable pair")
            members.append(member)
        if len(members) != 2 or {member.name for member in members} != set(expected):
            raise ValueError("The archive contains files outside the executable pair")
        for member in members:
            item = expected[member.name]
            if (not member.isfile() or not 0 < member.size <= MAX_FILE
                    or item["version"] != plan["version"] or not HEX.fullmatch(item["sha256"])
                    or type(item.get("size")) is not int or item["size"] != member.size
                    or member.mode != 0o755):
                raise ValueError("An executable's type, version, or size is invalid")
            with archive.extractfile(member) as source:
                if hashlib.file_digest(source, "sha256").hexdigest() != item["sha256"]:
                    raise ValueError("An executable differs from its manifest hash")
    proof = document(folder / f"{prefix}.proof.json")
    checks = ("keyed_read", "range_read", "update", "reopen", "cli_status", "crash_recovery")
    if (proof.get("schema") != "lastdb.nightly.probe.v1" or proof.get("target") != target
            or proof.get("source_git_oid") != plan["source"] or proof.get("version") != plan["version"]
            or proof.get("bundle_sha256") != manifest["artifact"]["sha256"]
            or proof.get("complete") is not True
            or proof.get("records") != 8 or proof.get("daemon_stopped") is not True
            or proof.get("home_removed") is not True or type(proof.get("api_version")) is not int
            or proof["api_version"] <= 0
            or not isinstance(proof.get("capabilities"), dict)
            or any(proof.get("checks", {}).get(key) is not True for key in checks)):
        raise ValueError(f"The {target} operational proof is incomplete")
    return {"manifest": manifest, "proof": proof}


def prepare(folder, plan):
    folder = Path(folder)
    expected = names() - {INDEX, SUMS}
    if {path.name for path in folder.iterdir()} != expected:
        raise ValueError("The assembled bundle set differs from the fixed platform set")
    platforms = {target: validate_bundle(folder, target, plan) for target in TARGETS}
    index = {"schema": "lastdb.nightly.release.v1", "repository": plan["repository"],
             "source_git_oid": plan["source"], "version": plan["version"], "tag": plan["tag"],
             "run_url": plan["run_url"], "source_epoch": plan["epoch"], "platforms": platforms,
             "limits": "Prerelease; macOS ad-hoc signatures; no stable, Homebrew, registry, or primary promotion."}
    (folder / INDEX).write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
    files = sorted(names() - {SUMS})
    (folder / SUMS).write_text("".join(f"{digest(folder / name)}  {name}\n" for name in files))
    validate(folder, plan)


def validate(folder, plan):
    folder = Path(folder)
    if {path.name for path in folder.iterdir()} != names():
        raise ValueError("The release assets differ from the complete fixed set")
    index = document(folder / INDEX)
    if (index.get("schema") != "lastdb.nightly.release.v1"
            or index.get("repository") != plan["repository"] or index.get("source_git_oid") != plan["source"]
            or index.get("version") != plan["version"] or index.get("tag") != plan["tag"]
            or type(index.get("source_epoch")) is not int or index["source_epoch"] != plan["epoch"]
            or not re.fullmatch(r"https://github\.com/EdgeVector/lastdb/actions/runs/[0-9]+", index.get("run_url", ""))
            or set(index.get("platforms", {})) != set(TARGETS)):
        raise ValueError("The release index differs from the selected source")
    expected = {name: digest(folder / name) for name in names() - {SUMS}}
    sums = folder / SUMS
    if sums.is_symlink() or not sums.is_file() or not 0 < sums.stat().st_size <= 8192:
        raise ValueError("The final checksum file exceeds its bound")
    with sums.open("rb") as source:
        raw = source.read(8193)
    if len(raw) > 8192:
        raise ValueError("The final checksum file exceeds its read bound")
    lines = raw.decode("utf-8").splitlines()
    if lines != [f"{expected[name]}  {name}" for name in sorted(expected)]:
        raise ValueError("The final checksums do not cover the exact release assets")
    for target in TARGETS:
        if index["platforms"][target] != validate_bundle(folder, target, plan):
            raise ValueError("The release index differs from its platform metadata")
    return {name: digest(folder / name) for name in names()}
