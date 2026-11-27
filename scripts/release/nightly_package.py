#!/usr/bin/env python3
"""Build, package, and validate the matched LastDB release CLI and daemon."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import file_sha256  # noqa: E402

TARGETS = {
    "aarch64-apple-darwin": ("darwin", "arm64"),
    "x86_64-unknown-linux-gnu": ("linux", "x86_64"),
}
INDEX = "nightly.json"
SUMS = "SHA256SUMS.txt"
MAX_FILE = 512 * 1024 ** 2
HEX = re.compile(r"[0-9a-f]{64}\Z")


def digest(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= MAX_FILE:
        raise ValueError(f"Invalid release asset: {path.name}")
    return file_sha256(path)


def document(path):
    path = Path(path)
    if path.is_symlink() or not 0 < path.stat().st_size <= 1024 ** 2:
        raise ValueError(f"Invalid release metadata: {path.name}")
    return json.loads(path.read_text())


def names():
    return {f"lastdb-{target}{suffix}" for target in TARGETS
            for suffix in (".tar.gz", ".manifest.json", ".proof.json")} | {INDEX, SUMS}


def binary_version(path):
    result = subprocess.run([str(path), "--version"], check=True, capture_output=True, text=True, timeout=20)
    words = result.stdout.strip().split()
    if len(words) != 2 or words[0] != path.name:
        raise ValueError(f"Unexpected version response from {path.name}")
    return words[1]


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    temporary.replace(path)


def validate_pair(directory, version):
    binaries = []
    for name in ("lastdb", "lastdbd"):
        path = (directory / name).absolute()
        if path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"The release lacks a regular executable {name}")
        if not 0 < path.stat().st_size <= MAX_FILE:
            raise ValueError(f"The release executable {name} exceeds its size limit")
        if binary_version(path) != version:
            raise ValueError(f"The release executable {name} has a different version")
        binaries.append({"name": name, "version": version, "sha256": digest(path), "size": path.stat().st_size})
    return binaries


def validate_architecture(directory, target):
    for name in ("lastdb", "lastdbd"):
        with (directory / name).open("rb") as stream:
            header = stream.read(20)
        if target == "aarch64-apple-darwin":
            valid = header[:4] == b"\xcf\xfa\xed\xfe" and int.from_bytes(header[4:8], "little") == 0x100000c
        else:
            valid = header[:6] == b"\x7fELF\x02\x01" and int.from_bytes(header[18:20], "little") == 62
        if not valid:
            raise ValueError(f"The executable {name} does not match {target}")


def archive_pair(path, directory, binaries, epoch):
    # Fixed ownership, mode, order, and timestamps remove archive-only changes.
    with path.open("wb") as stream:
        with gzip.GzipFile(filename="", fileobj=stream, mode="wb", mtime=epoch) as zipped:
            with tarfile.open(fileobj=zipped, mode="w|") as archive:
                for binary in binaries:
                    member = tarfile.TarInfo(binary["name"])
                    member.size, member.mode, member.mtime = binary["size"], 0o755, epoch
                    member.uid = member.gid = 0
                    member.uname = member.gname = ""
                    with (directory / binary["name"]).open("rb") as content:
                        archive.addfile(member, content)


def package_bundle(target, bin_dir, out_dir, git_oid, version, epoch, *, nightly_only=False):
    if not re.fullmatch(r"[0-9a-f]{40}", git_oid):
        raise ValueError("A full source commit is required")
    is_nightly = bool(re.fullmatch(r"\d+\.\d+\.\d+-nightly\.\d{8}\.g[0-9a-f]{9}", version))
    if nightly_only and not is_nightly:
        raise ValueError("The nightly version is invalid")
    if is_nightly and not version.endswith(".g" + git_oid[:9]):
        raise ValueError("The version and source commit differ")
    if not is_nightly and not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("The release version is invalid")
    if not 0 <= epoch <= 0xffffffff:
        raise ValueError("The source timestamp exceeds the archive format")
    out_dir.mkdir(parents=True, exist_ok=True)
    binaries = validate_pair(bin_dir, version)
    validate_architecture(bin_dir, target)
    archive = out_dir / f"lastdb-{target}.tar.gz"
    archive_pair(archive, bin_dir, binaries, epoch)
    if archive.stat().st_size > MAX_FILE:
        raise ValueError("The archive exceeds the 512 MiB release asset limit")
    system, architecture = TARGETS[target]
    write_json(out_dir / f"lastdb-{target}.manifest.json", {
        "schema": "lastdb.mini.bundle.v1", "target": target,
        "source_git_oid": git_oid, "version": version,
        "artifact": {"file": archive.name, "sha256": digest(archive), "size": archive.stat().st_size},
        "binaries": binaries, "platform": {"os": system, "arch": architecture},
        "build_host": {"system": platform.system(), "release": platform.release(), "machine": platform.machine()},
        "signing": "ad-hoc" if system == "darwin" else "none",
    })
    print(f"Matched release bundle: {target} {git_oid} {version}")


def build_target(args):
    if args.plan is not None:
        plan = document(args.plan)
        git_oid, version, epoch = plan["source"], plan["version"], plan["epoch"]
        nightly_only = False
    else:
        if not args.git_oid or not args.expect_version or args.source_epoch is None:
            raise ValueError("build-target requires --plan or --git-oid, --expect-version, and --source-epoch")
        git_oid, version, epoch = args.git_oid, args.expect_version, args.source_epoch
        nightly_only = "-nightly." in version
    env = {**os.environ, "FOLDDB_BUILD_VERSION_OVERRIDE": version}
    if args.target.startswith("aarch64-"):
        # .cargo/config.toml turns on the ARMv8 AES and PMULL backends for this target. Any of
        # these variables, even empty, replaces those flags and ships the software cipher.
        target_flags = "CARGO_TARGET_" + args.target.upper().replace("-", "_") + "_RUSTFLAGS"
        overriding = [name for name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", target_flags)
                      if name in env]
        if overriding:
            raise RuntimeError(f"{', '.join(overriding)} would drop the hardware AES cfgs of "
                               ".cargo/config.toml; unset it for the release build")
    subprocess.run(["rustup", "target", "add", args.target], env=env, check=True, timeout=300)
    if args.target == "aarch64-apple-darwin":
        if subprocess.run(["brew", "list", "--versions", "openssl@3"], env=env,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60).returncode != 0:
            subprocess.run(["brew", "install", "openssl@3"],
                           env={**env, "HOMEBREW_NO_AUTO_UPDATE": "1"}, check=True, timeout=900)
        env["OPENSSL_DIR"] = subprocess.check_output(
            ["brew", "--prefix", "openssl@3"], env=env, text=True, timeout=30).strip()
    cmd = ["cargo", "build", "--locked", "--release", "--target", args.target,
           "-p", "lastdb_node", "--bin", "lastdb", "--bin", "lastdbd"]
    if args.build_log is not None:
        args.build_log.parent.mkdir(parents=True, exist_ok=True)
        with args.build_log.open("w", encoding="utf-8") as log:
            proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            assert proc.stdout is not None
            for line in proc.stdout:
                sys.stdout.write(line)
                sys.stdout.flush()
                log.write(line)
            if proc.wait() != 0:
                raise subprocess.CalledProcessError(proc.returncode, cmd)
    else:
        subprocess.run(cmd, env=env, check=True)
    bin_dir = Path(env.get("CARGO_TARGET_DIR") or "target").resolve() / args.target / "release"
    if args.target.startswith("aarch64-"):
        # Before strip: the daemon must hold the ARMv8 backends of aes and polyval. Other
        # crates (ring, aws-lc) carry their own AES code, so the instructions alone prove nothing.
        symbols = subprocess.check_output(["nm", str(bin_dir / "lastdbd")], text=True, timeout=300)
        missing = [crate for crate, mark in (("aes", "3aes5armv8"), ("polyval", "7polyval7backend5pmull"))
                   if mark not in symbols]
        if missing:
            raise RuntimeError(f"lastdbd lacks the ARMv8 backend of {', '.join(missing)}; "
                               "check the cfgs in .cargo/config.toml")
    for name in ("lastdb", "lastdbd"):
        path = bin_dir / name
        subprocess.run(["strip", str(path)], check=True, timeout=60)
        if args.target == "aarch64-apple-darwin":
            subprocess.run(["codesign", "--force", "--sign", "-", str(path)], check=True, timeout=60)
            subprocess.run(["codesign", "--verify", "--strict", str(path)], check=True, timeout=60)
            otool = subprocess.check_output(["otool", "-L", str(path)], text=True, timeout=30)
            bad = [ln.strip() for ln in otool.splitlines()
                   if re.search(r"lib(ssl|crypto)\.", ln) and "/opt/homebrew/opt/openssl@3/lib/" not in ln]
            if bad:
                raise RuntimeError(f"{path} links an OpenSSL other than openssl@3: {bad}")
        else:
            ldd = subprocess.check_output(["ldd", "-r", str(path)], stderr=subprocess.STDOUT, text=True, timeout=30)
            if "not found" in ldd or "undefined symbol" in ldd:
                raise RuntimeError(f"{path} failed shared-library verification:\n{ldd}")
    package_bundle(args.target, bin_dir, args.out_dir, git_oid, version, epoch, nightly_only=nightly_only)


@contextmanager
def open_executable_pair(bundle):
    with tarfile.open(bundle, "r:gz") as archive:
        members = []
        for member in archive:
            if (len(members) == 2 or member.name not in {"lastdb", "lastdbd"}
                    or not member.isfile() or not 0 < member.size <= MAX_FILE):
                raise ValueError("The archive exceeds the bounded executable pair")
            members.append(member)
        if len(members) != 2 or {member.name for member in members} != {"lastdb", "lastdbd"}:
            raise ValueError("The archive must contain only the matched executable pair")
        yield archive, members


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
    with open_executable_pair(bundle) as (archive, members):
        for member in members:
            item = expected[member.name]
            if (item["version"] != plan["version"] or not HEX.fullmatch(item["sha256"])
                    or type(item.get("size")) is not int or item["size"] != member.size
                    or member.mode != 0o755):
                raise ValueError("An executable's type, version, or size is invalid")
            with archive.extractfile(member) as source:
                stream_hash = hashlib.sha256()
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    stream_hash.update(chunk)
                if stream_hash.hexdigest() != item["sha256"]:
                    raise ValueError("An executable differs from its manifest hash")
    proof = document(folder / f"{prefix}.proof.json")
    checks = ("keyed_read", "range_read", "update", "reopen", "cli_status", "crash_recovery")
    if (proof.get("schema") != "lastdb.nightly.probe.v1" or proof.get("target") != target
            or proof.get("source_git_oid") != plan["source"] or proof.get("version") != plan["version"]
            or proof.get("bundle_sha256") != manifest["artifact"]["sha256"]
            or proof.get("complete") is not True
            or proof.get("records") != 8 or proof.get("daemon_stopped") is not True
            or proof.get("home_removed") is not True or type(proof.get("api_version")) is not int
            or proof["api_version"] <= 0 or not isinstance(proof.get("capabilities"), dict)
            or any(proof.get("checks", {}).get(key) is not True for key in checks)):
        raise ValueError(f"The {target} operational proof is incomplete")
    return {"manifest": manifest, "proof": proof}


def prepare(folder, plan):
    folder = Path(folder)
    if {path.name for path in folder.iterdir()} != names() - {INDEX, SUMS}:
        raise ValueError("The assembled bundle set differs from the fixed platform set")
    platforms = {target: validate_bundle(folder, target, plan) for target in TARGETS}
    index = {"schema": "lastdb.nightly.release.v1", "repository": plan["repository"],
             "source_git_oid": plan["source"], "version": plan["version"], "tag": plan["tag"],
             "run_url": plan["run_url"], "source_epoch": plan["epoch"], "platforms": platforms,
             "limits": "Prerelease; macOS ad-hoc signatures; no stable, Homebrew, registry, or primary promotion."}
    (folder / INDEX).write_text(json.dumps(index, indent=2, sort_keys=True) + "\n")
    files = sorted(names() - {SUMS})
    (folder / SUMS).write_text("".join(f"{digest(folder / name)}  {name}\n" for name in files))
    return validate(folder, plan)


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
    if raw.decode("utf-8").splitlines() != [f"{expected[name]}  {name}" for name in sorted(expected)]:
        raise ValueError("The final checksums do not cover the exact release assets")
    for target in TARGETS:
        if index["platforms"][target] != validate_bundle(folder, target, plan):
            raise ValueError("The release index differs from its platform metadata")
    return {name: digest(folder / name) for name in names()}


def main():
    if sys.argv[1:2] == ["build-target"]:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--target", choices=TARGETS, required=True)
        parser.add_argument("--out-dir", type=Path, required=True)
        parser.add_argument("--plan", type=Path)
        parser.add_argument("--git-oid")
        parser.add_argument("--expect-version")
        parser.add_argument("--source-epoch", type=int)
        parser.add_argument("--build-log", type=Path)
        build_target(parser.parse_args(sys.argv[2:]))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=TARGETS, required=True)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--git-oid", required=True)
    parser.add_argument("--expect-version", required=True)
    parser.add_argument("--source-epoch", type=int, required=True)
    args = parser.parse_args()
    package_bundle(args.target, args.bin_dir, args.out_dir, args.git_oid,
                   args.expect_version, args.source_epoch, nightly_only=True)


if __name__ == "__main__":
    main()
