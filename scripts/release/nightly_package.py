#!/usr/bin/env python3
"""Build, package, and validate the matched LastDB release CLI and daemon."""

from __future__ import annotations

import argparse
import base64
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
import tempfile
import time

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import file_sha256  # noqa: E402

TARGETS = {
    "aarch64-apple-darwin": ("darwin", "arm64"),
    "x86_64-unknown-linux-gnu": ("linux", "x86_64"),
    "aarch64-unknown-linux-gnu": ("linux", "aarch64"),
}
# The manifest `signing` values of docs/designs/node-self-update.md section 3. `ad-hoc` exists
# only for pull request builds, which cannot reach the Apple credentials of the `node-release`
# environment; every other run requires `developer-id` on macOS and `none` on Linux.
SIGNING = ("developer-id", "ad-hoc")
ENTITLEMENTS = Path(__file__).with_name("entitlements.plist")
NOTARY_SECONDS = 3600
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
            machine = 62 if target.startswith("x86_64-") else 183
            valid = header[:6] == b"\x7fELF\x02\x01" and int.from_bytes(header[18:20], "little") == machine
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


def run_quiet(cmd, *, timeout, what, env=None):
    """Run a command whose arguments may carry a secret. Show output, never the arguments."""
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.returncode != 0:
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)
        raise RuntimeError(f"{what} failed with exit status {result.returncode}")
    return result.stdout


def apple_credentials():
    names = ("APPLE_CERTIFICATE", "APPLE_CERTIFICATE_PASSWORD", "APPLE_SIGNING_IDENTITY",
             "APPLE_ID", "APPLE_PASSWORD", "APPLE_TEAM_ID")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Developer ID signing needs these environment secrets of `node-release`: "
                           + ", ".join(missing))
    return {name: os.environ[name] for name in names}


@contextmanager
def signing_keychain(credentials):
    """A temporary keychain holding the Developer ID identity; removed on exit."""
    with tempfile.TemporaryDirectory(prefix="lastdb-sign-") as directory:
        directory = Path(directory)
        keychain = directory / "release.keychain-db"
        password = hashlib.sha256(os.urandom(32)).hexdigest()
        certificate = directory / "identity.p12"
        before = subprocess.check_output(["security", "list-keychains", "-d", "user"], text=True, timeout=30)
        listed = [line.strip().strip('"') for line in before.splitlines() if line.strip()]
        try:
            certificate.write_bytes(base64.b64decode(credentials["APPLE_CERTIFICATE"]))
            certificate.chmod(0o600)
            run_quiet(["security", "create-keychain", "-p", password, str(keychain)], timeout=60,
                      what="Creating the temporary keychain")
            run_quiet(["security", "set-keychain-settings", "-lut", "7200", str(keychain)], timeout=60,
                      what="Configuring the temporary keychain")
            run_quiet(["security", "unlock-keychain", "-p", password, str(keychain)], timeout=60,
                      what="Unlocking the temporary keychain")
            run_quiet(["security", "import", str(certificate), "-k", str(keychain),
                       "-P", credentials["APPLE_CERTIFICATE_PASSWORD"], "-f", "pkcs12",
                       "-T", "/usr/bin/codesign", "-T", "/usr/bin/security"], timeout=60,
                      what="Importing the Developer ID identity")
            run_quiet(["security", "set-key-partition-list", "-S", "apple-tool:,apple:,codesign:",
                       "-s", "-k", password, str(keychain)], timeout=60,
                      what="Allowing codesign to use the Developer ID key")
            run_quiet(["security", "list-keychains", "-d", "user", "-s", str(keychain), *listed], timeout=60,
                      what="Adding the temporary keychain to the search list")
            identities = subprocess.check_output(
                ["security", "find-identity", "-v", "-p", "codesigning", str(keychain)], text=True, timeout=60)
            if credentials["APPLE_SIGNING_IDENTITY"] not in identities:
                raise RuntimeError("The imported certificate does not hold the configured signing identity")
            yield keychain
        finally:
            subprocess.run(["security", "list-keychains", "-d", "user", "-s", *listed],
                           check=False, timeout=60)
            subprocess.run(["security", "delete-keychain", str(keychain)], check=False, timeout=60)
            if certificate.exists():
                certificate.unlink()


def notarize(paths, credentials):
    """Submit the matched pair as one zip and wait for Apple's verdict."""
    with tempfile.TemporaryDirectory(prefix="lastdb-notary-") as directory:
        directory = Path(directory)
        staging = directory / "pair"
        staging.mkdir()
        for path in paths:
            (staging / path.name).write_bytes(path.read_bytes())
        archive = directory / "lastdb-notarize.zip"
        subprocess.run(["ditto", "-c", "-k", "--norsrc", "--noextattr", str(staging), str(archive)],
                       check=True, timeout=300)
        auth = ["--apple-id", credentials["APPLE_ID"], "--password", credentials["APPLE_PASSWORD"],
                "--team-id", credentials["APPLE_TEAM_ID"]]
        print("Submitting the CLI and daemon to the Apple notary service (waiting for its verdict)")
        result = subprocess.run(
            ["xcrun", "notarytool", "submit", str(archive), *auth, "--wait",
             "--timeout", f"{NOTARY_SECONDS}s", "--output-format", "json"],
            capture_output=True, text=True, timeout=NOTARY_SECONDS + 300)
        try:
            verdict = json.loads(result.stdout)
        except ValueError:
            verdict = {}
        submission, status = verdict.get("id"), verdict.get("status")
        print(f"Notary submission {submission}: {status}")
        if result.returncode != 0 or status != "Accepted":
            if submission:
                log = subprocess.run(["xcrun", "notarytool", "log", submission, *auth],
                                     capture_output=True, text=True, timeout=300)
                print(log.stdout.strip())
            # The notary client names Apple's refusal (for example an expired agreement) here.
            print(verdict.get("message") or result.stderr.strip(), file=sys.stderr)
            raise RuntimeError("Apple did not accept the notarization submission")


def developer_id_sign(paths, credentials):
    """Sign with the hardened runtime and a secure timestamp, then notarize the pair."""
    with signing_keychain(credentials) as keychain:
        for path in paths:
            subprocess.run(["codesign", "--force", "--options", "runtime", "--timestamp",
                            "--entitlements", str(ENTITLEMENTS), "--keychain", str(keychain),
                            "--sign", credentials["APPLE_SIGNING_IDENTITY"], str(path)],
                           check=True, timeout=300)
            subprocess.run(["codesign", "--verify", "--strict", "--verbose=2", str(path)],
                           check=True, timeout=60)
            details = subprocess.run(["codesign", "--display", "--verbose=4", str(path)],
                                     capture_output=True, text=True, timeout=60).stderr
            if ("runtime" not in details or "Timestamp=" not in details
                    or f"TeamIdentifier={credentials['APPLE_TEAM_ID']}" not in details):
                raise RuntimeError(f"{path.name} lacks the hardened runtime, a timestamp, or the team identity")
    notarize(paths, credentials)
    # Gatekeeper reads the notarization ticket online for a bare executable, which may lag a few seconds.
    for path in paths:
        for attempt in range(6):
            check = subprocess.run(["spctl", "--assess", "--type", "execute", "-vv", str(path)],
                                   capture_output=True, text=True, timeout=120)
            print((check.stdout + check.stderr).strip())
            if check.returncode == 0 and "Notarized Developer ID" in check.stdout + check.stderr:
                break
            time.sleep(10)
        else:
            raise RuntimeError(f"Gatekeeper does not accept {path.name} as a notarized Developer ID program")


def manifest_signing(target, signing):
    """The only manifest `signing` values a bundle of this target may carry."""
    if TARGETS[target][0] != "darwin":
        return "none"
    if signing not in SIGNING:
        raise ValueError(f"The macOS signing mode {signing!r} is not one of {SIGNING}")
    if signing == "ad-hoc" and os.environ.get("GITHUB_EVENT_NAME") != "pull_request":
        raise ValueError("An ad-hoc macOS signature is allowed only in a pull request build")
    return signing


def package_bundle(target, bin_dir, out_dir, git_oid, version, epoch, *, nightly_only=False,
                   signing="developer-id"):
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
    signing = manifest_signing(target, signing)
    write_json(out_dir / f"lastdb-{target}.manifest.json", {
        "schema": "lastdb.mini.bundle.v1", "target": target,
        "source_git_oid": git_oid, "version": version,
        "artifact": {"file": archive.name, "sha256": digest(archive), "size": archive.stat().st_size},
        "binaries": binaries, "platform": {"os": system, "arch": architecture},
        "build_host": {"system": platform.system(), "release": platform.release(), "machine": platform.machine()},
        "signing": signing,
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
    # Build scripts and proc macros of every dependency run in these subprocesses. They never
    # see the Apple credentials, which only the signing step below reads.
    clean = {key: value for key, value in os.environ.items() if not key.startswith("APPLE_")}
    env = {**clean, "FOLDDB_BUILD_VERSION_OVERRIDE": version}
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
        # aes and polyval must have been compiled with their ARMv8 backend cfgs. The binary
        # cannot show it: the Apple target enables `aes` statically, so the backend inlines and
        # leaves no symbol, and ring and aws-lc carry AES instructions of their own. Cargo's
        # fingerprint of the newest build of each crate records the rustflags it used.
        for crate, cfg in (("aes", "aes_armv8"), ("polyval", "polyval_armv8")):
            prints = list((bin_dir / ".fingerprint").glob(f"{crate}-*/lib-{crate}.json"))
            if not prints:
                raise RuntimeError(f"no cargo fingerprint of {crate} under {bin_dir}")
            flags = document(max(prints, key=lambda p: p.stat().st_mtime)).get("rustflags", [])
            if not any(a == "--cfg" and b == cfg for a, b in zip(flags, flags[1:])):
                raise RuntimeError(f"{crate} was built without --cfg {cfg} (rustflags {flags}); "
                                   "check .cargo/config.toml")
    for name in ("lastdb", "lastdbd"):
        path = bin_dir / name
        subprocess.run(["strip", str(path)], check=True, timeout=60)
        if args.target == "aarch64-apple-darwin":
            otool = subprocess.check_output(["otool", "-L", str(path)], text=True, timeout=30)
            bad = [ln.strip() for ln in otool.splitlines()
                   if re.search(r"lib(ssl|crypto)\.", ln) and "/opt/homebrew/opt/openssl@3/lib/" not in ln]
            if bad:
                raise RuntimeError(f"{path} links an OpenSSL other than openssl@3: {bad}")
        else:
            ldd = subprocess.check_output(["ldd", "-r", str(path)], stderr=subprocess.STDOUT, text=True, timeout=30)
            if "not found" in ldd or "undefined symbol" in ldd:
                raise RuntimeError(f"{path} failed shared-library verification:\n{ldd}")
    signing = "none"
    if TARGETS[args.target][0] == "darwin":
        signing = manifest_signing(args.target, args.macos_signing)
        paths = [bin_dir / name for name in ("lastdb", "lastdbd")]
        if signing == "developer-id":
            developer_id_sign(paths, apple_credentials())
        else:
            for path in paths:
                subprocess.run(["codesign", "--force", "--sign", "-", str(path)], check=True, timeout=60)
                subprocess.run(["codesign", "--verify", "--strict", str(path)], check=True, timeout=60)
    package_bundle(args.target, bin_dir, args.out_dir, git_oid, version, epoch, nightly_only=nightly_only,
                   signing=signing)


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
    if manifest.get("signing") != manifest_signing(target, manifest.get("signing")):
        raise ValueError(f"The {target} bundle manifest names an unexpected signing mode")
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
             "limits": "Prerelease; macOS Developer ID signed and notarized; no stable, Homebrew, registry, or primary promotion."}
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
        parser.add_argument("--macos-signing", choices=SIGNING, default="developer-id")
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
