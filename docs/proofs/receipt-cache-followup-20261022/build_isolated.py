#!/usr/bin/env python3
"""Build one exact source graph in a fresh empty owned Cargo target."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True
from build_variant import BUILD, DEV, ROOT, digest, frozen, output, save

DISK_NAMES = ("lastdb-disk-home", "lastdb-disk-recovery", "lastdb-pack-maintain")
DISK_BUILD = ["cargo", "build", "--locked", "--release", "-p", "laststore"]
for disk_name in DISK_NAMES:
    DISK_BUILD += ["--bin", disk_name]


def prepare(variant, repo):
    root = Path(tempfile.mkdtemp(prefix="rcbuild." + variant + ".", dir=os.environ["TMPDIR"])).resolve(strict=True)
    info = root.stat()
    if (root.parent != Path(os.environ["TMPDIR"]).resolve(strict=True) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700 or list(root.iterdir())):
        raise ValueError("The isolated build root must be fresh, private, owned, and empty")
    owner = {"version": 1, "root": str(root), "device": info.st_dev, "inode": info.st_ino,
             "owner_uid": info.st_uid, "primary_data": False, "run_id": uuid.uuid4().hex,
             "variant": variant, "source_worktree": str(repo)}
    save(root / "owner.json", owner)
    save(ROOT / (variant + "-isolated-owner.json"), owner)
    (root / "bin").mkdir(mode=0o700)
    target = root / "target"
    target.mkdir(mode=0o700)
    if list(target.iterdir()):
        raise ValueError("The per-variant Cargo target must start empty")
    return root, target, owner


def source_graph(repo, variant, environment):
    argv = ["cargo", "metadata", "--locked", "--format-version", "1"]
    process = subprocess.run(argv, cwd=repo, env=environment, capture_output=True, text=True, check=True)
    if len(process.stdout) > 16 * 1024 * 1024:
        raise ValueError("The local source graph exceeds its finite metadata bound")
    metadata = json.loads(process.stdout)
    rows = []
    for package in metadata["packages"]:
        if package["source"] is not None:
            continue
        manifest = Path(package["manifest_path"]).resolve(strict=True)
        if not manifest.is_relative_to(repo):
            raise ValueError("A local source manifest escapes its exact detached worktree")
        rows.append({"package": package["name"], "manifest": str(manifest), "manifest_sha256": digest(manifest)})
    if not {"laststore", "fold_db", "lastdb_node", "fastembed"}.issubset({row["package"] for row in rows}):
        raise ValueError("The engine, core, and node source packages are required")
    save(ROOT / (variant + "-isolated-source-graph.json"), {"argv": argv, "local_packages": rows,
         "metadata_stdout_sha256": hashlib.sha256(process.stdout.encode()).hexdigest(),
         "metadata_target_directory": metadata["target_directory"], "all_local_manifests_under_source": True})
    if metadata["target_directory"] != environment["CARGO_TARGET_DIR"]:
        raise ValueError("Cargo did not select the fresh isolated target directory")


def compile_step(repo, variant, label, argv, environment):
    stdout = ROOT / (variant + "-isolated-" + label + ".stdout.log")
    stderr = ROOT / (variant + "-isolated-" + label + ".stderr.log")
    if stdout.exists() or stderr.exists():
        raise ValueError("Do not replace original compiler logs")
    started = time.time_ns()
    with stdout.open("xb") as out, stderr.open("xb") as err:
        process = subprocess.run(argv, cwd=repo, env=environment, stdout=out, stderr=err, check=False)
    result = {"build_argv": argv, "build_command": " ".join(argv), "build_start_unix_ns": started,
              "build_end_unix_ns": time.time_ns(), "exit_code": process.returncode,
              "compiler_logs": {path.name: {"sha256": digest(path), "bytes": path.stat().st_size}
                                for path in (stdout, stderr)}}
    save(ROOT / (variant + "-isolated-" + label + "-command.json"), result)
    if process.returncode:
        raise RuntimeError("The fresh isolated build failed; preserve its original root and compiler logs")
    return result


def copied(target, root, variant, name):
    source = target / "release" / name
    destination = root / "bin" / (variant + "-" + name)
    if destination.exists() or source.is_symlink():
        raise ValueError("Copy only an original unlinked output into an unused owned binary path")
    shutil.copy2(source, destination)
    expected = digest(source)
    if digest(destination) != expected:
        raise ValueError("The copied binary differs from its own isolated target output")
    return {"binary": str(destination), "binary_sha256": expected,
            "isolated_target_output": str(source), "isolated_target_output_sha256": expected}


def dependency_sources(repo, target, variant):
    evidence = []
    for name in ("lastdbd", "lastdb", *DISK_NAMES):
        path = target / "release" / (name + ".d")
        raw = path.read_bytes()
        if len(raw) > 8 * 1024 * 1024:
            raise ValueError("A compiler dependency file exceeds its finite byte bound")
        paths = sorted(set(re.findall(rb"/Users/tomtang/\.fkanban/worktrees/[^\s\\:]+", raw)))
        if not paths or any(not text.decode().startswith(str(repo) + "/") for text in paths):
            raise ValueError("Compiler dependency evidence contains a different local source worktree")
        evidence.append({"binary": name, "dependency_file": str(path), "sha256": hashlib.sha256(raw).hexdigest(),
                         "local_source_paths": [text.decode() for text in paths]})
    save(ROOT / (variant + "-compiled-source-paths.json"), {"complete": True, "files": evidence,
         "all_local_compiler_paths_under_exact_source": True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "after"), required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    if repo.parent != DEV or args.repo.is_symlink() or os.environ.get("FOLD_HOST_CARGO_LOCK_HELD") != "1":
        raise ValueError("Use the exact detached DEV source and standard exclusive host lock")
    if any((ROOT / (args.variant + "-" + name)).exists() for name in ("build.json", "cli-build.json", "disk-build.json", "isolated-owner.json")):
        raise ValueError("Do not replace a canonical build receipt or isolated build owner")
    frozen(repo, args.revision)
    root, target, owner = prepare(args.variant, repo)
    environment = dict(os.environ)
    wrappers = ("RUSTC_WRAPPER", "RUSTC_WORKSPACE_WRAPPER", "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER")
    inherited = {name: name in environment for name in wrappers}
    for name in wrappers:
        environment[name] = ""
    for name in ("FOLDDB_BUILD_VERSION_OVERRIDE", "GITHUB_REF_NAME"):
        environment.pop(name, None)
    environment.update(CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL="0")
    source_graph(repo, args.variant, environment)
    shared = {"revision": args.revision, "source_worktree": str(repo), "product_source_clean": True,
              "rustc": output(repo, "rustc", "--version"), "cargo": output(repo, "cargo", "--version"),
              "build_scratch_owner": owner, "driver_sha256": digest(Path(__file__)),
              "build_environment": {"CARGO_TARGET_DIR": str(target), "CARGO_INCREMENTAL": "0", "profile": "release",
                  "package_features": "lastdb_node defaults: sentry-telemetry,purging-allocator",
                  "compiler_wrappers_disabled": list(wrappers), "inherited_wrapper_presence": inherited},
              "fresh_empty_per_variant_target": True, "isolated_source_graph": args.variant + "-isolated-source-graph.json"}
    node = compile_step(repo, args.variant, "node", BUILD, environment)
    frozen(repo, args.revision)
    for name in ("lastdbd", "lastdb"):
        item = copied(target, root, args.variant, name)
        version = subprocess.check_output([item["binary"], "--version"], text=True).strip()
        if args.revision[:9] not in version:
            raise ValueError("A freshly linked node or CLI has the wrong source version")
        suffix = "build.json" if name == "lastdbd" else "cli-build.json"
        save(ROOT / (args.variant + "-" + suffix), {**shared, **node, **item, "version": version})
    disk = compile_step(repo, args.variant, "physical", DISK_BUILD, environment)
    frozen(repo, args.revision)
    tools = {name: copied(target, root, args.variant, name) for name in DISK_NAMES}
    save(ROOT / (args.variant + "-disk-build.json"), {**shared, **disk, "binaries": tools,
         "build_environment": {**shared["build_environment"], "package_features": "laststore has no package feature declarations"}})
    dependency_sources(repo, target, args.variant)
    print(json.dumps({"isolated_build_complete": args.variant, "revision": args.revision, "root": str(root)}), flush=True)


if __name__ == "__main__":
    main()
