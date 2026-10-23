#!/usr/bin/env python3
"""Force the changed crates to rebuild before the paired product benchmark."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

HERE = Path(__file__).resolve().parent
WORKTREE = HERE.parents[2]
BEFORE = WORKTREE.parent / "lastdb-direct-compaction-benchmark-before-20261023"
TARGET = WORKTREE.parent / "lastdb-direct-atom-lifecycle-completion-20261022" / "target"
SOURCES = {"before": (BEFORE, "c6b4f7ed9ac5a24169e3f41a66579fdc5ba80de7"),
           "after": (WORKTREE, "c94ec77605b40a1d99398ab24118b7856bff310d")}
MARKERS = (b"compaction_busy: two copies", b"compaction_jobs",
           b"private output path has an active deletion",
           b"compaction_behind: publication changed")


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    if any((HERE / name).exists() for name in ("builds.json", "binary-ownership.json", "measurements")):
        raise ValueError("Use a fresh benchmark directory; preserve the existing build and measurement receipts")
    root = Path(tempfile.mkdtemp(prefix="cb-bin.", dir=os.environ["TMPDIR"])).resolve()
    owner = {"version": 1, "identity": uuid.uuid4().hex, "path": str(root),
             "uid": os.getuid(), "primary_data": False,
             "purpose": "owned compaction benchmark executables"}
    save(root / "owner.json", owner)
    save(HERE / "binary-ownership.json", owner)
    env = dict(os.environ, CARGO_TARGET_DIR=str(TARGET))
    metadata = {"profile": "dev-release", "binary_directory": str(root),
                "cache_control": "clean laststore, fold_db, and lastdb_node before each build",
                "command": "cargo build -p lastdb_node --bin lastdbd --profile dev-release",
                "rustc": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                "cargo": subprocess.check_output(["cargo", "--version"], text=True).strip(),
                "profile_options": {"inherits": "release", "opt-level": 3,
                    "lto": "off", "codegen-units": 16, "debug": False, "panic": "abort"}}
    for label, (source, revision) in SOURCES.items():
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source,
                                         text=True).strip()
        if actual != revision:
            raise RuntimeError("Source revision differs")
        subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", "*.rs", "*Cargo.toml",
                        "Cargo.lock"], cwd=source, check=True)
        with (HERE / f"{label}-clean.log").open("w") as log:
            subprocess.run(["cargo", "clean", "--profile", "dev-release", "-p", "laststore",
                "-p", "fold_db", "-p", "lastdb_node"], cwd=source, env=env,
                stdout=log, stderr=subprocess.STDOUT, check=True)
        print(json.dumps({"build_start": label, "source": revision}), flush=True)
        started = time.monotonic()
        with (HERE / f"{label}-build.log").open("w") as log:
            subprocess.run(["cargo", "build", "-p", "lastdb_node", "--bin", "lastdbd",
                "--profile", "dev-release"], cwd=source, env=env,
                stdout=log, stderr=subprocess.STDOUT, check=True)
        built = TARGET / "dev-release" / "lastdbd"
        binary = root / label
        shutil.copy2(built, binary)
        if sha(binary) != sha(built):
            raise RuntimeError("Copied executable differs")
        data = binary.read_bytes()
        markers = {marker.decode(): marker in data for marker in MARKERS}
        if any(value != (label == "after") for value in markers.values()):
            raise RuntimeError(f"Executable implementation differs: {markers}")
        text = (HERE / f"{label}-build.log").read_text()
        for package in ("laststore", "fold_db", "lastdb_node"):
            if f"Compiling {package} " not in text:
                raise RuntimeError(f"No fresh compiler receipt for {package}")
        metadata[label] = {"revision": revision, "sha256": sha(binary),
            "binary": str(binary), "source": str(source), "markers": markers,
            "build_wall_seconds": time.monotonic() - started,
            "compiler_receipt_sha256": sha(HERE / f"{label}-build.log"),
            "clean_receipt_sha256": sha(HERE / f"{label}-clean.log")}
        save(HERE / "builds.json", metadata)
        print(json.dumps({"build_complete": label, "sha256": metadata[label]["sha256"]}),
              flush=True)
    subprocess.run([sys.executable, str(HERE / "run_pair.py"), "--builds",
        str(HERE / "builds.json"), "--output", str(HERE / "measurements"),
        "--pair", "0", "--records", "100000"], cwd=WORKTREE, check=True)


if __name__ == "__main__":
    main()
