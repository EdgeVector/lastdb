#!/usr/bin/env python3
"""Measure identical binaries, then fresh builds, on one public CI runner."""

import argparse
from collections import Counter
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys


SOURCE = "688222b04d18d717da913007cb41e7174e48f395"
REPOSITORY = "EdgeVector/lastdb"
GIB = 1024 ** 3
PAIRS = 4


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def read(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 4 * 1024 ** 2:
        raise ValueError("Invalid control metadata file")
    return json.loads(path.read_text())


def git(directory, *arguments):
    return subprocess.check_output(["git", "-C", str(directory), *arguments],
                                   text=True, timeout=20).strip()


def host_guard():
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"
            or os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or sys.platform != "linux" or os.uname().machine != "x86_64"):
        raise ValueError("Controls require the canonical public Ubuntu hosted workflow")


def space(phase, require=False):
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    paths = (temporary, Path(os.environ["GITHUB_WORKSPACE"]).resolve(strict=True))
    free = min(shutil.disk_usage(path).free for path in paths)
    required = 24 if phase == "build" else 16
    result = {"phase": phase, "free_bytes": free, "required_bytes": required * GIB}
    print(json.dumps(result), flush=True)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(f"\nControl {phase} space: {free / GIB:.1f} GiB free; {required} GiB required.\n")
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as stream:
        stream.write(f"cleanup-required={str(free < required * GIB).lower()}\n")
    if require and free < required * GIB:
        raise RuntimeError("Insufficient space for the bounded control recipe")
    return result


def child(command, directory, seconds):
    process = subprocess.Popen(command, cwd=directory, start_new_session=True)
    try:
        code = process.wait(timeout=seconds)
        if code:
            raise RuntimeError(f"Control command failed with exit code {code}")
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=10)
        raise


def remove_owned(path, parent, owner):
    if not path.exists():
        return
    if (path.is_symlink() or path.parent.resolve() != parent or not path.is_dir()
            or path.stat().st_uid != os.getuid() or path.stat().st_ino != owner):
        raise ValueError("The owned control directory identity changed")
    shutil.rmtree(path)


def digest(path):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= 512 * 1024 ** 2:
        raise ValueError("Invalid control executable")
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def build_one(label, product, output, programs, temporary, context):
    target = temporary / "bench-target-candidate"
    if target.exists() or target.is_symlink():
        raise ValueError("The exact fresh-build target must not exist")
    destination = output / f"build-{label}"
    destination.mkdir()
    command = [sys.executable, str(product / "scripts/benchmarks/build.py"), "build",
               "--source", str(product), "--revision", SOURCE, "--label", "candidate",
               "--context", str(context), "--cache", str(programs / label),
               "--output", str(destination)]
    try:
        space("build", require=True)
        child(command, product, 15 * 60)
        metadata = read(destination / "build-candidate.json")
        binary = programs / label / SOURCE / "lastdbd"
        cli = binary.with_name("lastdb")
        if (metadata.get("mode") != "built from source" or metadata.get("revision") != SOURCE
                or metadata.get("context") != read(context)["fingerprint"]
                or metadata.get("sha256") != digest(binary)
                or metadata.get("cli_sha256") != digest(cli)):
            raise ValueError("A fresh build differs from its recorded identity")
        return {**metadata, "path": str(binary), "cli_path": str(cli),
                "source_path": str(product), "target_path": str(target)}
    finally:
        if target.exists() and not target.is_symlink():
            remove_owned(target, temporary, target.stat().st_ino)


def verify_stage(directory):
    manifest = read(directory / "benchmark.json")
    expected = {(pair, label) for pair in range(PAIRS) for label in ("baseline", "candidate")}
    runs = manifest.get("runs", [])
    if (manifest.get("complete") is not True or manifest.get("driver_revision") != SOURCE
            or manifest.get("recipe", {}).get("pairs") != PAIRS
            or len(runs) != len(expected) or {(run["pair"], run["label"]) for run in runs} != expected):
        raise ValueError("A control comparison lacks complete balanced pairs")
    writes = 0
    for run in runs:
        folder = directory / run["directory"]
        shutdown, cleanup = read(folder / "shutdown.json"), read(folder / "cleanup.json")
        if (run.get("complete") is not True or shutdown.get("exit_code") != 0
                or shutdown.get("forced") is not False
                or shutdown.get("receipt", {}).get("flush_ok") is not True
                or cleanup.get("daemon_stopped") is not True
                or Path(cleanup["removed"]).exists()):
            raise ValueError("A control run lacks successful shutdown and home cleanup")
        counts = Counter()
        with gzip.open(folder / "samples.jsonl.gz", "rt") as stream:
            for line in stream:
                sample = json.loads(line)
                if "error" in sample or sample.get("kind") not in {"read", "write"}:
                    raise ValueError("A control request failed")
                counts[sample["case"]] += 1
                if sample["kind"] == "write":
                    # The pinned worker retains this field only after it validates
                    # the returned count and the actual durable acknowledgement.
                    if "resident_stages_us" not in sample:
                        raise ValueError("A write lacks its validated acknowledgement record")
                    writes += 1
        metrics = read(folder / "metrics.json")
        primary = {case: metric for case, metric in metrics.items() if "sequence" in metric}
        if (set(counts) != set(primary)
                or any(metric["errors"] or counts[case] != metric["requests"]
                       for case, metric in primary.items())):
            raise ValueError("The control request records do not cover the complete cases")
    return {"runs": len(runs), "validated_durable_write_records": writes, "cleanup_complete": True}


def measure_stage(name, left, right, product, output, builds):
    space("measure", require=True)
    destination = output / name
    command = [sys.executable, str(product / "scripts/benchmarks/run.py"),
               "--baseline", builds[left]["path"], "--candidate", builds[right]["path"],
               "--baseline-revision", SOURCE, "--candidate-revision", SOURCE,
               "--driver-revision", SOURCE, "--baseline-workers", "0", "--candidate-workers", "0",
               "--profile", "standard", "--pairs", str(PAIRS), "--output", str(destination)]
    child(command, product, 40 * 60)
    return {"baseline": left, "candidate": right, "directory": name,
            **verify_stage(destination)}


def interrupt(_signum, _frame):
    raise KeyboardInterrupt("The latency control received an interruption")


def run(args):
    controller = Path(__file__).resolve().parents[3]
    product = args.product.resolve(strict=True)
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    revision = os.environ["CONTROLLER_SHA"]
    if (not re.fullmatch(r"[0-9a-f]{40}", revision) or git(controller, "rev-parse", "HEAD") != revision
            or git(controller, "status", "--porcelain")
            or git(product, "rev-parse", "HEAD") != SOURCE or git(product, "status", "--porcelain")
            or os.environ.get("CARGO_BUILD_JOBS") != "4"
            or os.environ.get("CARGO_TERM_COLOR") != "always"
            or os.environ.get("RUSTC_WRAPPER") != ""
            or os.environ.get("RUSTC_WORKSPACE_WRAPPER") != ""):
        raise ValueError("The controller, source, or explicit build inputs differ")
    output = args.output.resolve()
    if output.parent != temporary or args.output.is_symlink():
        raise ValueError("Controls require a fresh direct child of RUNNER_TEMP")
    output.mkdir()
    programs = temporary / f"latency-control-programs-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    programs.mkdir()
    owner = programs.stat().st_ino
    manifest = {"format_version": 1, "source_revision": SOURCE, "driver_revision": SOURCE,
                "controller_revision": revision, "builds": {}, "stages": {}, "complete": False,
                "inputs": {"build_jobs": 4, "workers": 0, "compiled_cache": False,
                           "compiler_wrappers": "disabled",
                           "profile": "standard", "pairs": PAIRS},
                "run_url": f"https://github.com/{REPOSITORY}/actions/runs/{os.environ['GITHUB_RUN_ID']}"}
    try:
        save(output / "controls.json", manifest)
        with (output / "cpu.txt").open("w") as stream:
            subprocess.run(["lscpu"], check=True, stdout=stream, timeout=20)
        shutil.copy2("/proc/cpuinfo", output / "cpuinfo.txt")
        context = output / "build-context.json"
        child([sys.executable, str(product / "scripts/benchmarks/build.py"), "context",
               "--output", str(context)], product, 120)
        manifest["build_context"] = read(context)
        for label in ("A", "B"):
            manifest["builds"][label] = build_one(label, product, output, programs, temporary, context)
            save(output / "controls.json", manifest)
        for name, left, right in (("same-binary", "A", "A"), ("fresh-builds", "A", "B")):
            manifest["stages"][name] = measure_stage(name, left, right, product, output, manifest["builds"])
            save(output / "controls.json", manifest)
        manifest["complete"] = True
    except BaseException as failure:
        manifest["error"] = f"{type(failure).__name__}: {failure}"
        raise
    finally:
        remove_owned(programs, temporary, owner)
        manifest["programs_removed"] = not programs.exists()
        save(output / "controls.json", manifest)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--disk-phase", choices=("build", "measure"))
    parser.add_argument("--require-space", action="store_true")
    args = parser.parse_args()
    host_guard()
    if args.disk_phase:
        space(args.disk_phase, args.require_space)
    elif args.product is None or args.output is None:
        parser.error("The experiment requires --product and --output")
    else:
        signal.signal(signal.SIGTERM, interrupt)
        run(args)


if __name__ == "__main__":
    main()
