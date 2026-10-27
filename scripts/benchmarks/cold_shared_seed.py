"""CI control: both labels execute one byte-identical candidate binary."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

from node import file_hash, save
from report import compare, render


def main():
    parser = argparse.ArgumentParser()
    for label in ("baseline", "candidate"):
        parser.add_argument(f"--{label}-binary", type=Path, required=True)
        parser.add_argument(f"--{label}-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("This product control runs in CI only")
    if args.baseline_revision != args.candidate_revision:
        raise RuntimeError("This control requires the same exact revision for both labels")
    binary = args.candidate_binary.resolve(strict=True)
    driver = Path(__file__).with_name("run.py")
    command = [sys.executable, str(driver), "--profile", "standard", "--pairs", "4",
               "--baseline", str(binary), "--candidate", str(binary),
               "--baseline-revision", args.candidate_revision,
               "--candidate-revision", args.candidate_revision,
               "--driver-revision", os.environ["DRIVER_SHA"], "--output", str(args.output)]
    subprocess.run(command, check=True)
    manifest = json.loads((args.output / "benchmark.json").read_text())
    if not manifest["complete"] or len(manifest["runs"]) != 8:
        raise RuntimeError("The same-binary control did not complete all four pairs")
    baseline, candidate = manifest["binaries"]["baseline"], manifest["binaries"]["candidate"]
    if (baseline != candidate or baseline["sha256"] != file_hash(binary)
            or baseline["revision"] != args.candidate_revision):
        raise RuntimeError("The same-binary control used different programs")
    comparison = compare(args.output, manifest)
    save(args.output / "comparison.json", comparison)
    summary = ("# Same-binary control for aggregate source refactor\n\n"
               "Both labels use one exact candidate executable, with the same SHA-256 and path.\n"
               "Four alternating pairs use fresh generated 10,000-record homes and default socket workers.\n"
               "All opening samples remain. A label difference here cannot establish a source-code effect.\n"
               "This control measures run variation; it does not replace the base/candidate comparison.\n\n"
               + render(args.output, manifest, comparison))
    (args.output / "summary.md").write_text(summary)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
        stream.write(summary)


if __name__ == "__main__":
    main()
