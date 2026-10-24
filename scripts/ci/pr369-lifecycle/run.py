#!/usr/bin/env python3
"""Execute the authorized generated-data drill only on a GitHub CI runner."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

MODES = (
    "delete-age", "cutoff", "replay-age", "losing-age", "concurrency", "retry",
    "conflicts-seed", "conflicts-drain", "files-seed", "files-finish", "files-dwell",
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--program", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--variant", choices=("baseline", "fixed"), required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise SystemExit("This one-time drill runs only in GitHub Actions.")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    root = Path(tempfile.mkdtemp(prefix="pr369-generated.", dir=runner)).resolve()
    args.evidence.mkdir(parents=True, exist_ok=True)
    (args.evidence / "owned-root.txt").write_text(str(root) + "\n")
    records = []
    with (args.evidence / "results.jsonl").open("x") as output:
        with (args.evidence / "runtime.log").open("x") as errors:
            for mode in MODES:
                result = subprocess.run(
                    [str(args.program.resolve()), mode, str(root), args.variant],
                    stdout=subprocess.PIPE, stderr=errors, text=True, timeout=180,
                )
                output.write(result.stdout)
                output.flush()
                if result.returncode:
                    raise SystemExit(f"{mode} failed with exit {result.returncode}")
                record = json.loads(result.stdout)
                if record.get("passed") is not True:
                    raise SystemExit(f"{mode} did not report success")
                records.append(record)
    faults = [record for record in records if record.get("baseline_fault_reproduced")]
    if len(faults) != (4 if args.variant == "baseline" else 0):
        raise SystemExit("The exact baseline/candidate controls did not match.")
    source = (args.evidence / "product-source.txt").read_text().strip()
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write(f"## PR 369 lifecycle drill — {args.variant}\n\n")
        summary.write(f"Product source: {source}\n\n")
        summary.write(f"Drill source: {os.environ['DRILL_SOURCE']}\n\n")
        summary.write("The CI patch adds a module and entry point. Product functions remain unchanged.\n\n")
        summary.write("| Case | Result | Correct TTL behavior |\n|---|---|---|\n")
        for record in records:
            summary.write(f"| {record['case']} | passed | {record.get('correct_behavior', 'control')} |\n")
        summary.write("\nFour baseline fault observations are expected. The fixed source must show all four correct results.\n")
        summary.write("Every invocation uses native storage and generated data. No primary, archive, account credential, cloud service, or cargo test is used.\n")


if __name__ == "__main__":
    main()
