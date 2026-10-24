#!/usr/bin/env python3
"""Summarize actual lifecycle operations, with raw output in the CI artifact."""
import argparse
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = args.output / "lifecycle-run.json"
    lines = ["## Lifecycle operations", "",
             "Candidate-only operational measurements. No latency baseline or test suite.", "",
             "Source: `" + os.environ["CANDIDATE_SHA"] + "`", ""]
    if not manifest.exists():
        lines.append("No complete operation manifest. Inspect the build output.")
    else:
        value = json.loads(manifest.read_text())
        lines.append("Complete: **" + str(value["complete"]) + "**")
        for run in value["runs"]:
            lines.extend(["", "### " + run["mode"], "", "Complete: " + str(run["complete"])])
            result = json.loads((args.output / run["mode"] / "result.json").read_text())
            if result["error"]:
                lines.append("Failure: " + result["error"])
            for name, operation in result["operations"].items():
                if not isinstance(operation, dict):
                    continue
                elapsed = operation.get("elapsed_ms")
                if elapsed is not None:
                    lines.append("- " + name + ": " + str(round(elapsed, 3)) + " ms")
            lines.append("Raw request/reply records, owner commands and shutdown receipts are in the artifact.")
    lines.extend(["", "Scope: generated personal homes, plain and encrypted key modes, real writes, TTL,",
                  "bounded conflict drains, approved one-time damaged-counter repair drill, explicit delete cleanup,",
                  "old replay refusal, shared atoms, real minimum file-blob dwell, compaction and reopened known-key reads.",
                  "The repair drill changes only generated data. No power-loss, cloud transport, or physical erase claim."])
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()

