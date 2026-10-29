#!/usr/bin/env python3
"""CI-only canonical schema lifecycle checks through a real fresh-home daemon.

Use a lastdbd binary built in CI with its build version set to bench-<revision>.
All homes and raw output stay in RUNNER_TEMP. No database or identity is copied.
This checks node catalog consumption; the seed-system route bypasses cloud
catalog admission and app credential checks deliberately and explicitly.
"""

import argparse
import json
import os
from pathlib import Path
import sys
import time

from node_cases import (ambiguous_backfill, concurrent_members, late_member, linked_lifecycle, schema_evolution, separation,
                        unsupported_layouts, unsupported_marker)
from node_ops import ENVIRONMENT, Node, file_hash, require, save

LIMITS = [
    "Isolated system catalog setup does not prove schema-service admission or app authentication.",
    "Declaration handles are generated catalog inputs; no production registry receives a write.",
    "SIGKILL follows a durable acknowledgement; this does not enumerate every storage crash cut.",
    "Unsupported marker refusal does not prove conversion from any real historical disk format.",
    "Public mutation requests allocate new write identities; authenticated peer replay is outside this check.",
    "Historical backfill requires an explicit source-key batch; automatic schema-load backfill is unsupported.",
    "Explicit migration copies current visible tips; it does not replay historical source Delete barriers.",
]
CASES = [
    ("linked-lifecycle", linked_lifecycle),
    ("declaration-separation", lambda node: separation(node, declared=True)),
    ("same-name-distinct-meaning", lambda node: separation(node, declared=False)),
    ("concurrent-member-writes", concurrent_members),
    ("late-member-historical-read", late_member),
    ("ambiguous-target-backfill-refusal", ambiguous_backfill),
    ("existing-data-schema-expansion", schema_evolution),
    ("unsupported-layout-refusal", unsupported_layouts),
    ("unsupported-marker-data-preservation", unsupported_marker),
]


def run_case(args, runner, name, operation):
    output = args.output / name
    output.mkdir(mode=0o700)
    node = Node(args.candidate, args.candidate_revision, output, environment=ENVIRONMENT,
                start_seconds=120, temporary_directory=runner)
    result = {"name": name, "status": "failed", "failure": None}
    started = time.monotonic()
    try:
        require(node.home.resolve().is_relative_to(runner), "generated home escaped RUNNER_TEMP")
        node.start()
        require(node.disk_format() == 2, "fresh daemon did not create the current disk format")
        result["observed"] = operation(node)
        result["status"] = "passed"
    except Exception as error:
        result["failure"] = f"{type(error).__name__}: {error}"
    finally:
        try:
            node.close()
        except Exception as error:
            result["cleanup_failure"] = f"{type(error).__name__}: {error}"
            result["status"] = "failed"
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        save(output / "result.json", result)
    print(f"{name}: {result['status']}" + (f" ({result['failure']})" if result["failure"] else ""), flush=True)
    return result


def write_summary(args, results):
    passed = sum(result["status"] == "passed" for result in results)
    lines = ["## Real-node canonical schema checks", "",
             f"Source: `{args.candidate_revision}`. Result: {passed}/{len(results)} cases passed.", "",
             "| Case | Result |", "|---|---|"]
    for result in results:
        lines.append(f"| {result['name']} | {result['status']} |")
        if result["failure"]:
            lines.extend(["", f"Failure in `{result['name']}`: {result['failure']}", ""])
    lines.extend(["", "Limits:", ""] + ["- " + limit for limit in LIMITS])
    text = "\n".join(lines) + "\n"
    (args.output / "node-summary.md").write_text(text)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(path).open("a") as stream:
            stream.write(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "this check runs only on a GitHub-hosted CI runner")
    runner = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    require(args.output.is_absolute() and not args.output.exists(), "fresh absolute output path required")
    require(args.output.parent.resolve(strict=True).is_relative_to(runner), "output must stay in RUNNER_TEMP")
    args.output.mkdir(mode=0o700)
    results = [run_case(args, runner, name, operation) for name, operation in CASES]
    save(args.output / "node-result.json", {
        "source": args.candidate_revision, "binary_sha256": file_hash(args.candidate),
        "drivers": {name: file_hash(Path(__file__).parent / name)
                    for name in ("node_check.py", "node_cases.py", "node_ops.py")},
        "limits": LIMITS, "cases": results,
    })
    write_summary(args, results)
    return 0 if all(result["status"] == "passed" for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
