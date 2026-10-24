#!/usr/bin/env python3
"""Run scoped generated range operations on two exact GitHub Actions builds."""

import argparse
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time

sys.path.append(str(Path(__file__).resolve().parent.parent / "range_continuation"))
from node import Node, file_hash, save
from workload import Workload
from conditions import paired_conditions_match


def interrupted(_signum, _frame):
    raise KeyboardInterrupt("The generated range operation received an interruption")


def measure(binary, revision, mode, role, output, helper):
    output.mkdir()
    node, error, value = None, None, {}
    try:
        node = Node(binary, revision, mode, output)
        node.start()
        value = Workload(node, role == "baseline", helper).run()
    except BaseException as failure:
        error = type(failure).__name__ + ": " + str(failure)
    finally:
        if node is not None:
            try:
                node.close()
            except BaseException as failure:
                error = (error or "") + "; shutdown/cleanup: " + type(failure).__name__ + ": " + str(failure)
    raw = output / "requests.jsonl.gz"
    value.update(complete=error is None, error=error, revision=revision, role=role, mode=mode,
                 raw_sha256=file_hash(raw) if raw.exists() else None,
                 raw_bytes=raw.stat().st_size if raw.exists() else None)
    save(output / "result.json", value)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for role in ("baseline", "candidate"):
        parser.add_argument("--" + role, type=Path, required=True)
        parser.add_argument("--" + role + "-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed-helper", type=Path, required=True)
    parser.add_argument("--baseline-seed-helper", type=Path, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        parser.error("Product operations run only on GitHub Actions; no local daemon or build")
    args.output = args.output.resolve()
    workspace = Path(os.environ["GITHUB_WORKSPACE"]).resolve()
    temporary = Path(os.environ["RUNNER_TEMP"]).resolve()
    if args.output.is_relative_to(workspace) or not args.output.is_relative_to(temporary):
        parser.error("All generated output must stay outside Git under RUNNER_TEMP")
    args.output.mkdir(exist_ok=True)
    helpers = {"candidate": args.seed_helper.resolve(strict=True),
               "baseline": args.baseline_seed_helper.resolve(strict=True)}
    manifest_path = args.output / "range-run.json"
    if manifest_path.exists():
        parser.error("Preserve the original range operation manifest")
    signal.signal(signal.SIGTERM, interrupted)
    binaries = {}
    for role in ("baseline", "candidate"):
        revision = getattr(args, role + "_revision")
        if not re.fullmatch("[0-9a-f]{40}", revision):
            parser.error("Each exact source revision needs forty hexadecimal characters")
        binary = getattr(args, role).resolve(strict=True)
        version = subprocess.check_output([str(binary), "--version"], text=True, timeout=10).strip()
        if "range-" + revision not in version:
            parser.error("The executable stamp differs from its exact selected source")
        binaries[role] = {"path": str(binary), "revision": revision,
                          "sha256": file_hash(binary), "version": version}
    manifest = {"complete": False, "binaries": binaries, "runs": [],
        "host": {"platform": platform.platform(), "python": platform.python_version(),
                 "cpu_count": os.cpu_count(), "runner_image": os.environ.get("ImageVersion")},
        "seed_helpers": {role: {"path": str(helper), "sha256": file_hash(helper),
            "revision": binaries[role]["revision"]} for role, helper in helpers.items()},
        "drivers": {str(path.relative_to(Path(__file__).parent.parent)): file_hash(path)
            for folder in (Path(__file__).parent, Path(__file__).parent.parent / "range_continuation")
            for path in sorted(folder.glob("*.py"))},
        "recipe": {"records_per_received_source": 16384, "received_sources": 2,
            "received_overlap": 8192, "fields": 4, "body_bytes": 256, "seed_batch": 128,
            "keyed_batch": 256, "broad_page": 256, "narrow_selector_records": 128,
            "narrow_page": 64, "narrow_requests": 64, "pairs_per_mode": 2,
            "conditions": {"hash": "condition-reuse", "maximum_keys": 128,
                "declared_extra_fields": ["score", "optional", "never"], "cases": 8,
                "warm_requests_per_case": 32, "fresh_process_requests_per_case": 1,
                "fresh_restarts_per_run": 8, "memory": "Per-cell process before/after; byte-buffer receipts exclude captured field maps"},
            "modes": ["plain", "encrypted"], "automatic_maintenance": False,
            "warm_budget": "Product default; identical complete keyed prefill before timing",
            "limits": "Each owned run1200s,10000request rows,1GiB raw/512MiB gzip,4MiB response,50000home entries/4GiB",
            "timing": "HTTP round trip through complete response bytes; decode/validation/gzip excluded",
            "scope": "Baseline broad503 is unavailable, never a fast result. Matched narrow ranges alone compare latency. OS cache uncontrolled; Python client overhead applies."}}
    save(manifest_path, manifest)
    for mode in ("plain", "encrypted"):
        for pair in range(2):
            order = ("baseline", "candidate") if pair == 0 else ("candidate", "baseline")
            values = {}
            for role in order:
                name = mode + "-" + str(pair) + "-" + role
                print(json.dumps({"mode": mode, "pair": pair, "role": role}), flush=True)
                result = measure(Path(binaries[role]["path"]), binaries[role]["revision"], mode, role,
                                 args.output / name, helpers[role])
                manifest["runs"].append({"directory": name, "mode": mode, "pair": pair,
                    "role": role, "complete": result["complete"], "error": result["error"]})
                save(manifest_path, manifest)
                print(json.dumps(manifest["runs"][-1]), flush=True)
                if not result["complete"]:
                    return 1
                values[role] = result
            first, last = values["baseline"], values["candidate"]
            if (first["ordered_mutation_sha256"] != last["ordered_mutation_sha256"]
                    or first["final_values_sha256"] != last["final_values_sha256"]
                    or len(first["narrow_samples"]) != 64 or len(last["narrow_samples"]) != 64
                    or [row["request_sha256"] for row in first["narrow_samples"]]
                    != [row["request_sha256"] for row in last["narrow_samples"]]
                    or not paired_conditions_match(first, last)):
                raise RuntimeError("The paired mutation inputs, final values, or narrow requests differ")
    manifest["complete"] = True
    save(manifest_path, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
