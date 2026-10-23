#!/usr/bin/env python3
"""Capture compatible journal operations on a GitHub runner with generated data."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("This capture runs only in GitHub Actions")
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-bin", type=Path, required=True)
    parser.add_argument("--candidate-bin", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    args.report.mkdir(parents=True, exist_ok=True)
    os.environ["LASTDB_GROWTH_REPORT_DIR"] = str(args.report.resolve())
    spec = importlib.util.spec_from_file_location("growth_operations", Path(__file__).with_name("operations.py"))
    ops = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ops)
    with tempfile.TemporaryDirectory(prefix="lgc.", dir=os.environ["RUNNER_TEMP"]) as temporary:
        root = Path(temporary).resolve(strict=True)
        ops.checks.save(root / "owner.json", {"owner_uid": os.getuid(), "scratch": str(root),
            "primary_data": False, "primary_copy": False})
        for name, target in (("lastdbd", "baseline-daemon"), ("lastdb", "baseline-cli"),
                             ("lastdb-disk-home", "baseline-disk-home")):
            shutil.copy2(args.baseline_bin / name, root / target)
        context = argparse.Namespace(scratch=root, baseline=root / "baseline-daemon",
            candidate=args.candidate_bin / "lastdbd", output=args.report,
            source_commit=args.source, source=args.source,
            maintain=args.candidate_bin / "lastdb-pack-maintain")
        try:
            ops.checks.seed(context)
            ops.grow(context)
            ops.scale_input(context)
            home, seed = ops.build_scale(context)
            seed["home"] = str(home)
            ops.checks.save(args.report / "scale-seed.json", seed)
            ops.scale(context)
            growth = json.loads((args.report / "growth.json").read_text())
            scale = json.loads((args.report / "scale.json").read_text())
            ops.checks.save(args.report / "RESULTS.json", {"passed": True,
                "source": args.source, "primary_data": False, "primary_copy": False,
                "timings_advisory": True,
                "baseline_bytes": growth["variants"]["baseline"]["after"]["bytes"],
                "candidate_bytes": growth["variants"]["candidate"]["after"]["bytes"],
                "large_records": scale["repack"]["output"]["result"]["records_copied"],
                "cleanup_resumes_after_restart": scale["automatic_full_compaction"]["cleanup_resumed_after_restart"],
                "candidate_sha256": ops.checks.digest(args.candidate_bin / "lastdbd")})
        finally:
            # Each operation stops its owned node before this exact scratch removal.
            for label in ("baseline", "candidate", "baseline-growth", "candidate-growth",
                          "old-after-growth", "scale-node", "scale-final", "rollback"):
                log = root / (label + ".log")
                if log.exists():
                    shutil.copy2(log, args.report / log.name)
    (args.report / "scratch-cleanup.json").write_text(json.dumps({"removed": True,
        "primary_data": False, "primary_copy": False}) + "\n")


if __name__ == "__main__":
    main()
