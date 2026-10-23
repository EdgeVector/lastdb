#!/usr/bin/env python3
"""Repeat saved-receipt analysis, then restore every original summary byte."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def evidence_only(value):
    copy = json.loads(json.dumps(value))
    for run in copy["runs"].values():
        run.pop("request_timing_evidence")
    return copy


def main():
    target = ROOT / "post-packaging-analysis.json"
    if target.exists():
        raise ValueError("Do not replace the repeated packaged-analysis receipt")
    old_json = (ROOT / "analysis-original-summary.json").read_bytes()
    old_text = (ROOT / "analysis-original-summary.txt").read_bytes()
    if (old_json != (ROOT / "summary.json").read_bytes()
            or old_text != (ROOT / "summary.txt").read_bytes()):
        raise ValueError("An original analysis summary changed before the repeated analysis")
    manifest = json.loads((ROOT / "review-runs.json").read_text())
    argv = [sys.executable, str(ROOT / "summarize.py"), "--before-revision", manifest["before_revision"],
            "--after-revision", manifest["after_revision"]]
    result = {"complete": False, "argv": argv}
    try:
        process = subprocess.run(argv, capture_output=True, text=True, timeout=120, check=False)
        result.update(exit_code=process.returncode, stdout=process.stdout, stderr=process.stderr)
        if process.returncode:
            raise RuntimeError("The packaged saved-receipt analysis failed")
        generated = (ROOT / "summary.json").read_bytes()
        text = (ROOT / "summary.txt").read_bytes()
        if evidence_only(json.loads(old_json)) != evidence_only(json.loads(generated)) or text != old_text:
            raise RuntimeError("A metric or text result changed after exact receipt packaging")
        path = ROOT / "analysis-packaged-summary.json"
        path.write_bytes(generated)
        result.update(complete=True, all_metrics_identical=True, text_identical=True,
                      permitted_metadata_change="Only each run's raw-versus-compact request evidence source",
                      original_summary_sha256=hashlib.sha256(old_json).hexdigest(),
                      packaged_summary_sha256=digest(path), driver_sha256=digest(Path(__file__)))
    finally:
        (ROOT / "summary.json").write_bytes(old_json)
        (ROOT / "summary.txt").write_bytes(old_text)
        expected = json.loads((ROOT / "pre-packaging-summaries.json").read_text())["files"]
        result["all_fourteen_original_summaries_unchanged"] = all(
            (ROOT / name).stat().st_size == item["bytes"] and digest(ROOT / name) == item["sha256"]
            for name, item in expected.items())
        if result["all_fourteen_original_summaries_unchanged"] is not True:
            result["complete"] = False
        target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    if result.get("complete") is not True:
        raise RuntimeError("The repeated packaged analysis did not preserve all original summaries")
    print(json.dumps({"all_metrics_identical": True, "all_original_summaries_unchanged": True}))


if __name__ == "__main__":
    main()
