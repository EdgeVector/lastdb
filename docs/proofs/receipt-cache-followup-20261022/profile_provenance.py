#!/usr/bin/env python3
"""Add an explicit, retrospective helper inventory beside an unchanged profile."""

import argparse
import hashlib
import json
from pathlib import Path
import time

ROOT = Path(__file__).resolve().parent
PRIOR = ROOT.parent / "receipt-part-pruning-benchmark-20261022"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def load(path):
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("before", "draft", "after"), required=True)
    args = parser.parse_args()
    folder = ROOT / (args.variant + "-profile-1")
    target = folder / "profile-provenance.json"
    if target.exists():
        raise ValueError("Do not replace an existing profile provenance receipt")
    run_path = ROOT / (args.variant + "-profile-run.json")
    run = load(run_path)
    summary = load(folder / "summary.json")
    inputs = load(folder / "inputs.json")
    diagnostic = load(folder / "diagnostic-inputs.json")
    if run.get("complete") is not True or summary.get("complete") is not True:
        raise ValueError("A complete original diagnostic is required")
    recorded = dict(inputs["helpers_sha256"])
    recorded[str(ROOT / "profile_batches.py")] = diagnostic["driver_sha256"]
    launcher = ROOT / ("run_draft_profile.py" if args.variant == "draft" else "run_profile.py")
    if args.variant == "draft":
        recorded[str(launcher)] = run["driver_sha256"]
    else:
        recorded.update(run["helpers_sha256"])
    if any(digest(Path(path)) != checksum for path, checksum in recorded.items()):
        raise ValueError("A helper differs from a hash in the original diagnostic")
    additional = [PRIOR / "reuse.py", ROOT / "profile_sample.py"]
    unrecorded = {}
    for path in additional:
        if str(path) in recorded:
            continue
        if path.stat().st_mtime_ns >= run["start_unix_ns"]:
            raise ValueError("A retrospectively inventoried helper changed after diagnostic dispatch")
        unrecorded[str(path)] = digest(path)
    originals = [run_path, folder / "inputs.json", folder / "diagnostic-inputs.json", folder / "summary.json"]
    result = {"complete": True, "variant": args.variant, "captured_unix_ns": time.time_ns(),
              "originally_recorded_helpers_sha256": recorded,
              "retrospective_helpers_sha256": unrecorded,
              "retrospective_limit": "These extra helper hashes were captured after the diagnostic; their file modification times precede dispatch",
              "original_evidence_sha256": {str(path.relative_to(ROOT)): digest(path) for path in originals},
              "preserved_original_inputs_and_summary": True, "driver_sha256": digest(Path(__file__))}
    target.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"profile_provenance": args.variant, "helpers": len(recorded) + len(unrecorded)}))


if __name__ == "__main__":
    main()
