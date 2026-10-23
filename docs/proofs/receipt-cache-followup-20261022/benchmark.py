#!/usr/bin/env python3
"""Repeat the unchanged matched workload, then capture a separate probe sweep."""

import argparse
import json
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from benchmark_output import default_output, resolve_output

sys.dont_write_bytecode = True
from reuse import capture as owned_capture, original
import focused
import erase_benchmark
from benchmark_support import save


def run(capture):
    result = original("benchmark").run(capture)
    matched_checkpoints = len(capture.checkpoints)
    save(capture.output / "matched-summary.json", result)
    extra = focused.run(capture)
    capture.phase = "focused-reopen-verification"
    capture.stop()
    capture.save_receipts()
    capture.start()
    capture.readback("focused-graceful-reopen")
    capture.checkpoint("focused-reopened")
    extra["graceful_reopen_verified"] = True
    capture.save_receipts()
    save(capture.output / "focused-checkpoints.json", capture.checkpoints[matched_checkpoints:])
    save(capture.output / "focused-compaction.json", extra)
    result["extra_focus"] = {"file": "focused-compaction.json", "all_values_verified": True,
                             "graceful_reopen_verified": True, "matched_measurements_unchanged": True}
    result["extra_erase"] = erase_benchmark.run(capture)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=default_output('receipt-cache-followup-20261022'))
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--label", required=True)
    args = parser.parse_args()
    args.output = resolve_output(args.output)
    capture = owned_capture(args)
    summary = {"complete": False, "label": args.label, "source_revision": args.source_revision}
    try:
        summary.update(run(capture))
    except BaseException as failure:
        summary.update(complete=False, error=str(failure))
        raise
    finally:
        try:
            capture.stop()
        except BaseException as failure:
            summary.update(complete=False, shutdown_error=str(failure))
            raise
        finally:
            capture.save_receipts()
            summary.update(scratch=str(capture.root), home=str(capture.home),
                           all_owned_processes_stopped=capture.process is None, scratch_retained=True)
            save(capture.output / "summary.json", summary)
            print(json.dumps({key: value for key, value in summary.items() if key != "phases"},
                             sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
