#!/usr/bin/env python3
"""Describe every run; slowdowns are advisory and invalid measurements stay errors."""

import argparse
import json
import os
from pathlib import Path
import statistics
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent / "range_continuation"))
from node import file_hash, save
from observations import observations
from conditions import paired_conditions_match
from condition_report import condition_summary
from narrow_report import narrow_diagnostics, narrow_samples_complete


def inventory(root):
    pending, files, size = [(root, 0)], [], 0
    while pending:
        parent, depth = pending.pop()
        if depth > 3:
            raise RuntimeError("The artifact inventory exceeds three directory levels")
        for path in parent.iterdir():
            if path.is_symlink():
                raise RuntimeError("The artifact inventory contains a link")
            if path.is_dir():
                pending.append((path, depth + 1))
            elif path.is_file():
                size += path.stat().st_size
                if len(files) >= 250 or size > 8 * 1024 ** 3:
                    raise RuntimeError("The artifact inventory exceeds250 files or eightGiB")
                files.append({"path": str(path.relative_to(root)), "bytes": path.stat().st_size,
                              "sha256": file_hash(path)})
    return files


def paired_inputs_match(grouped):
    if any(len(values) != 2 for values in grouped.values()):
        return False
    by_pair = {role: {value["pair"]: value for value in values}
               for role, values in grouped.items()}
    if any(set(values) != {0, 1} for values in by_pair.values()):
        return False
    for pair in (0, 1):
        first, last = (by_pair[role][pair] for role in ("baseline", "candidate"))
        if (first["ordered_mutation_sha256"] != last["ordered_mutation_sha256"]
                or first["final_values_sha256"] != last["final_values_sha256"]
                or len(first["narrow_samples"]) != 64 or len(last["narrow_samples"]) != 64
                or [row["request_sha256"] for row in first["narrow_samples"]]
                != [row["request_sha256"] for row in last["narrow_samples"]]
                or not all(narrow_samples_complete(value) for value in (first, last))
                or not paired_conditions_match(first, last)):
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--job-status", required=True)
    args = parser.parse_args()
    manifest_path = args.output / "range-run.json"
    lines = ["## Bounded received-data ranges", "", "Job status: " + args.job_status + ".", ""]
    if not manifest_path.exists():
        lines.append("No measurement manifest exists. A prior build or setup step failed; no latency result is available.")
    else:
        manifest = json.loads(manifest_path.read_text())
        accepted = manifest.get("complete") is True and args.job_status == "success"
        if not accepted:
            lines.append("The measurement manifest or job is incomplete. Combined latency conclusions remain unavailable.")
        lines.extend(["Exact builds: " + "; ".join(role + " `" + row["revision"] + "`" for role, row in manifest["binaries"].items()),
            "", "Each exact source supplies its own storage-format-compatible seed helper. Generated mutation inputs and final values must match. Seed time is excluded."])
        helpers = manifest.get("seed_helpers", {"candidate": manifest.get("seed_helper", {})})
        lines.extend(role + " seed helper SHA256: `" + helper.get("sha256", "unavailable") + "`."
                     for role, helper in helpers.items())
        formats = {}
        for entry in manifest["runs"]:
            formats.setdefault(entry["role"], set()).add(str(entry.get("home_format") or "unreported"))
        lines.append("Home format: " + "; ".join(role + " " + ", ".join(sorted(values))
                     for role, values in sorted(formats.items())) + " (7 stores records; 4 to 6 store atoms).")
        for mode in ("typed",):
            grouped = {role: [] for role in ("baseline", "candidate")}
            for entry in manifest["runs"]:
                if entry["mode"] != mode:
                    continue
                value = json.loads((args.output / entry["directory"] / "result.json").read_text())
                if value["complete"]:
                    grouped[entry["role"]].append({**value, "pair": entry["pair"]})
                else:
                    lines.extend(["", "Incomplete operation: " + entry["directory"] + "; " + str(value["error"])])
            if not accepted or not paired_inputs_match(grouped):
                lines.extend(["", mode + ": no two accepted, identical-input pairs; no combined performance conclusion."])
                continue
            medians = {role: {name: statistics.median(row["narrow_latency"][name] for row in values)
                              for name in ("p50_ms", "p95_ms", "p99_ms")} for role, values in grouped.items()}
            change = (medians["candidate"]["p95_ms"] / medians["baseline"]["p95_ms"] - 1) * 100
            text = lambda role: " / ".join(f"{medians[role][name]:.3f}" for name in ("p50_ms", "p95_ms", "p99_ms"))
            lines.extend(["", "| Mode | Narrow requests/run | Base p50 / p95 / p99 ms | Candidate p50 / p95 / p99 ms | p95 change |",
                "|---|---:|---|---|---:|",
                f"| {mode} | 64 | {text('baseline')} | {text('candidate')} | {change:+.1f}% |"])
            lines.extend(observations(grouped, mode))
            lines.extend(narrow_diagnostics(grouped, mode))
            lines.extend(condition_summary(grouped, mode))
            if change > 20:
                print("::notice::" + mode + " narrow-range p95 increased more than20%; advisory only")
            limitations = [case["http_status"] for value in grouped["baseline"] for case in value["broad"]
                           if case.get("outcome") == "expected_baseline_limitation"]
            lines.extend(["", mode + ": baseline broad-range/prefix refusal statuses " + str(limitations) + ". These are unavailable operations, excluded from latency."])
            candidate = grouped["candidate"]
            lines.extend(["", mode + ": candidate broad ranges complete for both received sources, their overlap, and local duplicates.",
                "Reopen, source-specific Delete/Purge, current local values, API boundaries, and exclusive cursors return the complete expected values.",
                "Range-field-only and omitted-range-field projections preserve exact fields, including the existing primary hash field, membership, source values, and continuation.",
                "An independent schema supplies 640 prefix keys and 576 tail keys. The target schema keeps its own values and membership.",
                *(["The format 7 candidate seed helper shares that schema and checks that each personal write copies its records into the share scope."]
                  if all(value.get("home_format") == 7 for value in candidate) else []),
                "Actual preload selection verified per run: " + str([value["preload"]["actual_ready_selection_verified"] for value in candidate]) + "."])
        lines.extend(["", "Latency cells are medians of two per-run statistics; independent requests are not pooled.",
            "Raw requests, complete responses, storage read receipts, errors, and individual samples accompany this artifact.",
            "The narrow requests compare identical selectors after a complete known-key prefill. OS cache and runner variation remain uncontrolled.",
            "Memory uses Linux VmRSS/VmHWM samples, including earlier work in each process. No total-memory cap or power-loss result is claimed.",
            "Candidate preload coverage requires actual ready-domain and memory-selection counters; otherwise the result explicitly omits that claim.",
            "No cloud transport, cross-device sync, or broad-baseline latency improvement is established.",
            "The public mutation path rejects reserved body tombstone values. This recipe does not manufacture legacy tombstone bytes; body-drop accounting remains source review only."])
    text = "\n".join(lines) + "\n"
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(text)
    print(text, end="")
    save(args.output / "artifact-files.json", {"files": inventory(args.output),
         "scope": "Exact file hashes before upload; full raw bodies stay outside Git in this14-day artifact"})


if __name__ == "__main__":
    main()
