#!/usr/bin/env python3
"""Report fixed-source latency controls without assigning a regression cause."""

import argparse
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys


SOURCE = "688222b04d18d717da913007cb41e7174e48f395"
SHA = re.compile(r"[0-9a-f]{40}\Z")
HASH = re.compile(r"[0-9a-f]{64}\Z")
LABELS = ("baseline", "candidate")
STAGES = {"same-binary": ("A", "A"), "fresh-builds": ("A", "B")}
FOCUS = "concurrent-mixed/32"
PRIMARY = {f"{mode}/{batch}" for mode in ("cold-read", "hot-read") for batch in (1, 32)}
PRIMARY |= {f"{mode}/{batch}" for mode in ("insert", "update") for batch in (1, 128)}
PRIMARY |= {f"concurrent-{mode}/{clients}" for clients in (1, 4, 16, 32)
            for mode in ("read", "write", "mixed")}
CASES = PRIMARY | {f"concurrent-mixed/{clients}/{kind}" for clients in (1, 4, 16, 32)
                   for kind in ("read", "write")}


def read_json(path):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= 2 * 1024 ** 2:
        raise ValueError("A control metadata file has an invalid type or size")
    return json.loads(path.read_text())


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def number(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def historical_report(product):
    actual = subprocess.check_output(["git", "-C", str(product), "rev-parse", "HEAD"],
                                     text=True, timeout=15).strip()
    if actual != SOURCE:
        raise ValueError("The product checkout differs from the fixed historical source")
    subprocess.run(["git", "-C", str(product), "diff", "--quiet", "HEAD", "--", "scripts/benchmarks"],
                   check=True, timeout=15)
    driver = product / "scripts" / "benchmarks"
    sys.path.insert(0, str(driver))
    specification = importlib.util.spec_from_file_location("historical_latency_report", driver / "report.py")
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
    return module, driver


def check_controls(output, product):
    controls = read_json(output / "controls.json")
    if (controls.get("format_version") != 1 or controls.get("complete") is not True
            or controls.get("programs_removed") is not True
            or controls.get("source_revision") != SOURCE or controls.get("driver_revision") != SOURCE
            or not SHA.fullmatch(controls.get("controller_revision", ""))
            or set(controls.get("stages", {})) != set(STAGES)
            or set(controls.get("builds", {})) != {"A", "B"}):
        raise ValueError("The control identity or completion record is invalid")
    fingerprint = controls["build_context"]["fingerprint"]
    if not HASH.fullmatch(fingerprint):
        raise ValueError("The build context has no valid fingerprint")
    for build in controls["builds"].values():
        if (build.get("revision") != SOURCE or build.get("context") != fingerprint
                or build.get("mode") != "built from source"
                or not HASH.fullmatch(build.get("sha256", ""))
                or not HASH.fullmatch(build.get("cli_sha256", ""))
                or build.get("version") != f"lastdbd bench-{SOURCE}"
                or build.get("cli_version") != f"lastdb bench-{SOURCE}"
                or not isinstance(build.get("path"), str)
                or build.get("source_path") != str(product)
                or not isinstance(build.get("target_path"), str)
                or not Path(build["target_path"]).is_absolute()):
            raise ValueError("A fresh build lacks the fixed source, context, versions, or hashes")
    if controls["builds"]["A"]["path"] == controls["builds"]["B"]["path"]:
        raise ValueError("The two fresh builds share an executable path")
    if controls["builds"]["A"]["target_path"] != controls["builds"]["B"]["target_path"]:
        raise ValueError("The fresh builds used different target directory paths")
    return controls


def check_manifest(folder, stage, controls, driver):
    selected = controls["stages"][stage]
    expected = dict(zip(LABELS, STAGES[stage]))
    if (any(selected.get(name) != value for name, value in {**expected, "directory": stage}.items())
            or selected.get("runs") != 8 or selected.get("cleanup_complete") is not True
            or type(selected.get("validated_durable_write_records")) is not int
            or selected["validated_durable_write_records"] <= 0):
        raise ValueError("The control stage has unexpected roles or directory")
    manifest = read_json(folder / "benchmark.json")
    recipe = manifest["recipe"]
    expected_recipe = {"pairs": 4, "records": 10000, "samples": 256, "body_bytes": 1024,
                       "fields": 4, "single_write_samples": 256, "write_samples": 32,
                       "batch_update_samples": 128, "read_batches": [1, 32], "write_batches": [1, 128],
                       "clients": [1, 4, 16, 32], "uds_workers": {label: 0 for label in LABELS}}
    if (manifest.get("complete") is not True or manifest.get("profile") != "standard"
            or manifest.get("driver_revision") != SOURCE
            or any(recipe.get(name) != value for name, value in expected_recipe.items())
            or recipe.get("native_timing") is not False
            or set(manifest.get("binaries", {})) != set(LABELS)
            or not {"run.py", "node.py", "workload.py", "report.py", "build.py", "phases.py"}
                   <= set(manifest.get("drivers", {}))):
        raise ValueError("The historical standard recipe or driver identity differs")
    for name, recorded in manifest["drivers"].items():
        source = driver / name
        if name.startswith("/") or ".." in Path(name).parts or not HASH.fullmatch(recorded):
            raise ValueError("A historical driver path or hash is invalid")
        if hashlib.sha256(source.read_bytes()).hexdigest() != recorded:
            raise ValueError("A measured driver differs from the pinned source")
    for label, build_name in expected.items():
        binary, build = manifest["binaries"][label], controls["builds"][build_name]
        if (binary.get("revision") != SOURCE or binary.get("sha256") != build["sha256"]
                or binary.get("version") != build["version"] or binary.get("path") != build["path"]):
            raise ValueError("A measured executable differs from its selected build")
    order = [(pair, label) for pair in range(4)
             for label in (LABELS if pair % 2 == 0 else tuple(reversed(LABELS)))]
    if [(run["pair"], run["label"]) for run in manifest["runs"]] != order:
        raise ValueError("The four complete balanced pairs are absent or reordered")
    return manifest


def samples_from(path):
    if path.is_symlink() or not path.is_file() or not 0 < path.stat().st_size <= 8 * 1024 ** 2:
        raise ValueError("The sample archive has an invalid type or size")
    rows, size = [], 0
    with gzip.open(path, "rt") as source:
        while line := source.readline(65537):
            size += len(line.encode())
            if len(line) > 65536 or size > 32 * 1024 ** 2 or len(rows) >= 10000:
                raise ValueError("The sample archive exceeds the fixed standard recipe bounds")
            rows.append(json.loads(line))
    return rows


def lifecycle(folder, run):
    result = read_json(folder / "result.json")
    owner = read_json(folder / "ownership.json")
    cleanup = read_json(folder / "cleanup.json")
    shutdown = read_json(folder / "shutdown.json")
    native = read_json(folder / "native-status.json")
    receipt = shutdown.get("receipt") or {}
    if (run.get("complete") is not True or run.get("error") is not None
            or run.get("revision") != SOURCE or result.get("complete") is not True
            or result.get("error") is not None or result.get("revision") != SOURCE
            or owner.get("generated_data_only") is not True
            or not str(owner.get("root", "")).startswith("/tmp/ldb-bench-")
            or cleanup.get("removed") != owner["root"] or cleanup.get("daemon_stopped") is not True
            or Path(owner["root"]).exists() or shutdown.get("forced") is not False
            or shutdown.get("exit_code") != 0 or receipt.get("flush_ok") is not True
            or type(receipt.get("pid")) is not int or receipt["pid"] <= 0
            or native.get("native_disk", {}).get("writer_failed") is not False):
        raise ValueError("A control run lacks complete work, durable shutdown, or owned-home cleanup")


def inspect_run(folder, run, stage):
    lifecycle(folder, run)
    metrics, rows = read_json(folder / "metrics.json"), samples_from(folder / "samples.jsonl.gz")
    if set(metrics) != CASES or {row.get("case") for row in rows} != PRIMARY:
        raise ValueError("The full standard case set is incomplete")
    for case in PRIMARY:
        case_rows = [row for row in rows if row["case"] == case]
        expected = 32 if case == "insert/128" else 128 if case == "update/128" else 256
        if (len(case_rows) != expected or metrics[case].get("requests") != len(case_rows)
                or metrics[case].get("errors") != 0
                or any("error" in row or not number(row.get("elapsed_ms")) for row in case_rows)):
            raise ValueError("A timed case has errors or incomplete samples")
    writes = [row for row in rows if row["kind"] == "write"]
    if any(not number((row.get("resident_stages_us") or {}).get("ack")) for row in writes):
        raise ValueError("A write lacks its reported durable acknowledgement stage")
    mixed = [row for row in rows if row["case"] == FOCUS]
    cohort = {(row["request_id"], row["kind"], row["client"], row["client_request"],
               row["records"], row["request_sha256"]) for row in mixed}
    if (len(mixed) != 256 or len(cohort) != 256
            or any(row["records"] != 1 or not HASH.fullmatch(row["request_sha256"]) for row in mixed)):
        raise ValueError("The mixed case lacks 256 unique requests")
    for client in range(32):
        calls = sorted((row for row in mixed if row["client"] == client), key=lambda row: row["client_request"])
        expected = ["read" if (client + turn) % 2 == 0 else "write" for turn in range(8)]
        if [row["client_request"] for row in calls] != list(range(8)) or [row["kind"] for row in calls] != expected:
            raise ValueError("The 32 mixed clients lack eight alternating read/write turns")
    coverage = metrics[FOCUS]["phase_coverage"]
    if (coverage.get("complete") is not True or coverage.get("matched") != 256
            or coverage.get("with_phases") != 256
            or any(row.get("server_request_ops", {}).get("request_id") != row["request_id"]
                   or row["server_request_ops"].get("status") != 200 for row in mixed)):
        raise ValueError("The mixed case lacks exact complete server request records")
    workers = sorted({row["server_request_ops"].get("uds_workers") for row in mixed})
    if len(workers) != 1 or type(workers[0]) is not int or workers[0] <= 0:
        raise ValueError("The observed socket worker count is missing or inconsistent")
    focused = [row for row in mixed if row["kind"] == "write"]
    tails = sorted(focused, key=lambda row: row["elapsed_ms"], reverse=True)[:3]
    tails += sorted(focused, key=lambda row: row["resident_stages_us"]["ack"], reverse=True)[:2]
    unique = {row["request_id"]: exact_request(row) for row in tails}
    summary = {"stage": stage, "pair": run["pair"], "label": run["label"], "workers": workers[0],
               "mixed_p95_ms": metrics[FOCUS]["p95_ms"], "read_p95_ms": metrics[FOCUS + "/read"]["p95_ms"],
               "write_p95_ms": metrics[FOCUS + "/write"]["p95_ms"],
               "writes_over_500_ms": sum(row["elapsed_ms"] > 500 for row in focused),
               "acks_over_300_ms": sum(row["resident_stages_us"]["ack"] > 300000 for row in focused),
               "validated_write_request_samples": len(writes),
               "phase_matches": 256, "requests": 256, "writes": len(focused), "cleanup_complete": True,
               "exact_slow_requests": list(unique.values())}
    return summary, cohort


def exact_request(row):
    operation = row["server_request_ops"]
    return {"request_id": row["request_id"], "client": row["client"], "turn": row["client_request"],
            "rpc_ms": row["elapsed_ms"], "server_ms": operation.get("duration_ms"),
            "server_completed_unix_ms": operation.get("ts_ms"),
            "ack_us": row["resident_stages_us"].get("ack"),
            "prepare_us": row["resident_stages_us"].get("prepare"),
            "apply_residual_us": operation.get("phases", {}).get("apply_us"),
            "socket_queue_us": operation.get("phases", {}).get("queue_wait_us"),
            "reported_phases_us": operation.get("phases"),
            "reported_resident_stages_us": row.get("resident_stages_us")}


def summary_lines(controls, summaries):
    builds = controls["builds"]
    lines = ["# Same-binary and fresh-build latency controls", "", f"Product and historical driver: `{SOURCE}`.",
             f"Controller: `{controls['controller_revision']}`.", "",
             "Both stages use four balanced pairs on one standard hosted runner and fresh generated homes.",
             "The complete standard recipe has 20 timed cases and eight derived mixed submetrics.",
             "Each mixed/32 case has 256 calls: 128 durable writes and 128 keyed reads, across 32 clients.",
             "Each client performs eight alternating calls. All 16 runs have identical request IDs and body hashes.",
             "The same-binary stage uses the same executable path and hash for both roles.",
             "Each fresh build starts with an empty target directory and the same source, toolchain, profile, and version.", "",
             f"Build A daemon SHA256: `{builds['A']['sha256']}`.",
             f"Build B daemon SHA256: `{builds['B']['sha256']}`.",
             f"Fresh daemon hashes identical: **{builds['A']['sha256'] == builds['B']['sha256']}**.",
             "Identical fresh hashes are a valid result. Different hashes alone establish no performance cause.", "",
             "| Stage | Pair | Role | Mixed p95 ms | Read p95 ms | Write p95 ms | Writes >500 ms | Acks >300 ms | Observed workers |",
             "|---|---:|---|---:|---:|---:|---:|---:|---:|"]
    for row in summaries:
        lines.append(f"| {row['stage']} | {row['pair']} | {row['label']} | {row['mixed_p95_ms']:.3f} | "
                     f"{row['read_p95_ms']:.3f} | {row['write_p95_ms']:.3f} | {row['writes_over_500_ms']} | "
                     f"{row['acks_over_300_ms']} | {row['workers']} |")
    lines += ["", "The 500 ms RPC and 300 ms ack cutoffs describe cohorts; neither is a regression gate.",
              "Every run has complete 256/256 mixed request phase matches, successful durable shutdown, and home removal.",
              "Ack stages and socket queues use each exact request. No percentiles are added or subtracted.",
              "The apply phase is residual time, not a direct disk or CPU measurement.",
              "Missing phase fields remain null/unreported; they do not become measured zeros.",
              "Four pairs on one shared machine do not establish statistical certainty or a production speed guarantee.",
              "These controls do not assign a cause to any source, build, disk, scheduler, or background merge.", ""]
    lines += ["Full exact slow-write records for all runs are in control-comparison.json.", "",
              "Two exact examples follow: the largest RPC and the largest reported ack.", "", "```jsonl"]
    requests = [{"stage": row["stage"], "pair": row["pair"], "label": row["label"], **value}
                for row in summaries for value in row["exact_slow_requests"]]
    examples = [max(requests, key=lambda row: row["rpc_ms"]), max(requests, key=lambda row: row["ack_us"])]
    unique = {(row["stage"], row["pair"], row["label"], row["request_id"]): row for row in examples}
    lines.extend(json.dumps(row, sort_keys=True, separators=(",", ":")) for row in unique.values())
    lines += ["```", ""]
    return lines


def report(product, output):
    controls = check_controls(output, product)
    historical, driver = historical_report(product)
    summaries, cohorts, hosts = [], [], []
    for stage in STAGES:
        folder = output / stage
        manifest = check_manifest(folder, stage, controls, driver)
        hosts.append(manifest["host"])
        stage_summaries = []
        for run in manifest["runs"]:
            if run.get("directory") != f"pair-{run['pair']}-{run['label']}":
                raise ValueError("The measured run directory differs from its fixed pair")
            summary, cohort = inspect_run(folder / run["directory"], run, stage)
            summaries.append(summary)
            stage_summaries.append(summary)
            cohorts.append(cohort)
        if (sum(row["validated_write_request_samples"] for row in stage_summaries)
                != controls["stages"][stage]["validated_durable_write_records"]):
            raise ValueError("The stage durable write sample count differs from its controller record")
        comparison = historical.compare(folder, manifest)
        save(folder / "comparison.json", comparison)
        (folder / "summary.md").write_text(historical.render(folder, manifest, comparison))
    if any(cohort != cohorts[0] for cohort in cohorts) or any(host != hosts[0] for host in hosts):
        raise ValueError("The mixed request bodies or measurement hosts differ across the controls")
    if len({row["workers"] for row in summaries}) != 1:
        raise ValueError("The observed socket workers differ across the controls")
    save(output / "control-comparison.json", {"complete": True, "runs": summaries,
         "same_request_cohort": True, "same_host": True,
         "scope": "Descriptive fixed-source controls; no automatic cause or regression conclusion"})
    return "\n".join(summary_lines(controls, summaries)) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--product", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    product = args.product.resolve(strict=True)
    if any(output == root or output.is_relative_to(root)
           for root in (product, Path(__file__).resolve().parents[3])):
        parser.error("Raw control output must stay outside both source checkouts")
    try:
        text = report(product, output)
        complete = True
    except (OSError, ValueError, KeyError, TypeError, ZeroDivisionError, subprocess.SubprocessError) as failure:
        text = ("# Latency controls incomplete\n\n" + f"{type(failure).__name__}: {failure}\n\n"
                "This run supplies no complete control comparison. Inspect the retained CI artifact.\n")
        complete = False
    output.mkdir(parents=True, exist_ok=True)
    (output / "summary.md").write_text(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
            stream.write(text)
    print(text, flush=True)
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
