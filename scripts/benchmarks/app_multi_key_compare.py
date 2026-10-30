#!/usr/bin/env python3
"""Compare exact app releases across a real conversion and daemon upgrade in CI."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import re

from node import Node, file_hash, save

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "disk_v2"))
import cleanup


def finish_setup(node, output):
    # Cutover reclamation is a one-time cost, outside the steady-state requests.
    flushed, _, _ = node.call("/api/db/flush", {})
    if flushed.get("durable") is not True:
        raise RuntimeError("The app setup lacks a durable flush")
    until = time.monotonic() + 600
    while True:
        status = node.call("/api/db/molecule-drain", {})[0]["molecule_drain"]
        report = status["report"]
        if report["refused"] or report["last_error"]:
            raise RuntimeError(f"App setup reclamation failed: {status}")
        if (status["candidate_rows"] == 0 and not status["candidate_rows_capped"]
                and all(report[key] == 0 for key in ("pending", "parked", "waiting_records"))):
            break
        if time.monotonic() >= until:
            raise RuntimeError(f"App setup reclamation did not finish: {status}")
        time.sleep(0.5)
    save(output / "setup-drain.json", status)
    save(output / "setup-cleanup.json", cleanup.request_and_wait(node))


def run_app(args, app, label, node, output, mode):
    environment = dict(os.environ, FOLDDB_SOCKET_PATH=str(node.socket),
                       FBRAIN_APP_IDENTITY_ENFORCE="on",
                       FBRAIN_FOLDDB_SOCKET=str(node.socket), BRAIN_CONFIG=str(output / "config.json"),
                       KANBAN_CONFIG=str(output / "config.json"))
    command = ["bun", str(args.driver), "--app", app,
               "--baseline", str(args.apps / app / "baseline"),
               "--candidate", str(args.apps / app / "candidate"),
               "--socket", str(node.socket), "--config", str(output / "config.json"),
               "--output", str(output), "--mode", mode, "--label", label,
               "--records", str(args.records)]
    with (output / f"{mode}.log").open("w") as log:
        subprocess.run(command, env=environment, stdout=log, stderr=log, check=True,
                       timeout=3600)


def require_cloud_off(node, output, phase):
    status = node.call("/api/sync/byte-backup-v2", {"action": "status"})[0]
    if (status.get("enabled") is not False
            or status.get("automatic_backup_enabled") is not False
            or status.get("running") is not False
            or status.get("manual_connection") is not None):
        raise RuntimeError("The generated comparison node must keep cloud backup off")
    save(output / f"cloud-off-{phase}.json", {
        key: status.get(key) for key in
        ("enabled", "automatic_backup_enabled", "running", "manual_connection")})


def compare(args):
    revisions = json.loads(args.revisions.read_text())
    results = []
    for app, selected in revisions.items():
        for label, revision in selected.items():
            observed = subprocess.check_output(
                ["git", "-C", str(args.apps / app / label), "rev-parse", "HEAD"], text=True).strip()
            if observed != revision:
                raise RuntimeError("App checkout differs from its selected revision")
        for pair, order in enumerate([["baseline", "candidate"], ["candidate", "baseline"], ["fresh"]]):
            for label in order:
                output = args.output / app / str(pair) / label
                output.mkdir(parents=True)
                setup_binary, setup_revision = ((args.binary, args.revision) if label == "fresh"
                                               else (args.baseline_binary, args.baseline_revision))
                node = Node(setup_binary, setup_revision, output, full_socket_name="folddb-full.sock", environment={
                    "APP_IDENTITY_ENFORCE": "true", "LASTDB_SELF_METRICS_TO_DB": "false",
                    "LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "LASTDB_TTL_SWEEP": "0"},
                    temporary_directory=Path(os.environ["RUNNER_TEMP"]).resolve())
                try:
                    node.start()
                    require_cloud_off(node, output, "before-setup")
                    run_app(args, app, label, node, output, "setup")
                    require_cloud_off(node, output, "after-setup")
                    finish_setup(node, output)
                    save(output / "setup-daemon.json", {"revision": setup_revision,
                         "sha256": file_hash(setup_binary), "version": node.call("/api/version")[0]})
                    node.stop()
                    if label == "candidate":
                        # Reuse this owned generated home only after conversion and a clean stop.
                        # New-release startup must independently admit every installed schema.
                        node.binary, node.revision = args.binary, args.revision
                    node.start()
                    require_cloud_off(node, output, "before-measure")
                    run_app(args, app, label, node, output, "measure")
                    require_cloud_off(node, output, "after-measure")
                    results.append(json.loads((output / "result.json").read_text()))
                    save(output / "disk.json", node.disk_breakdown())
                finally:
                    node.close()
    rows = []
    for app in revisions:
        before = [item for item in results if item["app"] == app and item["label"] == "baseline"]
        after = [item for item in results if item["app"] == app and item["label"] == "candidate"]
        if len(before) != 2 or len(after) != 2:
            raise RuntimeError("The comparison lacks two balanced pairs")
        for item in before + after:
            if (item["membership"]["logical_records"] != args.records
                    or item["membership"]["body_bytes"] != args.records * 2048):
                raise RuntimeError("Logical app volume differs")
            if any(value["errors"] or value["requests"] < 16 for value in item["metrics"].values()):
                raise RuntimeError("The comparison contains failed or insufficient requests")
        for metric in before[0]["metrics"]:
            for percentile in ["p50_ms", "p95_ms"]:
                old = statistics.median(item["metrics"][metric][percentile] for item in before)
                new = statistics.median(item["metrics"][metric][percentile] for item in after)
                rows.append({"app": app, "operation": metric, "percentile": percentile,
                             "before_ms": old, "after_ms": new, "change_percent": (new / old - 1) * 100})
    report = {"daemon_revision": args.revision, "daemon_sha256": file_hash(args.binary),
              "baseline_daemon_revision": args.baseline_revision,
              "baseline_daemon_sha256": file_hash(args.baseline_binary),
              "records_per_home": args.records,
              "app_revisions": revisions, "balanced_pairs": 2, "rows": rows,
              "results": results, "primary_unchanged": True}
    save(args.output / "comparison.json", report)
    lines = ["## App multi-key comparison", "", f"Daemon: `{args.revision}`",
             f"Compatible release: `{args.baseline_revision}`.",
             f"Two balanced pairs. Each home contains {args.records} generated records and {args.records * 2048} body bytes.",
             "The compatible release seeds both arms and converts the candidate. The removal release then reopens the converted home.",
             "The baseline retains the earlier app and daemon. A separate fresh-init arm uses the new app and daemon.",
             "This measures the complete app-and-daemon cutover. The same-layout native benchmark isolates engine latency separately.",
             "No primary data participates.", "",
             "| App / operation | Percentile | Before ms | After ms | Change |",
             "|---|---|---:|---:|---:|"]
    for row in rows:
        lines.append(f"| {row['app']} / {row['operation']} | {row['percentile']} | "
                     f"{row['before_ms']:.3f} | {row['after_ms']:.3f} | {row['change_percent']:+.1f}% |")
    lines += ["", "Canonical field definitions remain shared; native record values remain independent.",
              "Native guarded competition and continuation checks appear in the raw result artifacts.",
              "Speed changes are advisory. Membership, bodies, counts, guards, and cursor checks must succeed."]
    text = "\n".join(lines) + "\n"
    (args.output / "summary.md").write_text(text)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(text)


def main():
    parser = argparse.ArgumentParser()
    for name in ["binary", "revision", "apps", "driver", "revisions", "output"]:
        parser.add_argument("--" + name, required=True, type=str if name == "revision" else Path)
    parser.add_argument("--baseline-binary", type=Path, required=True)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--records", type=int, choices=[1000, 10000], required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true":
        parser.error("App comparison builds and operations run only in CI")
    if any(not re.fullmatch("[0-9a-f]{40}", value) for value in (args.revision, args.baseline_revision)):
        parser.error("Both daemon revisions must name exact source commits")
    args.output.mkdir(parents=True, exist_ok=True)
    compare(args)


if __name__ == "__main__":
    main()
