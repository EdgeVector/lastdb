#!/usr/bin/env python3
"""Compare exact app revisions on one exact daemon, with generated owned homes."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess

from node import Node, file_hash, save


def run_app(args, app, label, node, output, mode):
    environment = dict(os.environ, FOLDDB_SOCKET_PATH=str(node.socket),
                       FBRAIN_APP_IDENTITY_ENFORCE="off")
    command = ["bun", str(args.driver), "--app", app,
               "--baseline", str(args.apps / app / "baseline"),
               "--candidate", str(args.apps / app / "candidate"),
               "--socket", str(node.socket), "--config", str(output / "config.json"),
               "--output", str(output), "--mode", mode, "--label", label]
    with (output / f"{mode}.log").open("w") as log:
        subprocess.run(command, env=environment, stdout=log, stderr=log, check=True,
                       timeout=1200)


def compare(args):
    revisions = json.loads(args.revisions.read_text())
    results = []
    for app, selected in revisions.items():
        for label, revision in selected.items():
            observed = subprocess.check_output(
                ["git", "-C", str(args.apps / app / label), "rev-parse", "HEAD"], text=True).strip()
            if observed != revision:
                raise RuntimeError("App checkout differs from its selected revision")
        for pair, order in enumerate([["baseline", "candidate"], ["candidate", "baseline"]]):
            for label in order:
                output = args.output / app / str(pair) / label
                output.mkdir(parents=True)
                node = Node(args.binary, args.revision, output, environment={
                    "APP_IDENTITY_ENFORCE": "false", "LASTDB_SELF_METRICS_TO_DB": "false",
                    "LASTDB_BYTE_BACKUP_V2_ENABLED": "false", "LASTDB_TTL_SWEEP": "0"})
                try:
                    node.start()
                    run_app(args, app, label, node, output, "setup")
                    node.stop()
                    node.start()
                    run_app(args, app, label, node, output, "measure")
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
            if item["membership"]["logical_records"] != 256 or item["membership"]["body_bytes"] != 524288:
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
              "app_revisions": revisions, "balanced_pairs": 2, "rows": rows,
              "results": results, "primary_unchanged": True}
    save(args.output / "comparison.json", report)
    lines = ["## App multi-key comparison", "", f"Daemon: `{args.revision}`",
             "Two balanced pairs. Each home contains 256 generated records and 524288 body bytes.",
             "The same daemon serves both exact app revisions. No primary data participates.", "",
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
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    compare(args)


if __name__ == "__main__":
    main()
