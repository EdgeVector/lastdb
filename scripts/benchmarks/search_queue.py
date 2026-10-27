#!/usr/bin/env python3
"""CI-only growth of an optional Search queue while its consumer is absent."""

import argparse
import json
import os
from pathlib import Path
import re
import signal
import time

from disk_budget import DiskBudgetWorkload
from growth import PROFILES
from node import Node, file_hash, save
from run import host_details, interrupted


def batch_stats(home):
    directory = home / "apps/search/inbox"
    files, size = 0, 0
    if directory.exists():
        for number, path in enumerate(directory.iterdir()):
            if number >= 8192:
                raise ValueError("The generated Search inbox exceeds the file-walk bound")
            if path.name.endswith(".json"):
                info = path.lstat()
                if not path.is_file() or path.is_symlink():
                    raise ValueError("The generated Search batch is not a regular file")
                files += 1
                size += info.st_size
    return {"batch_files": files, "batch_bytes": size}


def point(workload, number):
    result = workload.checkpoint(number)
    result["search_batches"] = batch_stats(workload.node.home)
    return result


def measure(args, mode, folder):
    folder.mkdir()
    result, node = {"complete": False, "mode": mode}, None
    cap = (4 if args.profile == "nightly" else 1) * 2**20
    try:
        node = Node(args.candidate, args.candidate_revision, folder,
                    environment={"LASTDB_SEARCH_QUEUE_MAX_BYTES": str(cap)},
                    arguments=("--enable-search",) if mode == "enabled" else ())
        node.start()
        workload = DiskBudgetWorkload(node, {"growth": PROFILES[args.profile]}, folder)
        workload.seed()
        started = time.monotonic()
        with workload.monitor():
            result["updates"] = workload.measure("fixed-update")
            result["after_updates"] = point(workload, 0)
            result["update_seconds"] = time.monotonic() - started
        node.stop()
        save(folder / "reopen-shutdown.json", json.loads((folder / "shutdown.json").read_text()))
        node.start()
        with workload.monitor():
            workload.write(0, workload.live_records, "after-reopen", False)
            result["after_reopen_write"] = point(workload, 1)
        result["peak_memory_bytes"] = max(sample["memory_bytes"] for sample in workload.samples)
        native, _, _ = node.call("/api/db/native-status")
        result["native_status"] = native
        policy = native["search"]
        if (native["native_disk"]["writer_failed"] is not False or policy["status_error"]
                or policy["enabled"] != (mode == "enabled") or policy["max_batch_bytes"] != cap):
            raise ValueError("The Search queue measurement lacks the selected policy or a healthy writer")
        before, after = (result[name]["search_batches"] for name in ("after_updates", "after_reopen_write"))
        result["advisories"] = []
        if mode == "default-off" and after["batch_bytes"]:
            result["advisories"].append("queue-created-while-disabled")
        if mode == "enabled":
            for name, crossed in (("queue-byte-cap", after["batch_bytes"] > cap),
                                  ("queue-file-cap", after["batch_files"] > policy["max_batch_files"]),
                                  ("missing-rebuild-signal", policy["rebuild_required"] is not True),
                                  ("queue-grew-after-cap-and-reopen", after != before)):
                if crossed:
                    result["advisories"].append(name)
        result["complete"] = True
    except BaseException as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        if node is not None:
            try:
                node.close()
            except BaseException as error:
                result.update(complete=False, cleanup_error=f"{type(error).__name__}: {error}")
        save(folder / "queue-growth.json", result)
    if not result["complete"]:
        raise ValueError(f"Search queue measurement incomplete: {result}")
    return result


def render(manifest):
    lines = ["# Optional Search queue growth — advisory", "",
             f"Candidate: `{manifest['candidate_revision']}`. Driver: `{manifest['driver_revision']}`.",
             "Default-off and explicit --enable-search use separate generated homes. No Search consumer runs.",
             "PRs use 512 records, three full update rounds, two pairs, and a 1 MiB queue cap.",
             "Nightly uses 2,048 records, eight rounds, three pairs, and a 4 MiB cap.",
             "These smaller caps expose saturation without a large corpus. The product default is 64 MiB and 4,096 files.",
             "Every accepted value is verified by key. After reopen, one further full update must complete.", "",
             "| Pair | Mode | Logical MiB | Database MiB | Search queue MiB | Batch bytes MiB | Batch files | Whole home MiB | Advisory findings |",
             "|---:|---|---:|---:|---:|---:|---:|---:|---|"]
    for row in manifest["runs"]:
        result = row["result"]
        last = result["after_reopen_write"]
        sizes, batches = last["disk_breakdown"], last["search_batches"]
        values = [last["logical_storage"]["total_logical_bytes"], max(sizes["database"].values()),
                  max(sizes["search_queue"].values()), batches["batch_bytes"]]
        lines.append(f"| {row['pair']} | {row['mode']} | " + " | ".join(f"{v / 2**20:.3f}" for v in values)
                     + f" | {batches['batch_files']} | {max(sizes['whole_home'].values()) / 2**20:.3f} | "
                     + (", ".join(result["advisories"]) or "None") + " |")
    lines.extend(["", "Queue caps count batch file lengths and file count. Allocated queue blocks remain visible separately.",
                  "Only the same finding in the first two independent runs produces a warning. Size findings never fail CI.",
                  "Missing measurements, incorrect values, an unhealthy writer, or failed cleanup fail completion.",
                  "This generated-data run does not read the primary or rebuild any installed Search store.", ""])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--driver-revision", required=True)
    parser.add_argument("--profile", choices=("standard", "nightly", "batch-insert"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted"):
        parser.error("The Search queue benchmark requires GitHub-hosted Actions")
    if any(not re.fullmatch(r"[0-9a-f]{40}", value) for value in (args.candidate_revision, args.driver_revision)):
        parser.error("Revisions must be complete SHAs")
    signal.signal(signal.SIGTERM, interrupted)
    args.candidate = args.candidate.resolve(strict=True)
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"complete": False, "candidate_revision": args.candidate_revision,
                "candidate_sha256": file_hash(args.candidate), "driver_revision": args.driver_revision,
                "profile": args.profile, "host": host_details(), "runs": [],
                "drivers": {name: file_hash(Path(__file__).with_name(name)) for name in
                            ("search_queue.py", "node.py", "disk_usage.py", "disk_budget.py", "growth.py", "workload.py")}}
    try:
        for pair in range(3 if args.profile == "nightly" else 2):
            modes = ("default-off", "enabled") if pair % 2 == 0 else ("enabled", "default-off")
            for mode in modes:
                folder = args.output / f"pair-{pair}-{mode}"
                result = measure(args, mode, folder)
                manifest["runs"].append({"pair": pair, "mode": mode, "result": result})
                save(args.output / "queue-growth.json", manifest)
        manifest["complete"] = True
    finally:
        save(args.output / "queue-growth.json", manifest)
    report = render(manifest)
    (args.output / "summary.md").write_text(report)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(report)
    print(report)
    for mode in ("default-off", "enabled"):
        runs = [row["result"] for row in manifest["runs"] if row["mode"] == mode]
        for finding in set(runs[0]["advisories"]) & set(runs[1]["advisories"]):
            print(f"::warning title=Repeated Search queue advisory::{mode}: {finding}. This does not block merge.")


if __name__ == "__main__":
    main()
