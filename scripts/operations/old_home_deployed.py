"""A home written by the old build moves and publishes its first snapshot, against a deployed relay.

The plain home of `old_home_proof.py` (steps 1 to 4), with a fresh identity: the old build writes
records, replaces, deletes, alternate keys and a file with no cloud; `adopt_old_home.py` makes the
plain-file changes; the current `lastdbd` starts on it with the deployed relay, publishes the files
it had as its first snapshot, and every later write goes up as one entry. A restore from the recovery
phrase installs the snapshot, replays only the writes after it, and equals the moved home.

Not run, because each needs the relay process or its disk: the kill before the snapshot is named
(the relay is stopped in one variant, and the other is timed on the relay's first stored file), the
marker variant, the second device, and the check that the relay holds no marker string.
"""
from pathlib import Path
import sys
import time

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from old_home_nodes import make_old_home  # noqa: E402
import old_home_adopt as adopt_checks  # noqa: E402
from old_home_steps import database_of, post_writes, view, wait_view  # noqa: E402
from personal_proof_nodes import DeployedRelay, Device, require, wait  # noqa: E402
from personal_proof_snapshot import last_record  # noqa: E402

ADOPT = _OPS / "adopt_old_home.py"
SYNC_SECONDS = 300
ALPHA_RECORDS, BETA_RECORDS = 400, 90
SKIPPED = (
    ("kill before the first snapshot is named, with the relay away or during a large upload",
     "the relay cannot be stopped, and the start of an upload cannot be seen on it"),
    ("variant home with unacknowledged spool entries and pending markers", "kept to the reference relay run"),
    ("second device writes and the moved home receives it", "kept to the reference relay run"),
    ("the relay holds no marker string from a record, a schema name or the file", "the relay's stored bytes cannot be read"),
    ("the relay holds exactly one snapshot entry and one entry per write, by listing its files",
     "the relay's storage cannot be listed; the spool and a restore are checked instead"),
)


def first_snapshot_done(device):
    """The moved home has named its first snapshot and released the preserved files. Read from the home alone."""
    state = device.spool_state()
    return (state is not None and state.get("snapshot_required") is False
            and (device.log_dir() / "snapshot-last.json").is_file()
            and not (device.node.home / ".signed-initial-snapshot").exists())


def prove_deployed(args, out, relay, cli, owned):
    """Steps 1 to 4 of the plain home. Returns the report that `render` turns into the job summary."""
    require(isinstance(relay, DeployedRelay), "this proof runs against a deployed relay")
    report = {"relay": relay.url, "steps": []}
    node, expected = make_old_home(args.old_daemon, args.revision, out / "old-plain", cli, "plain",
                                   ALPHA_RECORDS, BETA_RECORDS)
    try:
        started = time.monotonic()
        refused = adopt_checks.refuses_running(node, cli, ADOPT)
        node.stop()
        report["adopt"] = adopt_checks.adopt_and_repeat(node.home, cli, ADOPT, expect_spool=False)
        database = database_of(cli, node.home)
        report["steps"].append({"step": "1. old home written, adopted, adopt repeated", "seconds": round(time.monotonic() - started, 1)})

        moved = Device("moved-plain", args, out, relay)
        owned.append(moved)
        moved.node.home = node.home
        started = time.monotonic()
        moved.start()
        wait("the first snapshot of the moved home", lambda: first_snapshot_done(moved), seconds=SYNC_SECONDS)
        first_seconds = round(time.monotonic() - started, 1)
        wait_view(moved, expected, "the moved home to hold what the old build wrote")
        require(moved.database == database, "the moved home runs on another database id")
        snapshot = last_record(moved)
        files = snapshot["manifest"]["files"]
        report["first_snapshot"] = {"seconds_to_named": first_seconds, "files": len(files), "bytes": sum(f["bytes"] for f in files),
                                    **{k: snapshot["report"][k] for k in ("uploaded_files", "uploaded_bytes", "capture_ms", "upload_ms")}}
        report["steps"].append({"step": f"2. first snapshot named; every old record read by key and alternate key (a running daemon refused: {refused})",
                                "seconds": first_seconds})
        print(f"2. the old home moved; the first snapshot is on the DEV relay after {first_seconds}s and the moved home "
              f"reads all {len(expected['alpha'])} + {len(expected['beta'])} records, by key and by alternate key, and the file", flush=True)

        started = time.monotonic()
        writes = post_writes(moved, expected, "after")
        wait(f"{moved.name} to upload", moved.synced_up, seconds=SYNC_SECONDS)
        state = moved.spool_state()
        require(state["head"] == writes and state["acked"] == writes,
                f"the spool holds {state!r} for {writes} writes after the move")
        require(moved.checkpoint()["writer_seq"] == writes, f"the relay confirmed {moved.checkpoint()!r} for {writes} writes")
        report["writes_after"] = writes
        report["steps"].append({"step": "3. writes after the move are one confirmed entry each, none for the old records",
                                "seconds": round(time.monotonic() - started, 1)})
        print(f"3. {writes} writes after the move are {writes} confirmed entries, none for the old records", flush=True)

        restored = Device("restore-plain", args, out, relay)
        owned.append(restored)
        started = time.monotonic()
        restore = restored.restore(expected["phrase"])
        seconds = round(time.monotonic() - started, 1)
        require(restore["snapshot"] is not None, f"the restore found no snapshot: {restore!r}")
        require(restore["replayed"] == writes, f"the restore replayed {restore['replayed']} entries, expected {writes}")
        restored.start()
        wait_view(restored, expected, "the restored home to equal the moved home")
        require(view(restored, restored.out / "files") == view(moved, moved.out / "files"), "the restored home differs from the moved home")
        report["restore"] = {"seconds": seconds, "replayed": restore["replayed"], "snapshot": restore["snapshot"],
                             "peak_memory_bytes": restore["peak_memory_bytes"]}
        report["steps"].append({"step": "4. restore from the phrase equals the moved home and replays only the writes after the snapshot",
                                "seconds": seconds})
        print(f"4. a restore from the phrase installed the snapshot in {seconds}s, replayed the {writes} writes after it, "
              "and equals the moved home key by key", flush=True)
        return report
    finally:
        # The moved home is the old home: stop each daemon on it before its directory is removed.
        for device in owned:
            try:
                device.stop()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: stop failed: {error}", file=sys.stderr)
        node.close()


def render(report):
    """The job summary, in Markdown."""
    first, restore = report["first_snapshot"], report["restore"]
    lines = ["## An old home moves and publishes its first snapshot on the deployed Exemem DEV relay", "",
             "A fresh identity for this run (the old build's home gets a new seed; no account or secret).", "",
             "### Ran", "", "| Step | Seconds |", "|---|---|"]
    lines += [f"| {item['step']} | {item['seconds']} |" for item in report["steps"]]
    lines += ["", "### Skipped (each needs the relay process or its disk)", "", "| Step | Reason |", "|---|---|"]
    lines += [f"| {name} | {reason} |" for name, reason in SKIPPED]
    lines += ["", "### Timings and bytes", "",
              f"- First snapshot: named {first['seconds_to_named']}s after the daemon started; {first['files']} files, "
              f"{first['bytes']} bytes, {first['uploaded_files']} uploaded ({first['uploaded_bytes']} bytes); capture "
              f"{first['capture_ms']} ms, upload {first['upload_ms']} ms.",
              f"- Writes after the move: {report['writes_after']} entries, each confirmed by the relay.",
              f"- Restore: {restore['seconds']}s, {restore['replayed']} entries replayed, snapshot download "
              f"{restore['snapshot'].get('download_ms')} ms.",
              f"- Added to the relay: about {first['uploaded_bytes']} bytes of blobs, one manifest, one snapshot entry, one birth "
              f"and {report['writes_after']} data entries. The size of an entry is not visible to the client.", ""]
    return "\n".join(lines)
