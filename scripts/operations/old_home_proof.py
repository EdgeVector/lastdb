#!/usr/bin/env python3
"""A home written by the old build moves to the current build and publishes a first snapshot.

Real processes only. The old build (release nightly-20261119-d06054159) writes homes with no
cloud. `adopt_old_home.py` makes the minimum plain-file changes. The current `lastdbd` then starts
on the home with `reference_relay` and, because the home held data before it began to capture
writes, publishes the files it had as its first snapshot before any later write goes up.

1. Three old homes: a plain one (thousands of records, replaces, deletes, alternate keys, a file),
   a variant that the old build captured with pending markers and unacknowledged spool entries
   (cloud credentials that point nowhere), and one with a large file for the kill in step 6.
   The script refuses while the old daemon runs, `--check` changes nothing, a run changes only
   what it says, and a second run changes nothing.
2. The current build starts on the plain home. It publishes the first snapshot. Every record the
   old build wrote reads by primary key and by alternate key, and the file's bytes match.
3. Writes, replaces and deletes on the moved home after the snapshot are exactly the entries on
   the relay (one each, none for the old records).
4. A restore from the recovery phrase equals the moved home key by key, and the relay log shows
   that it read only the entries after the snapshot.
5. A second device from the phrase equals both, writes, and the moved home receives it.
6. The moved node is killed before the first snapshot is named, once during the upload of a large
   file and once with the relay away, with writes made before and during. After the restart it
   names the snapshot, and no write is lost or sent twice. The variant home shows that markers
   the old upload path left do not hold back a later snapshot.
7. The relay holds no marker string from a record, a schema name, or the file.

With `--relay-url` the relay is a parameter: the deployed Exemem DEV relay instead of a
`reference_relay` process. `old_home_deployed.py` then runs steps 1 to 4 on the plain home, with a
fresh identity, and lists what it does not run.
"""
import argparse
import json
from pathlib import Path
import sys
import time

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from harness import append_step_summary  # noqa: E402
from old_home_nodes import ALPHA, BETA, FILE_MARK, GROUPS, NOTE_MARK, make_old_home, recorded  # noqa: E402
import old_home_adopt as adopt_checks  # noqa: E402
from old_home_deployed import prove_deployed, render  # noqa: E402
from old_home_steps import database_of, post_writes, view, wait_view  # noqa: E402
from personal_proof_nodes import DeployedRelay, Device, Relay, relay_holds_none, require, wait  # noqa: E402
import v1_records as v1  # noqa: E402

ADOPT = _OPS / "adopt_old_home.py"
# Every current-build device the proof made, so each one is stopped and its scratch directory removed.
OWNED = []


def device_of(name, args, out, relay):
    device = Device(name, args, out, relay)
    OWNED.append(device)
    return device




def entries_of(relay, database):
    """The kinds of the entries on the relay for one database: a list of (kind, file name)."""
    found = []
    for path in sorted((relay.data / "logs" / database).glob("*/*/*.json")) if (relay.data / "logs" / database).is_dir() else []:
        text = path.read_text()
        kind = next((name for name in ("snapshot", "birth", "data") if f'"kind":"{name}"' in text), "other")
        found.append((kind, path))
    return found


def snapshots_on(relay, database):
    return sum(1 for kind, _ in entries_of(relay, database) if kind == "snapshot")


def start_moved(args, out, relay, name, node, down=False):
    """The current build on an adopted home. The Device keeps its own scratch root, the home is the old one."""
    device = device_of(name, args, out, relay)
    device.node.home = node.home
    if down:
        relay.stop()
    device.start()
    return device


def first_snapshot_done(relay, device):
    state = device.spool_state()
    return (state is not None and state.get("snapshot_required") is False
            and snapshots_on(relay, device.database) >= 1
            and not (device.node.home / ".signed-initial-snapshot").exists())




def require_each_write_once(relay, device, writes):
    """The relay holds one entry of this device for each write after the move, in order, and no more."""
    wait(f"{device.name} to upload", lambda: (device.spool_state() or {}).get("head") == (device.spool_state() or {}).get("acked")
         and not (device.log_dir() / "outgoing.json").exists())
    state = device.spool_state()
    require(state["head"] == writes and state["acked"] == writes,
            f"{device.name}: the spool holds {state!r} for {writes} writes after the move")
    sequences = relay.stream_entries(device.database, device.device_id())
    require(sequences == list(range(1, writes + 1)),
            f"{device.name}: the relay holds {len(sequences)} entries of this device for {writes} writes")


def restore_equals(args, out, relay, name, phrase, moved, expected, writes):
    """A restore into a fresh directory from the phrase. Returns the report and the started device."""
    restored = device_of(name, args, out, relay)
    mark = len(relay.lines())
    report = restored.restore(phrase)
    read = sum(count for _, count in relay.requests_since(mark, "log/read"))
    require(report["snapshot"] is not None, f"{name}: the restore found no snapshot: {report!r}")
    require(report["replayed"] == writes, f"{name}: replayed {report['replayed']} entries, expected {writes}")
    total = len(expected["alpha"]) + len(expected["beta"])
    require(writes <= read <= writes + 4 and total > 4 * writes,
            f"{name}: the restore read {read} entries from the relay for {writes} written after the snapshot")
    restored.start()
    wait_view(restored, expected, f"{name} to equal the moved home")
    require(view(restored, restored.out / "files") == view(moved, moved.out / "files"),
            f"{name} differs from the moved home")
    return report, read, restored


def plain_home(args, out, relay, cli, report):
    """1 to 5 on the plain home."""
    node, expected = make_old_home(args.old_daemon, args.revision, out / "old-plain", cli, "plain", 3000, 600)
    nodes = [node]
    try:
        refused = adopt_checks.refuses_running(node, cli, ADOPT)
        node.stop()
        report["adopt"] = adopt_checks.adopt_and_repeat(node.home, cli, ADOPT, expect_spool=False)
        database = database_of(cli, node.home)
        moved = start_moved(args, out, relay, "moved-plain", node)
        wait("the first snapshot of the moved home", lambda: first_snapshot_done(relay, moved), seconds=300)
        wait_view(moved, expected, "the moved home to hold what the old build wrote")
        require(moved.database == database, "the moved home runs on another database id")
        require(snapshots_on(relay, database) == 1, "the first snapshot was named more than once")
        print(f"2. the old home (a daemon running refused: {refused}) moved; the first snapshot is on the relay and "
              f"the moved home reads all {len(expected['alpha'])} + {len(expected['beta'])} records, by key and by "
              "alternate key, and the file", flush=True)
        writes = post_writes(moved, expected, "after")
        require_each_write_once(relay, moved, writes)
        print(f"3. {writes} writes after the move are {writes} entries on the relay, one each, none for the old records", flush=True)
        phrase = expected["phrase"]
        report["restore"], read, restored = restore_equals(args, out, relay, "restore-plain", phrase, moved, expected, writes)
        print(f"4. a restore from the phrase equals the moved home key by key; it read {read} entries from the relay "
              f"(the {writes} after the snapshot), not the {len(expected['alpha'])} records written before", flush=True)
        restored.stop()
        second_device(args, out, relay, phrase, moved, expected)
        report["relay_blind"] = hidden(relay)
        moved.stop()
        return report
    finally:
        for item in nodes:
            try:
                item.close()
            except Exception as error:  # noqa: BLE001
                print(f"cleanup: {error}", file=sys.stderr)


def second_device(args, out, relay, phrase, moved, expected):
    """5. A second device from the phrase equals the moved home, writes, and the moved home receives it."""
    second = device_of("second-device", args, out, relay)
    second.restore(phrase)
    second.start()
    wait_view(second, expected, "the second device to equal the moved home")
    fresh = "from-second-device"
    second.write([v1.create_item(ALPHA, {"hash": fresh}, recorded(fresh, "second"))])
    expected["alpha"][fresh] = recorded(fresh, "second")
    wait_view(moved, expected, "the moved home to receive the write of the second device")
    key = sorted(expected["alpha"])[3]
    moved.change([{"schema": ALPHA, "op": "replace", "key": {"hash": key}, "record": recorded(key, "moved-later")}])
    expected["alpha"][key] = recorded(key, "moved-later")
    wait_view(second, expected, "the second device to receive the write of the moved home")
    second.stop()
    print("5. a second device from the phrase equals the moved home, writes, and the moved home receives it (and the reverse)",
          flush=True)


def hidden(relay):
    """7. The relay stored files, and none holds a name, a value or the file."""
    relay_holds_none(relay, [ALPHA, BETA, NOTE_MARK, FILE_MARK, "old-attachment", *GROUPS])
    return {"checked": True}


def killed_home(args, out, relay, cli, label, down, big, report):
    """6. Kill the moved node before the first snapshot is named; after the restart nothing is lost or repeated."""
    node, expected = make_old_home(args.old_daemon, args.revision, out / f"old-{label}", cli, label,
                                   600 if not big else 300, 150 if not big else 30,
                                   unreachable_cloud=not big, fat_records=1800 if big else 0)
    nodes = [node]
    summary = {"variant": label}
    try:
        try:
            node.stop()
            summary["old_stopped_cleanly"] = True
        except RuntimeError as error:
            summary["old_stopped_cleanly"] = str(error)
        summary["adopt"] = adopt_checks.adopt_and_repeat(node.home, cli, ADOPT, expect_spool=not big)
        database = database_of(cli, node.home)
        moved = start_moved(args, out, relay, f"moved-{label}", node, down=down)
        if down:
            before = post_writes(moved, expected, "during")
            require(snapshots_on(relay, database) == 0, "the relay holds a snapshot while it was stopped")
        else:
            before = 0
            # The birth is the first thing the upload puts on the relay. The large file is still going up.
            deadline = time.monotonic() + 120
            while not any(kind == "birth" for kind, _ in entries_of(relay, database)):
                require(time.monotonic() < deadline, "the upload of the first snapshot never started")
                time.sleep(0.005)
        moved.crash()
        summary["snapshots_when_killed"] = snapshots_on(relay, database)
        require(summary["snapshots_when_killed"] == 0, f"{label}: the snapshot was named before the kill: the kill missed the window")
        require((moved.node.home / ".signed-initial-snapshot").is_dir(), f"{label}: the cut of the first snapshot is gone after the kill")
        if down:
            relay.stop()
        moved.start()
        after = post_writes(moved, expected, "restarted")
        if down:
            relay.start()
        wait(f"{label}: the first snapshot", lambda: first_snapshot_done(relay, moved), seconds=300)
        require_each_write_once(relay, moved, before + after)
        wait_view(moved, expected, f"{label}: the moved home to hold every record")
        restored_report, read, restored = restore_equals(args, out, relay, f"restore-{label}", expected["phrase"], moved,
                                                         expected, before + after)
        summary.update(writes=before + after, entries_read_by_restore=read, restore=restored_report["snapshot"])
        restored.stop()
        later = moved.snapshot_now()
        require(later.get("snapshot_seq"), f"{label}: a snapshot after the first one failed: {later!r}")
        summary["snapshot_after_first"] = later
        print(f"6. {label}: killed before the first snapshot was named, with {before} writes before the kill and "
              f"{after} after the restart; every write is on the relay once, the restore equals the home, and a "
              "later snapshot works", flush=True)
        moved.stop()
        report[label] = summary
    finally:
        for item in nodes:
            try:
                item.close()
            except Exception as error:  # noqa: BLE001
                print(f"cleanup: {error}", file=sys.stderr)


def oversize_refused(args, out, cli):
    """A file the old home holds by reference, larger than a run can hold, is refused before any change."""
    node, _ = make_old_home(args.old_daemon, args.revision, out / "old-oversize", cli, "oversize", 20, 5,
                            file_bytes=40 * 1024 * 1024)
    try:
        node.stop()
        owner = adopt_checks.registry_owner(node.home)
        result = adopt_checks.run(ADOPT, node.home, cli)
        require(result.returncode == 2 and "larger than" in result.stderr,
                f"the script did not refuse a 40 MiB file reference: {result.returncode} {result.stderr!r}")
        require(adopt_checks.registry_owner(node.home) == owner, "the refused run changed the registry")
    finally:
        node.close()


def dump_logs(root):
    """The end of every daemon log, for a failed run."""
    for log in sorted(root.glob("*/node.log")):
        lines = log.read_text(errors="replace").splitlines()
        print(f"--- {log.relative_to(root)}: last 40 of {len(lines)} lines", file=sys.stderr)
        print("\n".join(line[:400] for line in lines[-40:]), file=sys.stderr)


def run_deployed(args):
    """Steps 1 to 4 on the plain home against a deployed relay. The summary is printed and appended to the job summary."""
    cli = Path(args.cli)
    (args.output / "old-plain").mkdir(exist_ok=True)
    relay = DeployedRelay(args.relay_url)
    failure = None
    try:
        text = render(prove_deployed(args, args.output, relay, cli, OWNED))
        (args.output / "summary.md").write_text(text)
        append_step_summary(text)
        print(text, flush=True)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        dump_logs(args.output)
    finally:
        for device in OWNED:
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}", file=sys.stderr)
    if failure is not None:
        raise SystemExit(1)
    print("Old home proof passed against the deployed relay", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("old-daemon", "daemon", "cli", "revision"):
        parser.add_argument(f"--{name}", required=True)
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument("--relay-bin", help="run every step against a reference_relay process of this binary")
    where.add_argument("--relay-url", help="run steps 1 to 4 on the plain home against this deployed relay origin")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.relay_url:
        return run_deployed(args)
    for name in ("old-plain", "old-variant", "old-big", "old-oversize"):
        (args.output / name).mkdir(exist_ok=True)
    cli = Path(args.cli)
    relay = Relay(Path(args.relay_bin), args.output)
    relay.start()
    report = {}
    failure = None
    try:
        plain_home(args, args.output, relay, cli, report)
        if relay.process is None:
            relay.start()
        killed_home(args, args.output, relay, cli, "variant", True, False, report)
        if relay.process is None:
            relay.start()
        killed_home(args, args.output, relay, cli, "big", False, True, report)
        oversize_refused(args, args.output, cli)
        (args.output / "summary.json").write_text(json.dumps(report, indent=2, default=str))
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        dump_logs(args.output)
    finally:
        for device in OWNED:
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}", file=sys.stderr)
        relay.stop()
    if failure is not None:
        raise SystemExit(1)
    print("Old home proof passed", flush=True)


if __name__ == "__main__":
    main()
