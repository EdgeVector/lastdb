"""Steps 11 to 14 of the personal database proof: the snapshot as the checkpoint of the signed log.

Device A takes a snapshot, which uploads the native files of its store and a `snapshot` entry on the
snapshot stream, and then writes more. A home restored from the recovery phrase installs the snapshot
and reads only the entries after it. The proof checks that against a full replay, kills the restore
at several points, checks that a second snapshot uploads only the files that changed, that the relay
cannot read the snapshot, that a restored device and A exchange writes, and measures the scale.
"""
import hashlib
import json
from pathlib import Path
import shutil
import time

from personal_proof_nodes import (
    ALPHA, BETA, GAMMA, MEBIBYTE, MEMORY_BOUND_BYTES, MARK, converged, create, delete, expect, relay_files,
    relay_holds_none, replace, wait)
from personal_proof_steps import gamma_replace, gamma_seen, gamma_view
from record_runs import require
import v1_records as v1

SNAP_MARK = "SNAPMARK-7d41c0e9b2"
KILL_DELAYS = (0.15, 0.5, 1.0, 1.6, 2.4)


def stream_dirs(relay, database):
    """Every device stream directory of the relay: logs/<db>/<node>/<device>."""
    return sorted(path for path in (relay.data / "logs" / database).glob("*/*") if path.is_dir())


def snapshot_stream(relay, database):
    """The directory of the snapshot stream, or None. Its first entry is a `snapshot` entry."""
    for path in stream_dirs(relay, database):
        first = path / "1.json"
        if first.is_file() and '"kind":"snapshot"' in first.read_text():
            return path
    return None


def newest_snapshot_entry(relay, database):
    """The newest `snapshot` entry on the relay, as the JSON the relay stored."""
    directory = snapshot_stream(relay, database)
    require(directory is not None, "the relay holds no snapshot stream")
    newest = max(int(path.stem) for path in directory.glob("*.json"))
    return newest, json.loads((directory / f"{newest}.json").read_text())


def tail_entries(relay, database, entry):
    """Entries the relay holds after the positions the snapshot entry names, and the total history."""
    through = {position["device_id"]: position["writer_seq"] for position in entry["through"]}
    tail = total = 0
    for path in stream_dirs(relay, database):
        first = path / "1.json"
        kind = json.loads(first.read_text())["kind"] if first.is_file() else None
        if kind in ("snapshot", "birth"):
            continue
        count = len(list(path.glob("*.json")))
        total += count
        tail += count - through.get(path.name, 0)
    return tail, total


def last_record(device):
    """What the daemon kept of its last snapshot: the manifest, and the stream positions."""
    return json.loads((device.log_dir() / "snapshot-last.json").read_text())


def read_lines(relay, mark, route):
    """(entries) of every 200 answer of a route after `mark` lines of the relay's log."""
    return sum(entries for status, entries in relay.requests_since(mark, route) if status == "200")


def uploads_since(relay, mark):
    return sum(1 for line in relay.lines()[mark:] if "route=blob/upload status=200" in line)


def views(devices, state, folders, expected):
    """Every device holds `state` and reads `expected` through the alternate keys and the file."""
    return all(device.snapshot() == state and gamma_seen(device, folders[device.name]) == expected
               for device in devices)


def write_after(a, state, expected):
    """Changes, deletes and new records after the snapshot, in all three schemas."""
    a.change([replace(ALPHA, "a1", "three"), delete(ALPHA, "b3")])
    a.write([create(ALPHA, "post-1", "after"), create(BETA, "post-2", "after")])
    a.change([replace(BETA, "c1", "three")])
    a.change([gamma_replace("g2", "grp-y", "g2-three")])
    a.write([v1.create_item(GAMMA, {"hash": "g4"}, {"id": "g4", "note": "g4-one"},
                            alternates={"by_group": {"hash": "grp-x", "range": "g4"}})])
    a.change([{"schema": GAMMA, "op": "delete", "key": {"hash": "g1"}}])
    expect(state, ALPHA, "a1", "three")
    del state[(ALPHA, f"{MARK}-key-b3")]
    expect(state, ALPHA, "post-1", "after")
    expect(state, BETA, "post-2", "after")
    expect(state, BETA, "c1", "three")
    expected["groups"] = {"grp-x": ["g4"], "grp-y": ["g2"], "grp-f": ["f1"]}
    expected["records"]["g1"] = None
    expected["records"]["g2"] = {"id": "g2", "note": "g2-three"}


def snapshot_restore_equals_replay(relay, relay2, devices, phrase, state):
    """11. A restore reads only the tail and equals a full replay, key by key."""
    a, b, e, f, *others = devices
    folders = {}
    for device in (a, b, e, f, *others):
        folders[device.name] = device.out / "file-dir"
        folders[device.name].mkdir(mode=0o700, exist_ok=True)
    expected = gamma_view(a, folders[a.name])
    a.write([create(ALPHA, "snapmark", SNAP_MARK)])
    expect(state, ALPHA, "snapmark", SNAP_MARK)
    wait("A to upload", a.synced_up)
    first = a.snapshot_now()
    require(first["uploaded_files"] == first["files"] and first["files"] >= 5, f"first snapshot: {first!r}")
    write_after(a, state, expected)
    require(gamma_view(a, folders[a.name]) == expected, "A does not read what it wrote after the snapshot")
    wait("A to upload the writes after the snapshot", a.synced_up)
    wait("B to hold them", lambda: converged((b,), state))
    seq, entry = newest_snapshot_entry(relay, a.database)
    tail, total = tail_entries(relay, a.database, entry)
    require(seq == 1 and tail > 0 and total > 4 * tail, f"snapshot seq {seq}, tail {tail}, history {total}")
    mark = len(relay.lines())
    restored = f.restore(phrase)
    snapshot = restored["snapshot"]
    require(snapshot and snapshot["snapshot_seq"] == 1, f"F restore: {restored!r}")
    reads = read_lines(relay, mark, "log/read")
    require(restored["replayed"] == tail, f"F replayed {restored['replayed']} entries, the tail is {tail}")
    require(reads <= tail + 1, f"F read {reads} entries from the relay, the tail is {tail} (and one snapshot entry)")
    # The same data by full replay: a relay with no snapshot stream.
    shutil.copytree(relay.data, relay2.data, ignore=shutil.ignore_patterns(snapshot_stream(relay, a.database).name))
    relay2.start()
    replayed = e.restore(phrase)
    require(replayed["snapshot"] is None and replayed["replayed"] == total, f"E restore: {replayed!r}")
    f.start()
    e.start()
    wait("F, E and A to read the same", lambda: views((a, b, e, f), state, folders, expected))
    for schema in (ALPHA, BETA):
        require(e.records(schema) == f.records(schema) == a.records(schema), f"{schema} differs")
    print(f"11. F restored from the snapshot and replayed {tail} of {total} entries (read {reads} from the "
          "relay); E restored by full replay; A, B, E and F read the same records, alternate keys and file", flush=True)
    report = {"history_entries": total, "tail_entries": tail, "relay_entries_read": reads,
              "snapshot": snapshot, "restore_f": {k: restored[k] for k in ("replayed", "devices", "peak_memory_bytes")},
              "first_snapshot": first}
    return report, folders, expected


def kill_the_restore(g, phrase, a, state, folders, expected):
    """12. A restore killed at several points leaves no home or a complete one, and a rerun completes it."""
    outcomes = []
    for delay in KILL_DELAYS:
        finished = g.restore_killed(phrase, delay)
        home = g.node.home
        if home.exists():
            require(finished, f"a restore killed after {delay}s left a home behind")
            require((home / "data" / "laststore-pack-node-v1").is_file(), "a finished restore left an incomplete home")
            shutil.rmtree(home)
        outcomes.append({"delay": delay, "finished_first": finished})
    g.restore(phrase)
    g.start()
    folders[g.name] = g.out / "file-dir"
    folders[g.name].mkdir(mode=0o700, exist_ok=True)
    wait("the rerun restore to equal A", lambda: views((g,), state, folders, expected))
    g.stop()
    print(f"12. restore killed after {KILL_DELAYS} seconds: no home or a complete one each time; the rerun "
          "completed and equals A", flush=True)
    return outcomes


def second_snapshot(relay, a, h, phrase, state, folders, expected):
    """13. A second snapshot uploads only the files that changed, the newest is used, and the relay cannot read it."""
    a.write([create(ALPHA, "second-1", "x"), create(ALPHA, "second-2", "x")])
    expect(state, ALPHA, "second-1", "x")
    expect(state, ALPHA, "second-2", "x")
    wait("A to upload", a.synced_up)
    mark = len(relay.lines())
    second = a.snapshot_now()
    uploaded = uploads_since(relay, mark)
    require(second["snapshot_seq"] == 2 and 0 < second["uploaded_files"] < second["files"], f"second snapshot: {second!r}")
    require(uploaded == second["uploaded_files"] + 1, f"the relay took {uploaded} uploads, expected files + manifest")
    mark = len(relay.lines())
    restored = h.restore(phrase)
    require(restored["snapshot"]["snapshot_seq"] == 2 and restored["replayed"] == 0, f"H restore: {restored!r}")
    require(read_lines(relay, mark, "log/read") <= 1, "H read entries beyond the snapshot entry")
    h.start()
    folders[h.name] = h.out / "file-dir"
    folders[h.name].mkdir(mode=0o700, exist_ok=True)
    wait("H to equal A", lambda: views((h,), state, folders, expected))
    # The relay holds the files of the snapshot, by digest, and none of them holds a marker.
    digests = {file["sha256"] for file in last_record(a)["manifest"]["files"]}
    held = {hashlib.sha256(path.read_bytes()).hexdigest() for path in relay_files(relay)}
    require(digests <= held, "the relay lacks a file of the newest snapshot")
    relay_holds_none(relay, [SNAP_MARK, "FILEMARK-9f3c2a71d4", GAMMA, "grp-x"])
    print(f"13. second snapshot uploaded {second['uploaded_files']} of {second['files']} files; H restored from "
          "it and replayed nothing; no marker in any relay byte, snapshot files included", flush=True)
    return {"second_snapshot": second, "restore_h": restored["snapshot"]}


def exchange_writes(relay, a, f, state, folders, expected):
    """14. A restored device writes and A receives; A writes and the restored device receives."""
    f.write([create(ALPHA, "from-f", "restored-device")])
    expect(state, ALPHA, "from-f", "restored-device")
    wait("A to receive the write of F", lambda: converged((a,), state))
    require(relay.stream_entries(a.database, f.device_id()) == [1], "F did not sign entry 1 of its own stream")
    a.write([create(BETA, "from-a", "after-restore")])
    expect(state, BETA, "from-a", "after-restore")
    wait("F to receive the write of A", lambda: converged((f,), state))
    wait("F to upload", f.synced_up)
    print("14. the restored device wrote and A received it; A wrote and the restored device received it", flush=True)


def scale_snapshot(relay, a, j, phrase, delta):
    """Scale: size, capture, upload and restore time, and peak memory, with tens of thousands of records."""
    wait("A to upload", a.synced_up, seconds=1800)
    report = a.snapshot_now()
    largest = max(file["bytes"] for file in last_record(a)["manifest"]["files"])
    started = time.monotonic()
    restored = j.restore(phrase)
    seconds = round(time.monotonic() - started, 1)
    require(restored["snapshot"] is not None, f"J restore: {restored!r}")
    j.start()
    wait("J to hold every record of the scale run", lambda: j.has_schema(delta) and j.keys(delta) == a.keys(delta),
         seconds=600)
    require(j.records(delta) == a.records(delta), "J differs from A after the scale run")
    for name, peak in (("A", a.peak_memory_bytes()), ("restore", restored["peak_memory_bytes"]),
                       ("J", j.peak_memory_bytes())):
        require(peak <= MEMORY_BOUND_BYTES, f"{name}: peak memory {peak // MEBIBYTE} MiB passed its bound")
    out = {"snapshot": report, "largest_file_bytes": largest, "restore_seconds": seconds,
           "restore_report": restored["snapshot"], "restore_peak_memory_bytes": restored["peak_memory_bytes"],
           "daemon_a_peak_memory_bytes": a.peak_memory_bytes(), "daemon_j_peak_memory_bytes": j.peak_memory_bytes()}
    print(f"   scale snapshot: {report['files']} files, {report['snapshot_bytes'] // MEBIBYTE} MiB, largest "
          f"{largest // MEBIBYTE} MiB; capture {report['capture_ms']} ms, upload {report['upload_ms']} ms; restore "
          f"{seconds}s, peak {restored['peak_memory_bytes'] // MEBIBYTE} MiB", flush=True)
    return out
