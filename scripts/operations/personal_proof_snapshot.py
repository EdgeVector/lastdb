"""Steps 11 to 14 of the personal database proof: the snapshot as the checkpoint of the signed log.

Device A takes a snapshot, which uploads the native files of its store and a `snapshot` entry on the
snapshot stream, and then writes more. A home restored from the recovery phrase installs the snapshot
and reads only the entries after it. The proof checks that against a full replay, kills the restore
at several points, checks that a second snapshot uploads only the files that the relay lacks, that the
relay cannot read the snapshot, that a restored device and A exchange writes, and measures the scale.

The relay keeps one snapshot. Each snapshot asks the relay to drop the entries it covers and the older
snapshot entries, and a sweep then drops the files that nothing names. So every running device reads up
to the cut before A takes a snapshot, and the full replay runs against a copy of the relay that was made
before the first snapshot and given the later entries.
"""
import hashlib
import json
from pathlib import Path
import shutil
import time

from personal_proof_nodes import (
    ALPHA, BETA, GAMMA, MEBIBYTE, MEMORY_BOUND_BYTES, MARK, converged, create, delete, expect, relay_files,
    relay_holds_none, replace, wait)
from personal_proof_status import prove_installed, prove_taken, swept
from personal_proof_steps import gamma_replace, gamma_seen, gamma_view
from record_runs import require
import v1_records as v1

SNAP_MARK = "SNAPMARK-7d41c0e9b2"
KILL_DELAYS = (0.15, 0.5, 1.0, 1.6, 2.4)


def stream_dirs(relay, database):
    """Every device stream directory of the relay: logs/<db>/<node>/<device>."""
    return sorted(path for path in (relay.data / "logs" / database).glob("*/*") if path.is_dir())


def stored_seqs(directory):
    """The sequence numbers of the entries a stream directory holds, ascending. `floor.json` is not one."""
    return sorted(int(path.stem) for path in directory.glob("*.json") if path.stem.isdigit())


def stream_kind(directory):
    """The kind of the first stored entry of a stream, or None when it holds none."""
    seqs = stored_seqs(directory)
    return json.loads((directory / f"{seqs[0]}.json").read_text())["kind"] if seqs else None


def snapshot_stream(relay, database):
    """The directory of the snapshot stream, or None. Its entries are `snapshot` entries."""
    for path in stream_dirs(relay, database):
        if stream_kind(path) == "snapshot":
            return path
    return None


def newest_snapshot_entry(relay, database):
    """The newest `snapshot` entry on the relay, as the JSON the relay stored."""
    directory = snapshot_stream(relay, database)
    require(directory is not None, "the relay holds no snapshot stream")
    newest = stored_seqs(directory)[-1]
    return newest, json.loads((directory / f"{newest}.json").read_text())


def tail_entries(relay, database, entry):
    """Entries the relay holds after the positions the snapshot entry names, and the length of the history.

    A snapshot drops the entries it covers, so the history is counted by the last entry of each stream.
    """
    through = {position["device_id"]: position["writer_seq"] for position in entry["through"]}
    tail = total = 0
    for path in stream_dirs(relay, database):
        if stream_kind(path) in ("snapshot", "birth"):
            continue
        seqs = stored_seqs(path)
        position = through.get(path.name, 0)
        head = seqs[-1] if seqs else position
        total += head
        tail += head - position
    return tail, total


def copy_later(relay, copy, database):
    """Give `copy`, a copy of the relay's files from before the first snapshot, what the relay stored since.

    The snapshot stream and the floors that a trim wrote are left out, so `copy` holds every stream from its
    first entry and no snapshot: a restore from it replays the whole history.
    """
    snapshots = snapshot_stream(relay, database).name
    for path in relay_files(relay):
        relative = path.relative_to(relay.data)
        target = copy.data / relative
        if relative.parts[0] == "staging" or path.name == "floor.json" or snapshots in relative.parts or target.exists():
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)


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
    wait("A and B to upload", lambda: a.synced_up() and b.synced_up())
    # The snapshot drops the entries it covers: B reads up to the cut first, and the copy for the full
    # replay is made while the relay still holds every entry.
    wait("B to hold what A wrote before the snapshot", lambda: converged((b,), state))
    shutil.copytree(relay.data, relay2.data)
    first = a.snapshot_now()
    require(first["uploaded_files"] == first["files"] and first["files"] >= 5, f"first snapshot: {first!r}")
    published = prove_taken(a, first, newest_snapshot_entry(relay, a.database), relay, "published")
    swept(a, 1)
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
    # The same data by full replay: a relay with no snapshot stream, which holds every entry.
    copy_later(relay, relay2, a.database)
    relay2.start()
    replayed = e.restore(phrase)
    require(replayed["snapshot"] is None and replayed["replayed"] == total, f"E restore: {replayed!r}")
    f.start()
    e.start()
    prove_installed(f, published)
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
    folders[g.name] = g.out / "file-dir"
    folders[g.name].mkdir(mode=0o700, exist_ok=True)
    for delay in KILL_DELAYS:
        finished = g.restore_killed(phrase, delay)
        home = g.node.home
        if home.exists():
            require((home / "data" / "laststore-pack-node-v1").is_file(),
                    f"a restore killed after {delay}s left an incomplete home")
            if not finished:
                # The home appears in one rename, when it is complete. This kill came after the
                # rename and before the command ended, so the home must be the complete one.
                g.start()
                wait(f"the home that a restore killed after {delay}s left to equal A",
                     lambda: views((g,), state, folders, expected))
                g.stop()
            shutil.rmtree(home)
        outcomes.append({"delay": delay, "finished_first": finished})
    g.restore(phrase)
    g.start()
    wait("the rerun restore to equal A", lambda: views((g,), state, folders, expected))
    g.stop()
    print(f"12. restore killed after {KILL_DELAYS} seconds: no home or a complete one each time; the rerun "
          "completed and equals A", flush=True)
    return outcomes


def second_snapshot(relay, a, b, f, h, phrase, state, folders, expected):
    """13. A second snapshot uploads only the files the relay lacks, replaces the first on the relay, and the relay cannot read it.

    Returns the report, and the last four entries of A before the snapshot (the trim drops them).
    """
    a.write([create(ALPHA, "second-1", "x"), create(ALPHA, "second-2", "x")])
    expect(state, ALPHA, "second-1", "x")
    expect(state, ALPHA, "second-2", "x")
    wait("A to upload", a.synced_up)
    # The other running devices read the two new entries now: the snapshot drops them from the relay, and
    # their reads are not counted for H.
    wait("B and F to hold the two records", lambda: converged((b, f), state))
    directory = next(path for path in stream_dirs(relay, a.database) if path.name == a.device_id())
    kept = {seq: json.loads((directory / f"{seq}.json").read_text()) for seq in stored_seqs(directory)[-4:]}
    before = {file["sha256"] for file in last_record(a)["manifest"]["files"]}
    mark = len(relay.lines())
    second = a.snapshot_now()
    uploaded = uploads_since(relay, mark)
    prove_taken(a, second, newest_snapshot_entry(relay, a.database), relay, "published")
    require(second["snapshot_seq"] == 2 and 0 < second["uploaded_files"] < second["files"], f"second snapshot: {second!r}")
    require(uploaded == second["uploaded_files"] + 1, f"the relay took {uploaded} uploads, expected files + manifest")
    swept(a, 2)
    mark = len(relay.lines())
    restored = h.restore(phrase)
    require(restored["snapshot"]["snapshot_seq"] == 2 and restored["replayed"] == 0, f"H restore: {restored!r}")
    require(read_lines(relay, mark, "log/read") <= 1, "H read entries beyond the snapshot entry")
    h.start()
    folders[h.name] = h.out / "file-dir"
    folders[h.name].mkdir(mode=0o700, exist_ok=True)
    wait("H to equal A", lambda: views((h,), state, folders, expected))
    # The relay holds the files of the newest snapshot, by digest, and none of them holds a marker. The files
    # that only the first snapshot named are gone.
    digests = {file["sha256"] for file in last_record(a)["manifest"]["files"]}
    held = {hashlib.sha256(path.read_bytes()).hexdigest() for path in relay_files(relay)}
    require(digests <= held, "the relay lacks a file of the newest snapshot")
    gone = before - digests
    require(gone and not gone & held, f"the relay still holds {len(gone & held)} of {len(gone)} files that only snapshot 1 named")
    require(stored_seqs(snapshot_stream(relay, a.database)) == [2], "the relay holds another snapshot entry than the newest")
    relay_holds_none(relay, [SNAP_MARK, "FILEMARK-9f3c2a71d4", GAMMA, "grp-x"])
    print(f"13. second snapshot uploaded {second['uploaded_files']} of {second['files']} files; the relay dropped snapshot 1 "
          f"and the {len(gone)} files that only it named; H restored from snapshot 2 and replayed nothing; no marker in "
          "any relay byte, snapshot files included", flush=True)
    return {"second_snapshot": second, "restore_h": restored["snapshot"], "files_dropped_with_snapshot_1": len(gone)}, kept


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
    swept(a, report["snapshot_seq"], seconds=600)
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
