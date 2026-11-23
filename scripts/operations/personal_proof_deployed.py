"""The personal database proof against a deployed relay (the Exemem DEV relay), with a fresh identity.

`personal_database_proof.py --relay-url` runs this instead of the reference relay scenarios. The
identity is new on every run: `admin backup init` makes a seed offline, the personal database id
and the node key come from it, and the relay bills the account that the node key derives. No
account, API key or secret is involved. Each run therefore writes to a personal database of its
own and restores nothing of an earlier run.

What runs: A writes singles, batches, an alternate key and a file in three schemas; B restores
from the phrase alone and equals A; A and B edit one record apart (both offline, in both orders
of syncing) and converge by edit time; A snapshots and writes more; C restores from the phrase
alone, installs the snapshot and replays only the tail; C and A exchange writes; a snapshot that
uploads every file again is accepted (the relay already holds each digest); D restores from the
newest snapshot. What does not run is in `SKIPPED`, with the reason: each needs the relay process.
"""
import hashlib
import json
import os
import time

from personal_proof_nodes import (
    ALPHA, BETA, ENVELOPE_OVERHEAD, FIELDS, FILE_MARK, GAMMA, NAME_MARK, SCHEMAS, DeployedRelay, converged,
    create, delete, expect, replace, wait)
from personal_proof_deployed_trim import after_trim, before_trim
from personal_proof_snapshot import last_record, views, write_after
from personal_proof_steps import gamma_create, gamma_replace, gamma_view
from record_runs import RANGED, definition, require, system_seed

SYNC_SECONDS = 300
SKIPPED = (
    ("relay request log: idle turns read no entry, a restore reads only the tail by count of relay reads",
     "the deployed relay has no request log we can read; the tail is checked by the restore report"),
    ("reads of a restored home with the relay stopped", "the relay cannot be stopped"),
    ("the relay holds one signed entry for each write, by listing its files",
     "the relay's storage cannot be listed; the spool, the confirmed position and a restore are checked instead"),
    ("no key, value, schema name or marker in any byte the relay stored", "the relay's stored bytes cannot be read"),
    ("the relay holds no entry at or before the positions of the retained older snapshot, the floors, and "
     " the publisher's quota counter after it",
     "the relay's storage and the account's counter cannot be read; the snapshots of this run call `log/trim`, and "
     "the restores that follow (D restores after two trims) are checked"),
    ("restore by full replay from a relay copy with no snapshot", "the relay cannot be copied"),
    ("a restore killed at several points", "kept to the reference relay run: it adds nothing about the relay"),
    ("a deleted record stays deleted when an older edit arrives after", "kept to the reference relay run"),
    ("a 429 that says try again is tried again", "the deployed relay cannot be told to refuse a request"),
    ("a slow upload completes and a stalled one fails", "the deployed relay cannot be told to read slowly or to stop"),
    ("a closed upload connection is retried and a 400 is not", "the deployed relay cannot be told to close a connection"),
    ("kill -9 under write load and the scale run", "kept to the reference relay run: too much data for a shared DEV relay"),
)


def seconds_since(start):
    return round(time.monotonic() - start, 2)


def folders_of(devices):
    found = {}
    for device in devices:
        found[device.name] = device.out / "file-dir"
        found[device.name].mkdir(mode=0o700, exist_ok=True)
    return found


# 1. A writes ------------------------------------------------------------------------------

def a_writes(a, state, out):
    """Singles, batches, an alternate key and a file in three schemas. Returns what A must read and the timings."""
    started = time.monotonic()
    for schema in SCHEMAS:
        system_seed(a.node, definition(schema, "Hash", FIELDS, description_prefix="Personal proof "))
    system_seed(a.node, definition(GAMMA, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Personal proof "))
    for call in ([create(ALPHA, "a1", "one")], [create(ALPHA, "a2", "one")],
                 [create(ALPHA, "b1", "base"), create(ALPHA, "b2", "base"), create(ALPHA, "b3", "base"),
                  create(BETA, "c1", "one")]):
        a.write(call)
    for batch in range(3):
        keys = [f"bulk-{batch}-{n}" for n in range(100)]
        a.write([create(ALPHA, key, "bulk") for key in keys])
        for key in keys:
            expect(state, ALPHA, key, "bulk")
    a.change([replace(ALPHA, "a1", "two")])
    a.change([delete(ALPHA, "a2")])
    for key, text in (("a1", "two"), ("b1", "base"), ("b2", "base"), ("b3", "base")):
        expect(state, ALPHA, key, text)
    expect(state, BETA, "c1", "one")
    content = FILE_MARK.encode() + os.urandom(200_000)
    source = out / "source-files"
    source.mkdir(mode=0o700)
    (source / "attachment.bin").write_bytes(content)
    a.write([gamma_create("g1", "grp-x", "g1-one")])
    a.write([gamma_create("g2", "grp-x", "g2-one"), gamma_create("g3", "grp-y", "g3-one")])
    attached = {"path": str(source / "attachment.bin"), "name": f"{NAME_MARK}.bin", "media_type": "application/x-proof"}
    a.write([gamma_create("f1", "grp-f", "f1-one", file=attached)])
    a.change([gamma_replace("g1", "grp-y", "g1-two")])
    a.change([{"schema": GAMMA, "op": "delete", "key": {"hash": "g3"}}])
    a.change([gamma_replace("f1", "grp-f", "f1-two")])
    expected = {
        "groups": {"grp-x": ["g2"], "grp-y": ["g1"], "grp-f": ["f1"]},
        "records": {"g1": {"id": "g1", "note": "g1-two"}, "g2": {"id": "g2", "note": "g2-one"}, "g3": None,
                    "f1": {"id": "f1", "note": "f1-two"}},
        "file": {"name": f"{NAME_MARK}.bin", "media_type": "application/x-proof", "size": len(content),
                 "sha256": hashlib.sha256(content).hexdigest()},
    }
    require(a.snapshot() == state, "A does not hold what it wrote")
    written = seconds_since(started)
    drained = time.monotonic()
    wait("A to upload every entry", a.synced_up, seconds=SYNC_SECONDS)
    signed = a.spool_state()["head"]
    require(a.checkpoint()["writer_seq"] == signed, f"A confirmed {a.checkpoint()!r}, expected {signed}")
    require(a.spool_bytes()[1] == 0, "A's spool still holds entries after the relay confirmed them")
    print(f"1. A wrote {signed} signed entries (three schemas, an alternate key, a {len(content)} byte file); the "
          f"relay confirmed every one {seconds_since(drained)}s after the last write", flush=True)
    return expected, {"entries": signed, "write_seconds": written, "first_sync_seconds": seconds_since(drained),
                      "file_blob_bytes": len(content) + ENVELOPE_OVERHEAD}


# 2. B from the phrase ---------------------------------------------------------------------

def b_restores(a, b, phrase, state, expected, folders, signed):
    started = time.monotonic()
    restored = b.restore(phrase)
    seconds = seconds_since(started)
    require(restored["devices"] == 1 and restored["replayed"] == signed and restored["snapshot"] is None,
            f"B restore: {restored!r}, expected 1 device and {signed} entries")
    b.start()
    require(b.database == a.database, "B has another personal database id than A")
    wait("B to hold every record", lambda: views((b,), state, folders, expected), seconds=SYNC_SECONDS)
    print(f"2. B restored from the phrase alone in {seconds}s: 1 device stream, {signed} entries replayed, every "
          "record, alternate key and the file", flush=True)
    return {"restore_seconds": seconds, "report": {k: restored[k] for k in ("devices", "replayed", "peak_memory_bytes")}}


# 3. Edit apart ----------------------------------------------------------------------------

def edit_apart(relay, a, b, state):
    """A edits earlier and B later, both cut off from the relay. They converge on B's edit in both orders of syncing."""
    outcomes = {}
    for key, label, first, other in (("order-first", "earlier edit syncs first", a, b),
                                     ("order-last", "earlier edit syncs last", b, a)):
        a.write([create(ALPHA, key, "base")])
        expect(state, ALPHA, key, "base")
        wait("B to hold the base", lambda: converged((b,), state), seconds=SYNC_SECONDS)
        for device in (a, b):
            device.point_at(relay.OFFLINE_URL)
        a.change([replace(ALPHA, key, "from-A-earlier")])
        time.sleep(1.5)
        b.change([replace(ALPHA, key, "from-B-later")])
        first.point_at(relay.url)
        wait(f"{first.name} to upload", first.synced_up, seconds=SYNC_SECONDS)
        other.point_at(relay.url)
        wait(f"{other.name} to upload", other.synced_up, seconds=SYNC_SECONDS)
        expect(state, ALPHA, key, "from-B-later")
        wait(f"both to hold the later edit ({label})", lambda: converged((a, b), state), seconds=SYNC_SECONDS)
        outcomes[label] = "from-B-later"
    print("3. A and B edited one record apart, cut off from the relay: the later edit won whichever device synced "
          "first", flush=True)
    return outcomes


# 4. Snapshot and the tail -----------------------------------------------------------------

def snapshot_then_tail(a, b, state, expected, folders):
    """A snapshots, then writes more in all three schemas. Returns the snapshot report and the entries after it."""
    wait("A and B to upload", lambda: a.synced_up() and b.synced_up(), seconds=SYNC_SECONDS)
    wait("A to hold what B wrote", lambda: converged((a,), state), seconds=SYNC_SECONDS)
    started = time.monotonic()
    first = a.snapshot_now()
    snapshot_seconds = seconds_since(started)
    require(first["snapshot_seq"] == 1 and first["uploaded_files"] == first["files"] and first["files"] >= 5,
            f"first snapshot: {first!r}")
    at_snapshot = a.spool_state()["head"]
    manifest = len(json.dumps(last_record(a)["manifest"], separators=(",", ":")))
    write_after(a, state, expected)
    require(gamma_view(a, folders[a.name]) == expected, "A does not read what it wrote after the snapshot")
    wait("A to upload the writes after the snapshot", a.synced_up, seconds=SYNC_SECONDS)
    tail = a.spool_state()["head"] - at_snapshot
    wait("B to hold them", lambda: views((b,), state, folders, expected), seconds=SYNC_SECONDS)
    print(f"4. A snapshotted ({first['files']} files, {first['snapshot_bytes']} bytes, capture {first['capture_ms']} ms, "
          f"upload {first['upload_ms']} ms, {snapshot_seconds}s with the wait) and wrote {tail} more entries", flush=True)
    return first, tail, {"snapshot_wall_seconds": snapshot_seconds, "manifest_bytes": manifest}


# 5. C restores from the snapshot ----------------------------------------------------------

def c_restores(a, b, c, phrase, state, expected, folders, tail):
    started = time.monotonic()
    restored = c.restore(phrase)
    seconds = seconds_since(started)
    snapshot = restored["snapshot"]
    require(snapshot and snapshot["snapshot_seq"] == 1, f"C restore: {restored!r}")
    require(restored["replayed"] == tail, f"C replayed {restored['replayed']} entries, the tail is {tail}")
    require(restored["devices"] >= 2, f"C found {restored['devices']} device streams, expected the streams of A and B")
    c.start()
    wait("C to equal A and B", lambda: views((a, b, c), state, folders, expected), seconds=SYNC_SECONDS)
    for schema in SCHEMAS:
        require(c.records(schema) == a.records(schema) == b.records(schema), f"{schema} differs")
    print(f"5. C restored from the phrase alone in {seconds}s: installed the snapshot (seq 1, {snapshot['files']} "
          f"files), replayed {tail} entries, {restored['devices']} device streams listed; equals A and B, "
          "alternate keys and file bytes included", flush=True)
    return {"restore_seconds": seconds, "snapshot": snapshot, "replayed": restored["replayed"],
            "devices": restored["devices"], "peak_memory_bytes": restored["peak_memory_bytes"]}


def exchange(a, c, state):
    """A write of the restored device reaches A, and a write of A reaches it."""
    c.write([create(ALPHA, "from-c", "restored-device")])
    expect(state, ALPHA, "from-c", "restored-device")
    wait("A to receive the write of C", lambda: converged((a,), state), seconds=SYNC_SECONDS)
    a.write([create(BETA, "from-a", "after-restore")])
    expect(state, BETA, "from-a", "after-restore")
    wait("C to receive the write of A", lambda: converged((c,), state), seconds=SYNC_SECONDS)
    wait("C to upload", c.synced_up, seconds=SYNC_SECONDS)
    print("6. C wrote and A received it; A wrote and C received it", flush=True)


# 7. A snapshot that uploads every file again ----------------------------------------------

def snapshot_again(relay, a, d, phrase, state, expected, folders):
    """An incremental snapshot, then one that has forgotten the last: it puts every file again.

    The relay already holds each digest, answers `begin` with no upload, and the node counts it as stored.
    """
    wait("A to upload", a.synced_up, seconds=SYNC_SECONDS)
    early = before_trim(relay, a)
    second = a.snapshot_now()
    trimmed = after_trim(relay, a, early)
    require(second["snapshot_seq"] == 2, f"second snapshot: {second!r}")
    (a.log_dir() / "snapshot-last.json").unlink()
    third = a.snapshot_now()
    require(third["snapshot_seq"] == 3 and third["uploaded_files"] == third["files"],
            f"the snapshot that puts every file again: {third!r}")
    started = time.monotonic()
    restored = d.restore(phrase)
    seconds = seconds_since(started)
    require(restored["snapshot"]["snapshot_seq"] == 3 and restored["replayed"] == 0, f"D restore: {restored!r}")
    require(restored["devices"] >= 3, f"D found {restored['devices']} device streams, expected A, B and C")
    d.start()
    wait("D to equal A", lambda: views((d,), state, folders, expected), seconds=SYNC_SECONDS)
    print(f"7. a snapshot that put all {third['files']} files again was accepted (the relay already held them); D "
          f"restored from snapshot 3 in {seconds}s, replayed nothing, {restored['devices']} device streams listed",
          flush=True)
    return second, third, {"restore_seconds": seconds, "devices": restored["devices"], "after_trim": trimmed}


# Run --------------------------------------------------------------------------------------

def prove_deployed(relay, devices, out):
    """Run every step against `relay`. Returns the report that `render` turns into the job summary."""
    a, b, c, d = devices
    require(isinstance(relay, DeployedRelay), "this proof runs against a deployed relay")
    report = {"relay": relay.url, "steps": []}
    state = {}

    def step(name, function, *arguments):
        started = time.monotonic()
        result = function(*arguments)
        report["steps"].append({"step": name, "seconds": seconds_since(started)})
        return result

    phrase = a.init()
    a.start()
    # For an owner who has not written anywhere, the relay answers 403 to a list of the streams. That is an empty relay.
    time.sleep(6)
    require("personal database sync failed" not in "\n".join(a.warnings(all_lines=True)),
            "A's first turns with an empty relay failed")
    folders = folders_of(devices)
    expected, report["a_writes"] = step("A writes", a_writes, a, state, out)
    signed = report["a_writes"]["entries"]
    report["b_restore"] = step("B restores from the phrase", b_restores, a, b, phrase, state, expected, folders, signed)
    report["edit_apart"] = step("edit apart, both orders", edit_apart, relay, a, b, state)
    first, tail, extra = step("snapshot and tail", snapshot_then_tail, a, b, state, expected, folders)
    report["snapshot"] = {**first, **extra, "tail_entries": tail}
    report["c_restore"] = step("C restores from the snapshot", c_restores, a, b, c, phrase, state, expected, folders, tail)
    step("C and A exchange writes", exchange, a, c, state)
    second, third, report["d_restore"] = step("snapshot again, D restores", snapshot_again, relay, a, d, phrase, state,
                                              expected, folders)
    report["second_snapshot"], report["third_snapshot"] = second, third
    report["entries_signed"] = sum(device.spool_state()["head"] for device in (a, b, c))
    # Bytes the relay was given that it had not held: the new blobs. Entries are counted, not measured.
    report["blob_bytes_added"] = (report["a_writes"]["file_blob_bytes"] + first["uploaded_bytes"]
                                  + second["uploaded_bytes"] + extra["manifest_bytes"] * 3)
    report["skipped"] = [{"step": name, "reason": reason} for name, reason in SKIPPED]
    (out / "summary.json").write_text(json.dumps(report, indent=2))
    return report


def render(report):
    """The job summary, in Markdown: what ran, what was skipped and why, timings and bytes."""
    snap, a, c = report["snapshot"], report["a_writes"], report["c_restore"]
    lines = ["## Personal database on the deployed Exemem DEV relay", "",
             "A fresh identity for this run (seed made offline, no account or secret). Relay origin: the DEV relay of "
             "`lastdb_node/environments.json`.", "", "### Ran", "", "The relay cannot be stopped, so the edit-apart step cuts a device off by restarting it with an "
             "unreachable relay address.", "", "| Step | Seconds |", "|---|---|"]
    lines += [f"| {item['step']} | {item['seconds']} |" for item in report["steps"]]
    lines += ["", "### Skipped (each needs the relay process or its disk)", "", "| Step | Reason |", "|---|---|"]
    lines += [f"| {item['step']} | {item['reason']} |" for item in report["skipped"]]
    lines += ["", "### Timings", "",
              f"- First sync: {a['entries']} entries written in {a['write_seconds']}s, the relay confirmed the last "
              f"{a['first_sync_seconds']}s after the final write.",
              f"- Snapshot: capture {snap['capture_ms']} ms, upload {snap['upload_ms']} ms, "
              f"{snap['snapshot_wall_seconds']}s from request to named ({snap['files']} files, {snap['snapshot_bytes']} bytes).",
              f"- Restore from entries alone (B): {report['b_restore']['restore_seconds']}s.",
              f"- Restore from the snapshot (C): {c['restore_seconds']}s, {c['replayed']} entries replayed, snapshot download "
              f"{c['snapshot'].get('download_ms')} ms.",
              f"- Restore from snapshot 3 (D): {report['d_restore']['restore_seconds']}s.", "",
              "### Added to the relay (it keeps all of it; a node cannot delete)", "",
              f"- Blobs: about {report['blob_bytes_added']} bytes (the sealed file, the new snapshot files, three manifests).",
              f"- Signed entries: {report['entries_signed']} data entries, plus a birth and three snapshot entries. "
              "The size of an entry is not visible to the client.", ""]
    return "\n".join(lines)
