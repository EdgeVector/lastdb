"""Step 23 of the personal database proof: the cloud does not grow beyond the local store.

After a snapshot is named, the node asks the relay to drop what it covers (`log/trim` of the newest
snapshot) and then sweeps: it lists the stored files (`blob/list`) and drops every one that the newest
snapshot and the entries after it do not name (`blob/drop`). The relay then holds one snapshot, the
entries after its positions and the files they name, and nothing else.

`cloud_rounds` runs rounds on one writer and the other devices of its owner. Round 1 writes records and
files. Each later round overwrites every record, deletes a tenth of them and makes them again, and
replaces every file, with values of the same length, so the data does not grow. One other device replaces
a file of its own each round. Every device reads the round, then the writer takes a snapshot. After each
round, once the sweep is done, the proof asks the relay through its routes, signed as the writer
(`log/streams`, `log/read` of every stream from its floor, `blob/list`, `blob.get` of the manifest), so
the same checks run against the reference relay and against a deployed one:

1. Exact. The relay holds one snapshot entry. Its stored files are the files of that manifest (one for
   each digest), the manifest, and the sealed files that stored entries name, each with its length,
   and no other file. So the stored bytes are the manifest's file bytes plus the manifest length plus
   those sealed bytes.
2. Not above the local store. Stored files plus stored entries are at most the local store's data files
   plus an allowance: the manifest, the stored entries, 29 bytes for each sealed file (its envelope) and
   SLACK_BYTES. The data files are the native files of the writer's store on disk and the files that its
   records hold by path (`data/local-file-refs.json`; a snapshot seals those into its own copy of the
   file store, so the relay holds them and the store's directory does not). The native files are measured before the snapshot, when
   two listings a second apart are equal, and again after the sweep. The larger of the two is used,
   because the store may rewrite a file between a measurement and the cut.
3. No growth without data. From the first round to the last, the relay grew by no more than the local
   files did (the last round's larger measurement less the first round's smaller one, or nothing), plus
   what the allowance grew and SLACK_BYTES.
4. The sweep dropped. Each round the relay released at least the sealed files that the round's entries
   named, and from the second round on the manifest of the snapshot before.

After the last round the writer attaches one more file. A sweep asked with `lastdb backup reclaim --wait`
keeps it: it is the one sealed file on the relay. A `blob/drop` that the proof signs itself and that leaves
one stream of the publisher out of `heads` is answered 409 `heads` and drops nothing
(`personal_proof_blobs.py`). A home restored from the recovery phrase then installs
the last snapshot, replays that one entry and equals the writer, file included.

The node's own account must say what the relay says: the report of `lastdb backup reclaim --wait`, the
`cloud_sync.reclaim` object of `/api/status`, and the `Cloud storage:` line of `lastdb status`. On the
reference relay the proof also reads the relay's disk: every stored object is one the database owns, and
no pending record of an upload is left after its commit.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil

from personal_proof_blobs import list_blobs, omitted_stream_refused
from personal_proof_deployed_trim import node_hex
from personal_proof_cleanup import cleanup_owned
from personal_proof_nodes import ALPHA, ENVELOPE_OVERHEAD, FIELDS, Device, Relay, create, wait
from personal_proof_reconcile import get_blob, read_stream, uploaded
from personal_proof_status import reclaim_now, status_lines, storage_line, swept
from personal_proof_trim import relay_post
from record_runs import RANGED, definition, require, system_seed
import v1_records as v1

SWEEP = "proofmarkersweep"
ROUNDS = 4
RECORDS, NOTE_BYTES = 200, 1500
FILES, FILE_BYTES = 2, 120_000
# What the store may rewrite between a measurement of its files and the cut of a snapshot, what the
# snapshot's sealed copy of the files held by path adds to them, and what the encoding of an entry or
# a manifest may differ by between two rounds.
SLACK_BYTES = 64 * 1024
SYNC_SECONDS = 300
GROUP = "grp-s"


def record(key, text, file=None):
    return v1.create_item(SWEEP, {"hash": key}, {"id": key, "note": text},
                          alternates={"by_group": {"hash": GROUP, "range": key}}, file=file)


def replace(key, text):
    return {"schema": SWEEP, "op": "replace", "key": {"hash": key}, "record": {"id": key, "note": text},
            "alternates": {"by_group": {"hash": GROUP, "range": key}}}


def remove(key):
    return {"schema": SWEEP, "op": "delete", "key": {"hash": key}}


def attach(device, source, key, text, first):
    """Give the record `key` a new file of FILE_BYTES random bytes. A record that exists is made again."""
    path = source / f"{device.name}-{key}-{text}.bin"
    path.write_bytes(os.urandom(FILE_BYTES))
    if not first:
        device.change([remove(key)])
    device.write([record(key, text, file={"path": str(path), "name": f"{key}.bin", "media_type": "application/x-proof"})])
    return path


def write_round(writer, other, source, number):
    """One round. Returns what every device must hold afterwards: key -> fields."""
    keys = [f"r{index:04d}" for index in range(RECORDS)]
    texts = {key: f"{number:02d}-" + os.urandom(NOTE_BYTES // 2).hex() for key in keys}
    first = number == 1
    for start in range(0, RECORDS, 100):
        batch = keys[start:start + 100]
        if first:
            writer.write([record(key, texts[key]) for key in batch])
        else:
            writer.change([replace(key, texts[key]) for key in batch])
    if not first:
        again = keys[::10]
        writer.change([remove(key) for key in again])
        writer.write([record(key, texts[key]) for key in again])
    expected = {key: {"id": key, "note": text} for key, text in texts.items()}
    for index in range(FILES):
        attach(writer, source, f"f{index}", f"file-{number:02d}", first)
        expected[f"f{index}"] = {"id": f"f{index}", "note": f"file-{number:02d}"}
    wait(f"{other.name} to hold the schema", lambda: other.has_schema(SWEEP), seconds=SYNC_SECONDS)
    attach(other, source, "o0", f"other-{number:02d}", first)
    expected["o0"] = {"id": "o0", "note": f"other-{number:02d}"}
    return expected


def holds(device, expected):
    try:
        return device.records(SWEEP) == expected
    except RuntimeError:
        return False


def settle(devices, expected, what):
    wait(f"{what}: every device to hold it", lambda: all(holds(device, expected) for device in devices), seconds=SYNC_SECONDS)
    wait(f"{what}: every device to upload", lambda: all(uploaded(device) for device in devices), seconds=SYNC_SECONDS)


def local_files(device):
    """The native files of the store on disk: path -> length. A snapshot takes these kinds of files."""
    data = device.node.home / "data"
    paths = [data / "laststore-pack-node-v1"]
    for root in ("disk-run", "disk-cas"):
        paths += [data / root / "CURRENT", data / root / "laststore-run-home-v2", *(data / root / "runs").glob("*.ldrun")]
    found = {}
    for path in paths:
        try:
            found[str(path.relative_to(data))] = path.stat().st_size
        except OSError:
            continue
    return found


def by_path_bytes(device):
    """The length of the files that records of this home hold by path (`data/local-file-refs.json`)."""
    try:
        catalog = json.loads((device.node.home / "data" / "local-file-refs.json").read_text())
    except (OSError, ValueError):
        return 0
    return sum(entry["size_bytes"] for entry in catalog["files"].values())


def settled_local(device):
    """The native files once two listings a second apart are equal: the store is not rewriting them."""
    last = [local_files(device)]

    def probe():
        now, before = local_files(device), last[0]
        last[0] = now
        return now if now == before else None
    return wait(f"the store of {device.name} to stop rewriting its files", probe, seconds=120)


def released_blobs(device):
    try:
        return json.loads((device.log_dir() / "cloud-usage.json").read_text()).get("released_blobs", 0)
    except (OSError, ValueError):
        return 0


# What the relay holds, through its routes -------------------------------------------------

def relay_view(relay, device):
    """What the relay holds of the database of `device`: its one snapshot, its entries and its stored files."""
    database, node = device.database, node_hex(device)
    status, answer = relay_post(relay, device, database, node, "/relay/v1/log/streams",
                                {"database_id": database, "writer_key": node})
    require(status == 200, f"log/streams: {status} {answer!r}")
    entries = []
    for item in answer["streams"]:
        entries += read_stream(relay, device, database, node, item["device_id"], item["head_seq"])[1]
    snapshots = [entry for entry in entries if entry["kind"] == "snapshot"]
    require(len(snapshots) == 1, f"the relay holds {len(snapshots)} snapshot entries, expected the newest alone")
    snapshot = snapshots[0]
    manifest = json.loads(get_blob(relay, device, database, node, snapshot["manifest_sha256"],
                                   snapshot["manifest_bytes"], "the manifest"))["files"]
    return {"snapshot": snapshot, "manifest": manifest, "files": {file["sha256"]: file["bytes"] for file in manifest},
            "named": {entry["file"]["sha256"] for entry in entries if entry.get("file")}, "entries": len(entries),
            "entry_bytes": sum(len(json.dumps(entry, separators=(",", ":"), ensure_ascii=False).encode()) for entry in entries),
            "blobs": list_blobs(relay, device, node)}


def check_exact(label, view):
    """1. The stored files are exactly what the snapshot and the stored entries name. Returns the stored and the sealed bytes."""
    snapshot, files, blobs = view["snapshot"], view["files"], view["blobs"]
    sealed = view["named"] - set(files) - {snapshot["manifest_sha256"]}
    needed = {**dict.fromkeys(sealed), **files, snapshot["manifest_sha256"]: snapshot["manifest_bytes"]}
    extra, missing = sorted(set(blobs) - set(needed)), sorted(set(needed) - set(blobs))
    require(not extra, f"{label}: the relay holds {len(extra)} files ({sum(blobs[digest] for digest in extra)} bytes) that "
            f"neither the newest snapshot nor a stored entry names: {extra[:3]}")
    require(not missing, f"{label}: the relay lacks {len(missing)} files that the snapshot or a stored entry names: {missing[:3]}")
    wrong = [digest for digest, length in needed.items() if length is not None and blobs[digest] != length]
    require(not wrong, f"{label}: {len(wrong)} stored files have another length than the snapshot names: {wrong[:3]}")
    stored, sealed_bytes = sum(blobs.values()), sum(blobs[digest] for digest in sealed)
    require(stored == sum(files.values()) + snapshot["manifest_bytes"] + sealed_bytes,
            f"{label}: the relay stores {stored} bytes of files, not the manifest's files, the manifest and the sealed files")
    return stored, sealed, sealed_bytes


def check_node(label, device, view, stored, sealed, sealed_bytes, report):
    """The node's account of the sweep says what the relay says, in the report, the status object and the status line."""
    expected = {"snapshot_seq": view["snapshot"]["writer_seq"], "snapshot_files": len(view["manifest"]),
                "snapshot_bytes": sum(file["bytes"] for file in view["manifest"]), "snapshot_blobs": len(view["files"]),
                "snapshot_blob_bytes": sum(view["files"].values()), "manifest_bytes": view["snapshot"]["manifest_bytes"],
                "named_files": len(sealed), "named_bytes": sealed_bytes, "missing_files": 0, "entries": view["entries"],
                "cloud_files": len(view["blobs"]), "cloud_bytes": stored, "message": None}
    require({name: report.get(name) for name in expected} == expected,
            f"{label}: `backup reclaim` reports {report!r}, the relay holds {expected!r}")
    shown = device.sync_status()["reclaim"]
    require({name: shown.get(name) for name in expected} == expected,
            f"{label}: /api/status shows {shown!r}, the relay holds {expected!r}")
    lines = [line for line in status_lines(device) if line.startswith("Cloud storage")]
    require(lines == [storage_line(expected)], f"{label}: status lines {lines!r}")


def check_disk(label, relay, database, blobs):
    """The reference relay's disk: the stored objects are the listed files, and no pending record is left."""
    root = relay.data / "blobs"
    objects = {path.name: path.stat().st_size for path in root.iterdir() if path.is_file()}
    owned = {path.name for path in (root / database).iterdir()}
    require(objects == blobs and owned == set(blobs),
            f"{label}: the relay's disk holds {len(objects)} objects and {len(owned)} owned digests, blob/list gave {len(blobs)}")
    staging = relay.data / "staging" / database
    pending = sorted(path.name for path in staging.iterdir()) if staging.is_dir() else []
    require(not pending, f"{label}: the relay kept {len(pending)} pending records of uploads after their commit: {pending[:3]}")


def measure(relay, writer, label, before):
    """Ask for a sweep now, then check 1 and 2 and the node's account. Returns the numbers of the round.

    `before` is the listing of the native files from before the snapshot. The files that the writer's
    records hold by path are added to both measurements.
    """
    report = reclaim_now(writer)
    after = local_files(writer)
    view = relay_view(relay, writer)
    stored, sealed, sealed_bytes = check_exact(label, view)
    check_node(label, writer, view, stored, sealed, sealed_bytes, report)
    if not relay.deployed:
        check_disk(label, relay, writer.database, view["blobs"])
    held = by_path_bytes(writer)
    sizes = sorted((sum(before.values()) + held, sum(after.values()) + held))
    manifest = view["snapshot"]["manifest_bytes"]
    allowance = manifest + view["entry_bytes"] + ENVELOPE_OVERHEAD * len(sealed)
    total = stored + view["entry_bytes"]
    require(total <= sizes[1] + allowance + SLACK_BYTES,
            f"{label}: the relay holds {total} bytes ({stored} of files, {view['entry_bytes']} of entries); the local store "
            f"holds {sizes[1]} bytes of files, and the allowance is {allowance} + {SLACK_BYTES}")
    return {"snapshot_seq": view["snapshot"]["writer_seq"], "relay_bytes": total, "relay_files": len(view["blobs"]),
            "relay_file_bytes": stored, "relay_entries": view["entries"], "relay_entry_bytes": view["entry_bytes"],
            "manifest_bytes": manifest, "sealed_files": len(sealed), "sealed_bytes": sealed_bytes,
            "local_bytes": sizes[1], "local_bytes_least": sizes[0], "allowance": allowance}


def check_growth(rounds):
    """3. From the first round to the last, the relay grew by no more than the local store did."""
    first, last = rounds[0], rounds[-1]
    grew = last["relay_bytes"] - first["relay_bytes"]
    local = max(0, last["local_bytes"] - first["local_bytes_least"])
    allowance = max(0, last["allowance"] - first["allowance"]) + SLACK_BYTES
    require(grew <= local + allowance,
            f"the relay grew by {grew} bytes from round 1 ({first['relay_bytes']}) to round {len(rounds)} "
            f"({last['relay_bytes']}) with data of the same size; the local files grew by {local}, the allowance is {allowance}")
    return {"relay_growth_bytes": grew, "local_growth_bytes": local, "allowance": allowance}


def restore_from_swept(relay, writer, device, phrase, expected, path):
    """A home restored from the phrase installs the last snapshot, replays the entry after it, and reads its file."""
    device.stop()
    if device.node.home.exists():
        shutil.rmtree(device.node.home)
    restored = device.restore(phrase)
    seq = writer.sync_status()["snapshot"]["snapshot_seq"]
    require(restored["snapshot"] and restored["snapshot"]["snapshot_seq"] == seq and restored["replayed"] == 1,
            f"{device.name} restore from the swept relay: {restored!r}, expected snapshot {seq} and one entry")
    device.start()
    wait(f"{device.name}, restored from the swept relay, to equal {writer.name}", lambda: holds(device, expected),
         seconds=SYNC_SECONDS)
    folder = device.out / "sweep-read"
    folder.mkdir(mode=0o700, exist_ok=True)
    item = v1.point(SWEEP, {"hash": "tail"}, file=True)
    item["file_dir"] = str(folder)
    rows = v1.read_items(device.node, [item])[0]["records"]
    require(rows and rows[0].get("file") and rows[0]["file"].get("path"), f"{device.name} reads no file for the last write: {rows!r}")
    require(hashlib.sha256(Path(rows[0]["file"]["path"]).read_bytes()).digest() == hashlib.sha256(path.read_bytes()).digest(),
            f"{device.name} reads another file than {writer.name} wrote after the last snapshot")
    return {"snapshot_seq": seq, "replayed": restored["replayed"]}


def cloud_rounds(relay, writer, others, phrase, out, rounds=ROUNDS):
    """23. Rounds of overwrites and snapshots: the relay holds what the newest snapshot and the entries after it name."""
    source = out / "sweep-files"
    source.mkdir(mode=0o700, exist_ok=True)
    if not writer.has_schema(SWEEP):
        system_seed(writer.node, definition(SWEEP, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Personal proof "))
    devices, measured = (writer, *others), []
    for number in range(1, rounds + 1):
        expected = write_round(writer, others[0], source, number)
        settle(devices, expected, f"round {number}")
        released = released_blobs(writer)
        before = settled_local(writer)
        snapshot = writer.snapshot_now()
        swept(writer, snapshot["snapshot_seq"], seconds=SYNC_SECONDS)
        numbers = measure(relay, writer, f"round {number}", before)
        require(numbers["snapshot_seq"] == snapshot["snapshot_seq"] and numbers["sealed_files"] == 0,
                f"round {number}: the relay holds {numbers!r} after snapshot {snapshot['snapshot_seq']}")
        # 4. The sealed files of the round's entries went, and from round 2 the manifest before.
        dropped = released_blobs(writer) - released
        least = FILES + 1 + (0 if number == 1 else 1)
        require(dropped >= least, f"round {number}: the sweep released {dropped} files, expected at least {least}")
        measured.append({**numbers, "released_files": dropped})
        print(f"   round {number}: the relay holds {numbers['relay_files']} files ({numbers['relay_file_bytes']} bytes) and "
              f"{numbers['relay_entries']} entries ({numbers['relay_entry_bytes']} bytes), exactly what snapshot "
              f"{numbers['snapshot_seq']} names; the local store holds {numbers['local_bytes']} bytes; the sweep released "
              f"{dropped} files", flush=True)
    growth = check_growth(measured)
    # A file that an entry after the cut names stays: it is the one sealed file on the relay.
    path = attach(writer, source, "tail", "after-the-cut", True)
    expected["tail"] = {"id": "tail", "note": "after-the-cut"}
    settle(devices, expected, "the write after the last snapshot")
    tail = measure(relay, writer, "after the last snapshot", local_files(writer))
    require(tail["sealed_files"] == 1 and tail["sealed_bytes"] == FILE_BYTES + ENVELOPE_OVERHEAD,
            f"the relay holds {tail['sealed_files']} sealed files ({tail['sealed_bytes']} bytes) for one file after the cut")
    omitted_stream_refused(relay, writer, relay_view(relay, writer))
    restored = restore_from_swept(relay, writer, others[-1], phrase, expected, path)
    print(f"23. {rounds} rounds of overwrites, deletes and new files with a snapshot each: after every sweep the relay "
          f"holds exactly the newest snapshot, the entries after it and the files they name; it grew by "
          f"{growth['relay_growth_bytes']} bytes from round 1 to round {rounds} (the local files by "
          f"{growth['local_growth_bytes']}), and never held more than the local store plus the manifest and the entries; "
          f"a file written after the last snapshot stayed, and {others[-1].name} restored from the swept relay equals "
          f"{writer.name}", flush=True)
    return {"rounds": measured, "growth": growth, "last": tail, "restore": restored, "slack_bytes": SLACK_BYTES,
            "records": RECORDS, "files": FILES, "file_bytes": FILE_BYTES}


def prove_cloud(args):
    """The rounds on a reference relay of their own, with a writer and a second device of the owner."""
    root = args.output / "relay10"
    root.mkdir()
    relay = Relay(Path(args.relay_bin), root)
    writer, other = (Device(name, args, args.output, relay) for name in ("device-u", "device-v"))
    try:
        relay.start()
        phrase = writer.init()
        writer.start()
        # The relay knows the database once it holds an entry: a second device restores from it then.
        system_seed(writer.node, definition(ALPHA, "Hash", FIELDS, description_prefix="Personal proof "))
        writer.write([create(ALPHA, "first", "one")])
        wait("U to upload its first write", writer.synced_up, seconds=SYNC_SECONDS)
        other.restore(phrase)
        other.start()
        return cloud_rounds(relay, writer, [other], phrase, args.output)
    except Exception:
        for device in (writer, other):
            print(f"{device.name} sync status: {device.sync_status()}", flush=True)
            print("\n".join(device.warnings()), flush=True)
        raise
    finally:
        cleanup_owned((writer, other), (relay,))
