"""Step 16 of the personal database proof: the relay keeps one snapshot, and still restores.

After a snapshot, the node asks the relay to drop what that snapshot covers (`log/trim`) and then sweeps
the stored files that nothing names (`blob/list`, `blob/drop`). The first two trims of the proof ran at
steps 11 and 13, when A took its first and second snapshot. This step checks the relay, restores from
it, and takes a third snapshot:

- the relay holds one snapshot entry, the newest, and no entry at or before its positions, on any
  stream, and keeps the birth and every entry after the positions;
- a late repeat of an append at and below the floor is answered as a duplicate and stored nowhere;
- a device that stopped before those positions is told it is behind and applies nothing;
- a restore from the recovery phrase installs the newest snapshot, replays only the entries after
  it, and equals A key by key, alternate keys and file included;
- the relay restarts and still lists the streams, takes the next append of each device, and
  restores another home (the floor of each stream is on disk);
- a third snapshot drops the second snapshot entry, the entries up to its own positions, and the
  manifest and the files that only the second snapshot named. A restore from the third equals A;
- devices that read up to the cut before each snapshot (A, B, F and the restored homes) read nothing
  wrong, and log no warning that they are behind.

Every blob the relay holds afterwards is accounted for in step 21 (`personal_proof_reconcile.py`).
"""
import base64
import hashlib
import json
import secrets
import shutil
import time
import urllib.error
import urllib.request

from personal_proof_nodes import ALPHA, BETA, converged, create, expect, wait
from personal_proof_snapshot import (
    last_record, snapshot_stream, stored_seqs, stream_dirs, stream_kind, views)
from personal_proof_status import swept
from record_runs import require

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
except ImportError:  # pragma: no cover - the runner image has it
    Ed25519PrivateKey = None

BEHIND = "behind history that the relay dropped after a snapshot"


def told_behind(device):
    """The status of the daemon says the device is behind history the relay dropped (its sync error)."""
    return BEHIND in json.dumps(device.sync_status())


def entry_of(directory, seq):
    return json.loads((directory / f"{seq}.json").read_text())


def positions(entry):
    """The stream positions a snapshot entry names: device id -> last seq it holds."""
    return {position["device_id"]: position["writer_seq"] for position in entry["through"]}


def blob_names(relay):
    return {path.name for path in (relay.data / "blobs").iterdir() if path.is_file()}


def manifest_files(relay, entry):
    """(manifest digest, digests of the files it lists) of a snapshot entry, read from the relay's blobs."""
    digest = entry["manifest_sha256"]
    listed = json.loads((relay.data / "blobs" / digest).read_text())["files"]
    return digest, {file["sha256"] for file in listed}


def check_dropped(relay, database, entry):
    """No stream holds an entry at or before its position in `entry`, the rest is whole, the birth stays.

    A stream with nothing after its position holds no entry at all, and its floor is the position.
    Returns the number of entries after the positions, which is what a restore from `entry` replays.
    """
    through, tail, streams = positions(entry), 0, {"birth": 0, "snapshot": 0}
    for directory in stream_dirs(relay, database):
        kind = stream_kind(directory)
        seqs = stored_seqs(directory)
        require(not seqs or seqs == list(range(seqs[0], seqs[-1] + 1)), f"the stream {directory.name} has a gap: {seqs[:5]}")
        if kind in streams:
            require(seqs and (kind != "birth" or seqs == [1]), f"the {kind} stream holds {seqs[:5]}")
            streams[kind] += 1
            continue
        position = through.get(directory.name, 0)
        if seqs:
            require(seqs[0] > position, f"the stream {directory.name} still holds entry {seqs[0]}, at or before {position}")
            require(not position or seqs[0] == position + 1,
                    f"the stream {directory.name} lost entries after position {position}")
        else:
            require(position and relay.stream_floor(database, directory.name) == position,
                    f"the stream {directory.name} holds no entry and its floor is not {position}")
        tail += (seqs[-1] if seqs else position) - position
    require(streams == {"birth": 1, "snapshot": 1}, f"the relay holds streams {streams!r}")
    return tail


def relay_post(relay, device, database, node_hex, path, body):
    """One signed request of A to the relay, as the node signs it. Returns (HTTP status, JSON answer)."""
    key = Ed25519PrivateKey.from_private_bytes((device.node.home / "identity.key").read_bytes())
    raw = json.dumps(body, separators=(",", ":")).encode()
    issued, nonce = str(int(time.time())), secrets.token_hex(32)
    message = f"POST\n{path}\n1\n{node_hex}\n{database}\n{hashlib.sha256(raw).hexdigest()}\n{issued}\n{nonce}"
    signature = base64.urlsafe_b64encode(key.sign(message.encode())).rstrip(b"=").decode()
    headers = {"X-LastDB-Relay-Protocol": "1", "X-LastDB-Node-Key": node_hex, "X-LastDB-Database-Id": database,
               "X-LastDB-Device-Id": device.device_id(), "X-LastDB-Issued-At": issued, "X-LastDB-Nonce": nonce,
               "X-LastDB-Signature": signature, "content-type": "application/json"}
    request = urllib.request.Request(relay.url + path, data=raw, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as answer:
            return answer.status, json.loads(answer.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def late_repeat(relay, a, database, newest, kept):
    """A repeat of an append of entries at and below the floor is answered as a duplicate, is not stored
    and is not readable (410). `kept` holds the entries, read from the relay before the trim dropped them."""
    seq = positions(newest)[a.device_id()]
    directory = next(path for path in stream_dirs(relay, database) if path.name == a.device_id())
    node_hex, stored = directory.parent.name, relay.stream_floor(database, a.device_id())
    require(stored == seq, f"the floor of A is {stored}, not {seq}")
    before = stored_seqs(directory)
    for repeated in (seq, seq - 3):
        require(repeated in kept, f"the proof kept no entry {repeated} of A from before the trim")
        status, answer = relay_post(relay, a, database, node_hex, "/relay/v1/log/append",
                                    {"database_id": database, "entries": [kept[repeated]]})
        require(status == 200 and answer.get("duplicate") is True, f"a repeat of entry {repeated}: {status} {answer!r}")
        require(not (directory / f"{repeated}.json").exists(), f"the relay stored entry {repeated} again below the floor")
        status, answer = relay_post(relay, a, database, node_hex, "/relay/v1/log/read",
                                    {"database_id": database, "writer_key": node_hex, "device_id": a.device_id(),
                                     "from_seq": repeated, "limit": 1})
        require(status == 410 and answer.get("code") == "trimmed", f"a read at entry {repeated}: {status} {answer!r}")
    require(stored_seqs(directory) == before, "the repeats changed the entries of A's stream")
    print(f"   a repeat of entries {seq - 3} and {seq} of A (at and below the floor) was answered as a duplicate, "
          "not stored, and a read there is 410", flush=True)


def behind_device(c, relay, phrase):
    """A device that stopped before the positions is told it is behind, and applies nothing."""
    c.start()
    before = wait("C to read its home", c.snapshot)
    wait("C to be told it is behind", lambda: told_behind(c), seconds=120)
    time.sleep(5)
    require(c.snapshot() == before, "C applied entries although the relay dropped the ones it needs")
    c.stop()
    return before


def restore_equals(relay, device, phrase, state, folders, expected, snapshot_seq, tail):
    """A fresh home from the phrase installs the newest snapshot, replays only the tail, and equals A."""
    device.stop()
    if device.node.home.exists():
        shutil.rmtree(device.node.home)
    restored = device.restore(phrase)
    require(restored["snapshot"] and restored["snapshot"]["snapshot_seq"] == snapshot_seq,
            f"{device.name} restore: {restored!r}")
    require(restored["replayed"] == tail, f"{device.name} replayed {restored['replayed']} entries, the tail is {tail}")
    device.start()
    folders[device.name] = device.out / "file-dir"
    folders[device.name].mkdir(mode=0o700, exist_ok=True)
    wait(f"{device.name} to equal A", lambda: views((device,), state, folders, expected))
    return restored


def trimmed_relay_restores(relay, a, b, c, f, g, phrase, state, folders, expected, kept):  # noqa: PLR0915 one scenario, in order
    """16. The relay holds one snapshot and no covered entry, still restores, survives a restart, and a third replaces it."""
    database = a.database
    wait("A, B and F to upload", lambda: a.synced_up() and b.synced_up() and f.synced_up())
    directory = snapshot_stream(relay, database)
    require(directory is not None and stored_seqs(directory) == [2],
            f"the relay does not hold snapshot 2 alone: {stored_seqs(directory) if directory else None}")
    require(relay.stream_floor(database, directory.name) == 1, "the floor of the snapshot stream is not entry 1")
    newest = entry_of(directory, 2)
    require(newest["through"], "snapshot 2 names no position: nothing was covered")
    dropped = sum(positions(newest).values())
    check_dropped(relay, database, newest)
    for device_id, position in positions(newest).items():
        require(relay.stream_floor(database, device_id) == position, f"the floor of {device_id[:8]} is not {position}")

    late_repeat(relay, a, database, newest, kept)

    # A device behind the positions fails clearly and applies nothing.
    behind = behind_device(c, relay, phrase)
    require(behind != state, "C is not behind: the proof did not make a device that needs dropped entries")

    # A restore from the trimmed relay: the newest snapshot, then only what follows it.
    tail_newest = tail_after(relay, database, newest)
    first_restore = restore_equals(relay, g, phrase, state, folders, expected, 2, tail_newest)
    print(f"16. the relay dropped the {dropped} entries that snapshot 2 covers and snapshot 1, and keeps the birth, "
          f"snapshot 2 and what follows; G restored from the phrase and replayed {tail_newest} entries and equals A; C, "
          "which stopped before the positions, was told it is behind and applied nothing", flush=True)

    # The relay restarts: the floors are on disk, the streams list, appends and a restore work.
    relay.stop()
    relay.start()
    a.write([create(ALPHA, "after-trim", "one")])
    expect(state, ALPHA, "after-trim", "one")
    b.write([create(BETA, "after-trim-b", "one")])
    expect(state, BETA, "after-trim-b", "one")
    wait("A, B, F and G to hold both writes", lambda: converged((a, b, f, g), state))
    wait("A, B and F to upload", lambda: a.synced_up() and b.synced_up() and f.synced_up())
    restore_equals(relay, c, phrase, state, folders, expected, 2, tail_after(relay, database, newest))
    print("   the relay restarted: it lists the trimmed streams, takes the next entry of A and B, and restored C",
          flush=True)

    # A third snapshot replaces the second: its entry, its manifest and the files only it named go.
    second_manifest, second_files = manifest_files(relay, newest)
    require((second_files | {second_manifest}) <= blob_names(relay), "the relay lacks a file of snapshot 2 before the third")
    a.write([create(ALPHA, "before-third", "one")])
    expect(state, ALPHA, "before-third", "one")
    wait("A to upload", a.synced_up)
    # Every running device reads up to the cut: the snapshot drops what it covers.
    wait("B, F, G and C to hold what A wrote", lambda: views((b, f, g, c), state, folders, expected))
    third = a.snapshot_now()
    require(third["snapshot_seq"] == 3, f"third snapshot: {third!r}")
    swept(a, 3)
    directory = snapshot_stream(relay, database)
    require(stored_seqs(directory) == [3], f"the snapshot entries after the third trim: {stored_seqs(directory)}")
    require(relay.stream_floor(database, directory.name) == 2, "the floor of the snapshot stream is not entry 2")
    newest = entry_of(directory, 3)
    held = blob_names(relay)
    third_files = {file["sha256"] for file in last_record(a)["manifest"]["files"]}
    require((third_files | {newest["manifest_sha256"]}) <= held, "the relay lacks a file of the newest snapshot")
    only_second = (second_files | {second_manifest}) - (third_files | {newest["manifest_sha256"]})
    require(len(only_second) > 1, "snapshot 2 names no file that snapshot 3 does not: the proof cannot see a file go")
    require(not only_second & held, f"the relay kept {len(only_second & held)} blobs that only snapshot 2 named")
    check_dropped(relay, database, newest)
    for device_id, position in positions(newest).items():
        require(relay.stream_floor(database, device_id) == position, f"the floor of {device_id[:8]} is not {position}")
    tail_third = tail_after(relay, database, newest)
    restored = restore_equals(relay, g, phrase, state, folders, expected, 3, tail_third)
    wait("A, B, F and G to hold every record", lambda: views((a, b, f, g), state, folders, expected))
    for device in (a, b, f, c):
        require(not told_behind(device), f"{device.name}, which was past the positions, was told it is behind")
    print(f"   a third snapshot dropped snapshot 2, its manifest and the {len(only_second) - 1} files that only it "
          f"named; G restored from snapshot 3 ({tail_third} entries replayed) and equals A; A, B, C and F were never "
          "told they are behind", flush=True)
    return {"dropped_entries": dropped, "tail_after_snapshot_2": tail_newest, "blobs_dropped_with_snapshot_2": len(only_second),
            "restore_after_trim": {k: first_restore[k] for k in ("replayed", "devices")},
            "restore_after_third": {k: restored[k] for k in ("replayed", "devices")}}


def tail_after(relay, database, entry):
    """Entries the relay holds after the positions of the snapshot entry, on every data stream."""
    through, tail = positions(entry), 0
    for directory in stream_dirs(relay, database):
        if stream_kind(directory) in ("birth", "snapshot"):
            continue
        seqs = stored_seqs(directory)
        position = through.get(directory.name, 0)
        tail += (seqs[-1] if seqs else position) - position
    return tail
