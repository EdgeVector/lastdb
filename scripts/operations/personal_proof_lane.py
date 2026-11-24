"""Step 20 of the personal database proof: entries go on while a snapshot's files upload.

A snapshot of a home uploads tens of megabytes. On a slow uplink that takes minutes, and it used to
run inside the sync turn, so no entry was sent or acknowledged and no other device was read until
it ended. Now a turn freezes the files, and the upload runs in a lane beside the turns.

The reference relay reads an upload of 256 KiB or more at LANE_BYTES_PER_SECOND. Device W holds a
record with a file of LANE_FILE_BYTES, so its snapshot has one large file. Device X restored from
the phrase and follows W.

    while the snapshot uploads: W writes. The spool of W is acknowledged, X reads the writes, and the
        relay holds no snapshot entry yet. `lastdb status` says the snapshot is uploading and how far.
    kill -9 of W in the middle of the upload: nothing is lost and nothing repeats. The restart finds the
        request and takes the snapshot again from a new cut. Its files are the cut it freezes then.
    during the second upload: W writes again, with the same three checks.
    after it publishes: the relay holds one snapshot entry. Its position for W is the last entry that
        was acknowledged when the cut was frozen, and W signed more entries after it. W's stream on
        the relay has every sequence number once. A home Y restored from the phrase installs the
        snapshot, replays exactly the entries after that position, and equals W and X.

Not proven here: a stop of the daemon (not a kill) while the lane uploads. The code path is an abort
of the lane at an await, which kill -9 covers in its effect on disk and on the relay.
"""
from pathlib import Path
import re
import subprocess
import time

from personal_proof_nodes import GAMMA, Device, Relay, require, wait
from personal_proof_snapshot import newest_snapshot_entry, snapshot_stream, stored_seqs
from personal_proof_status import status_lines, swept
from personal_proof_steps import gamma_create
from personal_proof_uplink import home_with_file

LANE_BYTES_PER_SECOND = 24 * 1024
LANE_FILE_BYTES = 500_000
SLOW_UPLOAD_MS = 5000


def request_snapshot(device):
    """Queue a snapshot and return at once. The daemon freezes it in its next turn."""
    env = {**device.cli_env(), "LASTDB_SOCKET_PATH": str(device.node.socket), "LASTDB_HOME": str(device.node.home)}
    result = subprocess.run([str(device.cli), "backup", "snapshot"], capture_output=True, text=True, timeout=120,
                            check=False, env=env)
    require(result.returncode == 0, f"{device.name}: backup snapshot failed: {result.stderr[-500:]}")


def snapshot_state(device):
    sync = device.sync_status()
    return sync["snapshot"] if isinstance(sync, dict) else {}


def uploading(device):
    """(files done, files in all) while a file of a snapshot is still to go up, else None."""
    snapshot = snapshot_state(device)
    found = re.match(r"uploaded (\d+) of (\d+) files", snapshot.get("message") or "")
    if snapshot.get("state") == "in_progress" and found and int(found[1]) < int(found[2]):
        return int(found[1]), int(found[2])
    return None


def records_of(device):
    try:
        return device.records(GAMMA)
    except RuntimeError:
        return None


def write_during_upload(relay, writer, reader, keys, label):
    """Writes made while the files upload are acknowledged and read by the other device, before the
    relay holds a snapshot entry. The snapshot is still uploading when all three are true."""
    require(uploading(writer) is not None, f"{writer.name}: no snapshot is uploading before the writes")
    writer.write([gamma_create(key, "grp-x", label) for key in keys])
    wait(f"{writer.name} to acknowledge the writes made during the upload", writer.synced_up, seconds=60)
    wait(f"{reader.name} to read the writes made during the upload",
         lambda: set(keys) <= set(records_of(reader) or {}), seconds=60)
    left = uploading(writer)
    require(left is not None, f"{writer.name}: the snapshot ended before the writes were read ({snapshot_state(writer)!r})")
    require(snapshot_stream(relay, writer.database) is None, "the relay holds a snapshot entry before the upload ended")
    return left


def prove_lane(args):
    out = args.output
    root = out / "relay9"
    root.mkdir()
    relay = Relay(Path(args.relay_bin), root, extra=["--upload-bytes-per-second", str(LANE_BYTES_PER_SECOND)])
    w, x, y = (Device(name, args, out, relay) for name in ("device-w", "device-x", "device-y"))
    started = time.monotonic()
    try:
        relay.start()
        home_with_file(w, LANE_FILE_BYTES)
        phrase = (w.node.home / "recovery-phrase.txt").read_text().strip()
        wait("W to upload the record with its file", w.synced_up, seconds=180)
        x.restore(phrase)
        x.start()
        wait("X to hold the record with the file", lambda: "f1" in (records_of(x) or {}))

        # First upload: writes go on, then kill -9.
        request_snapshot(w)
        wait("the files of the snapshot to upload", lambda: uploading(w), seconds=90)
        lines = status_lines(w)
        require(any(line.startswith("Snapshot uploading: uploaded ") for line in lines), f"status: {lines!r}")
        first = write_during_upload(relay, w, x, ["l1", "l2", "l3"], "during-first")
        w.crash()
        w.start()

        # The restart finds the request and freezes a new cut. Nothing was added since the kill.
        wait("the snapshot to upload again after the restart", lambda: uploading(w), seconds=90)
        wait("W to settle its spool", w.synced_up, seconds=60)
        cut = w.spool_state()["head"]
        second = write_during_upload(relay, w, x, ["m1", "m2", "m3"], "during-second")
        wait("the snapshot to publish", lambda: snapshot_state(w).get("state") == "published", seconds=180)
        wait("W to upload", w.synced_up, seconds=60)
        # The lane then asks the relay to drop what the snapshot covers, and the next turns sweep.
        swept(w, 1)

        # What the snapshot says, and what the relay holds.
        seq, entry = newest_snapshot_entry(relay, w.database)
        require(seq == 1 and stored_seqs(snapshot_stream(relay, w.database)) == [1], f"snapshot entries: seq {seq}")
        through = {position["device_id"]: position["writer_seq"] for position in entry["through"]}
        head = w.spool_state()["head"]
        require(through.get(w.device_id()) == cut and head > cut,
                f"the snapshot holds {through!r}; W was acknowledged through {cut} at the cut and signed up to {head}")
        require(relay.stream_entries(w.database, w.device_id()) == list(range(cut + 1, head + 1))
                and relay.stream_floor(w.database, w.device_id()) == cut,
                "W's stream on the relay is not every sequence number after the cut, once")
        require(w.checkpoint()["writer_seq"] == head, f"W confirmed {w.checkpoint()!r}")

        # A restore from it plus the tail equals W and X.
        restored = y.restore(phrase)
        require(restored["snapshot"]["snapshot_seq"] == 1 and restored["replayed"] == head - cut,
                f"Y restore: {restored!r}, expected a replay of {head - cut} entries")
        y.start()
        wait("Y to equal W", lambda: records_of(y) == records_of(w) and records_of(y) is not None)
        wait("X to equal W", lambda: records_of(x) == records_of(w))
        require(len(records_of(w)) == 7, f"W holds {sorted(records_of(w))!r}")

        slow = [int(line.split("upload_ms=")[1]) for line in relay.lines() if "upload_ms=" in line]
        slow = [ms for ms in slow if ms >= SLOW_UPLOAD_MS]
        require(len(slow) >= 2, f"the relay did not read two large uploads slowly: {slow!r}")
        seconds = time.monotonic() - started
        print(f"20. while the files of a snapshot uploaded at {LANE_BYTES_PER_SECOND // 1024} KiB/s, W's writes were "
              f"acknowledged and read by X before the relay held a snapshot entry (file {first[0]} of {first[1]} done "
              f"at the first check, {second[0]} of {second[1]} at the second); kill -9 of W in the first upload lost "
              f"and repeated nothing; the snapshot names W through entry {cut} and W signed up to {head}; Y restored "
              f"it and replayed {restored['replayed']} entries and equals W and X ({seconds:.0f} s)", flush=True)
        return {"cut": cut, "head": head, "replayed": restored["replayed"], "slow_uploads_ms": slow,
                "files_done_first": first, "files_done_second": second, "seconds": round(seconds)}
    except Exception:
        for device in (w, x, y):
            print(f"{device.name} sync status: {device.sync_status()}", flush=True)
            print("\n".join(device.warnings()), flush=True)
        raise
    finally:
        for device in (w, x, y):
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}")
        relay.stop()
