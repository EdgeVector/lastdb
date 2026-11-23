"""Step 18 of the personal database proof: a transfer fails when the connection stalls, not when it is slow.

The node used to allow a blob transfer 30 s plus 1 s per MiB, then a 30 s read timeout that reqwest keeps
armed until the answer starts, which for an upload is after the last byte. Either way a progressing
upload of a few MB on a home uplink was cut off. Now an upload fails only when the transport takes no
byte of the body (or no answer follows the last byte) for 30 s.

A record with a file attached sends the file as one sealed blob (one `blob.put`), the same call a
snapshot file uses. Two homes attach a file:

    slow:     the relay reads an upload body of 256 KiB or more at SLOW_BYTES_PER_SECOND. The one upload
              takes longer than 30 s (the relay logs its duration), makes progress all the time, and
              completes: the home is synced up.
    stalled:  the relay stops reading such a body after 256 KiB, and the file is larger than the kernel
              buffers of a loopback connection, so the body cannot be buffered away. The sync turn fails
              after 30 s with "stalled for 30 s with <n> of <size> bytes sent" and no URL query.

Both run at the same time.

A file above 32 MiB (`MAX_RECORD_FILE_BYTES`, the largest value the file store holds) is accepted by
the write up to 512 MiB (LastDB #1872). What the node accepts, it must be able to back up: a third home
attaches such a file and writes one more record after it. Then the home publishes a snapshot, and a
home restored from the recovery phrase holds both records and reads the same file bytes.

Not proven here: a download that stalls (the download client has the 30 s read
timeout), the redaction of a query on a failing upload (the reference relay URL has none), a real network.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import time

from personal_proof_nodes import FIELDS, GAMMA, Device, Relay, require, wait
from personal_proof_steps import gamma_create
import v1_records as v1
from record_runs import RANGED, definition, system_seed

SLOW_BYTES_PER_SECOND = 32 * 1024
SLOW_FILE_BYTES = 1_500_000
# Under 32 MiB, and far above the kernel buffers of a loopback connection.
STALL_FILE_BYTES = 24_000_000
# Above 32 MiB, the largest value the file store holds, and under the 512 MiB the write accepts.
OVER_CEILING_FILE_BYTES = 40_000_000
STALL_AFTER_BYTES = 256 * 1024
OLD_ALLOWANCE_SECONDS = 30
STALL_SECONDS = 30
STALL_BOUND_SECONDS = 150


def home_with_file(device, size):
    """A home with one record that has a file of `size` random bytes. Returns the time the write was made."""
    device.init()
    device.start()
    system_seed(device.node, definition(GAMMA, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Personal proof "))
    source = device.out / "attachment.bin"
    source.write_bytes(os.urandom(size))
    attached = {"path": str(source), "name": "uplink.bin", "media_type": "application/x-proof"}
    device.write([gamma_create("f1", "grp-f", "uplink", file=attached)])
    return time.monotonic()


def large_file_is_backed_up(args, out):
    """A home that attaches a file above 32 MiB still publishes a snapshot, and a restore equals it."""
    root = out / "relay9"
    root.mkdir()
    relay = Relay(Path(args.relay_bin), root)
    source, restored = Device("device-u", args, out, relay), Device("device-v", args, out, relay)
    try:
        relay.start()
        phrase = source.init()
        source.start()
        system_seed(source.node, definition(GAMMA, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Personal proof "))
        content = os.urandom(OVER_CEILING_FILE_BYTES)
        attachment = source.out / "large.bin"
        attachment.write_bytes(content)
        attached = {"path": str(attachment), "name": "large.bin", "media_type": "application/x-proof"}
        source.write([gamma_create("f1", "grp-f", "large", file=attached)])
        source.write([gamma_create("f2", "grp-f", "after the large file")])
        wait("the home with the large file to sync up", source.synced_up, seconds=300)
        snapshot = source.snapshot_now()
        restored.restore(phrase)
        restored.start()
        wait("the restored home to hold both records",
             lambda: restored.has_schema(GAMMA) and sorted(restored.keys(GAMMA)) == ["f1", "f2"], seconds=300)
        folder = restored.out / "read-files"
        folder.mkdir()
        item = v1.point(GAMMA, {"hash": "f1"}, file=True)
        item["file_dir"] = str(folder)
        rows = v1.read_items(restored.node, [item])[0]["records"]
        require(rows and rows[0].get("file") and rows[0]["file"].get("path"), f"the restored home reads no file for f1: {rows!r}")
        read = Path(rows[0]["file"]["path"]).read_bytes()
        require(hashlib.sha256(read).digest() == hashlib.sha256(content).digest(),
                f"the restored file differs: {len(read)} bytes read, {len(content)} written")
        return {"file_bytes": OVER_CEILING_FILE_BYTES, "snapshot_seq": snapshot.get("snapshot_seq")}
    except Exception:
        for device in (source, restored):
            try:
                print(f"{device.name} sync status: {device.sync_status()}", flush=True)
                print(f"{device.name} warnings: {device.warnings()[-8:]}", flush=True)
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: no status: {error}", flush=True)
        raise
    finally:
        for device in (source, restored):
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}")
        relay.stop()


def prove_uplink(args):
    out = args.output
    slow_root, stall_root = out / "relay5", out / "relay6"
    slow_root.mkdir()
    stall_root.mkdir()
    slow_relay = Relay(Path(args.relay_bin), slow_root, extra=["--upload-bytes-per-second", str(SLOW_BYTES_PER_SECOND)])
    stall_relay = Relay(Path(args.relay_bin), stall_root, extra=["--upload-stall-after", str(STALL_AFTER_BYTES)])
    slow, stalled = Device("device-q", args, out, slow_relay), Device("device-r", args, out, stall_relay)
    try:
        slow_relay.start()
        stall_relay.start()
        slow_started = home_with_file(slow, SLOW_FILE_BYTES)
        stall_started = home_with_file(stalled, STALL_FILE_BYTES)
        wait("the slow upload to complete", slow.synced_up, seconds=180)
        slow_seconds = time.monotonic() - slow_started
        # One upload, not several short ones, took longer than the old allowance.
        uploads = [int(line.split("upload_ms=")[1]) for line in slow_relay.lines() if "upload_ms=" in line]
        require(uploads and max(uploads) > (OLD_ALLOWANCE_SECONDS + 2) * 1000,
                f"no single upload took over {OLD_ALLOWANCE_SECONDS + 2} s; uploads took {uploads!r} ms")
        message = wait("the stalled upload to fail", lambda: re.search(
            r"(\d+) s with (\d+) of (\d+) bytes sent", json.dumps(stalled.sync_status())),
            seconds=STALL_BOUND_SECONDS)
        stall_seconds = time.monotonic() - stall_started
        require(30 <= int(message[1]) < 40 and int(message[2]) < int(message[3]),
                f"the stall did not end in the middle of the body after 30 s: {message[0]!r}")
        require(not stalled.synced_up(), "the stalled home reports its writes as uploaded")
        place = re.search(r"\(for (\S+)\)", json.dumps(stalled.sync_status()))
        require(place and "?" not in place[1] and "X-Amz" not in place[1], f"the error names no URL or one with a query: {place}")
        print(f"18. a file upload at {SLOW_BYTES_PER_SECOND // 1024} KiB/s took {max(uploads) / 1000:.0f} s as one upload "
              f"(old allowance {OLD_ALLOWANCE_SECONDS} s) and completed after {slow_seconds:.0f} s; an upload the relay "
              f"stopped reading failed after {stall_seconds:.0f} s: {message[0]}", flush=True)
        large = large_file_is_backed_up(args, out)
        print(f"18b. a home with a file of {large['file_bytes']} bytes attached, above 32 MiB, published snapshot "
              f"{large['snapshot_seq']}; a home restored from the phrase holds the record written after it and reads "
              f"the same file bytes", flush=True)
        return {"longest_upload_ms": max(uploads), "slow_seconds": round(slow_seconds),
                "stall_seconds": round(stall_seconds), "stall_message": message[0],
                "stall_file_bytes": STALL_FILE_BYTES, "large_file": large}
    except Exception:
        for device in (slow, stalled):
            print(f"{device.name} sync status: {device.sync_status()}", flush=True)
        raise
    finally:
        for device in (slow, stalled):
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}")
        slow_relay.stop()
        stall_relay.stop()
