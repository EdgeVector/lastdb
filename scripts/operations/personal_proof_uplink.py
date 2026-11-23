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

Both run at the same time. Not proven here: a download that stalls (the download client has the 30 s read
timeout), the redaction of a query on a failing upload (the reference relay URL has none), a real network.
"""
import json
import os
from pathlib import Path
import re
import time

from personal_proof_nodes import FIELDS, GAMMA, Device, Relay, require, wait
from personal_proof_steps import gamma_create
from record_runs import RANGED, definition, system_seed

SLOW_BYTES_PER_SECOND = 32 * 1024
SLOW_FILE_BYTES = 1_500_000
STALL_FILE_BYTES = 16_000_000
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
            r"stalled for 3\d s with (\d+) of (\d+) bytes sent", json.dumps(stalled.sync_status())),
            seconds=STALL_BOUND_SECONDS)
        stall_seconds = time.monotonic() - stall_started
        require(int(message[1]) < int(message[2]), f"the stall message names {message[0]!r}")
        require(not stalled.synced_up(), "the stalled home reports its writes as uploaded")
        require("?" not in json.dumps(stalled.sync_status()).split("stalled for")[1][:60], "the error holds a URL query")
        print(f"18. a file upload at {SLOW_BYTES_PER_SECOND // 1024} KiB/s took {max(uploads) / 1000:.0f} s as one upload "
              f"(old allowance {OLD_ALLOWANCE_SECONDS} s) and completed after {slow_seconds:.0f} s; an upload the relay "
              f"stopped reading failed after {stall_seconds:.0f} s: {message[0]}", flush=True)
        return {"longest_upload_ms": max(uploads), "slow_seconds": round(slow_seconds),
                "stall_seconds": round(stall_seconds), "stall_message": message[0]}
    finally:
        for device in (slow, stalled):
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}")
        slow_relay.stop()
        stall_relay.stop()
