"""Step 18 of the personal database proof: a transfer fails when the connection stalls, not when it is slow.

The node used to allow a blob transfer 30 s plus 1 s per MiB, which assumes 1 MiB/s. On a home uplink
of 0.14 MB/s a snapshot file of a few MB ran out of time and every attempt started over. Now an upload
fails only when the transport takes no byte of the body (or no answer follows the last byte) for 30 s.

    slow:     the relay reads an upload body at SLOW_BYTES_PER_SECOND. The snapshot file takes longer than
              the old allowance, makes progress all the time, and the snapshot completes.
    stalled:  the relay stops reading an upload body. The snapshot request fails after 30 s, and its
              message says "stalled for 30 s with <n> of <size> bytes sent".

Both run at the same time, so the step costs about one minute. Not proven here: a stall in the middle
of a long upload on a real network, and a download that stalls (the download uses the same 30 s
read timeout of the HTTP client, which the proof does not exercise).
"""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import time

from personal_proof_nodes import ALPHA, FIELDS, Device, Relay, require, wait
from personal_proof_status import create_big
from record_runs import definition, system_seed

SLOW_BYTES_PER_SECOND = 32 * 1024
OLD_ALLOWANCE_SECONDS = 30
BIG_RECORDS = 100
STALL_SECONDS = 30
STALL_BOUND_SECONDS = 120


def home_with_data(device, records):
    device.init()
    device.start()
    system_seed(device.node, definition(ALPHA, "Hash", FIELDS, description_prefix="Personal proof "))
    device.write([create_big(i) for i in range(records)])
    wait(f"{device.name} to upload its writes", device.synced_up)


def snapshot_request(device):
    """`lastdb backup snapshot --wait` without the success check: (exit code, stderr, seconds)."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("LASTDB")}
    env.update(LASTDB_SOCKET_PATH=str(device.node.socket), LASTDB_HOME=str(device.node.home))
    started = time.monotonic()
    result = subprocess.run([str(device.cli), "backup", "snapshot", "--wait"], capture_output=True, text=True,
                            timeout=600, check=False, env=env)
    return result.returncode, result.stderr, time.monotonic() - started


def prove_uplink(args):
    out = args.output
    roots = {name: out / name for name in ("relay5", "relay6")}
    for root in roots.values():
        root.mkdir()
    slow_relay = Relay(Path(args.relay_bin), roots["relay5"], extra=["--upload-bytes-per-second", str(SLOW_BYTES_PER_SECOND)])
    stall_relay = Relay(Path(args.relay_bin), roots["relay6"], extra=["--upload-stall-after", "0"])
    slow, stalled = Device("device-q", args, out, slow_relay), Device("device-r", args, out, stall_relay)
    try:
        slow_relay.start()
        stall_relay.start()
        home_with_data(slow, BIG_RECORDS)
        home_with_data(stalled, 2)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first, second = pool.submit(snapshot_request, slow), pool.submit(snapshot_request, stalled)
            slow_code, slow_error, slow_seconds = first.result()
            stall_code, stall_error, stall_seconds = second.result()
        require(slow_code == 0, f"the slow snapshot failed after {slow_seconds:.0f} s: {slow_error[-800:]!r}")
        require(slow_seconds > OLD_ALLOWANCE_SECONDS,
                f"the slow snapshot took {slow_seconds:.0f} s, which the old allowance of {OLD_ALLOWANCE_SECONDS} s would have allowed")
        require(stall_code != 0 and f"stalled for {STALL_SECONDS} s with " in stall_error and " bytes sent" in stall_error,
                f"the stalled snapshot did not fail with the stall message: {stall_code} {stall_error[-800:]!r}")
        require(STALL_SECONDS <= stall_seconds < STALL_BOUND_SECONDS, f"the stalled snapshot took {stall_seconds:.0f} s")
        print(f"18. an upload at {SLOW_BYTES_PER_SECOND // 1024} KiB/s took {slow_seconds:.0f} s (old allowance "
              f"{OLD_ALLOWANCE_SECONDS} s) and completed; an upload the relay stopped reading failed after "
              f"{stall_seconds:.0f} s: {stall_error.strip()[-160:]}", flush=True)
        return {"slow_seconds": round(slow_seconds), "stall_seconds": round(stall_seconds), "stall_message": stall_error.strip()}
    finally:
        for device in (slow, stalled):
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}")
        slow_relay.stop()
        stall_relay.stop()
