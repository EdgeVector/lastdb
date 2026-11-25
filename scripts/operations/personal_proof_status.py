"""Step 15 of the personal database proof: `lastdb status` and `/api/status` name the snapshot.

The status of a node says where the newest snapshot of its personal database is, from what the
node recorded when it published or installed that snapshot, with no call to the relay:

    Snapshot: <local time>, <N> files, <size>, on <relay origin>, covers this device through entry <seq>

After a sweep (the relay dropped every stored file that the newest snapshot and the entries after it do
not name), one more line says what the relay holds, from the `cloud_sync.reclaim` object:

    Cloud storage: <N> files, <size> on the relay after the last sweep (the snapshot it kept: <N> files, <size>)

A home with no snapshot says `Snapshot: none yet`. A snapshot file that is larger than the relay
accepts is reported with what a person can do, once, and is not retried in a loop.
"""
from datetime import datetime
import os
import subprocess
import time

from personal_proof_nodes import ALPHA, FIELDS as RECORD_FIELDS, require, wait
from record_runs import definition, system_seed
import v1_records as v1

NONE_YET = "Snapshot: none yet"
# The reference relay for the refusal takes no blob larger than this. It must take every log segment: a
# segment holds one sent batch, at most 2 MiB of entries (BATCH_BYTES in personal/outgoing.rs) and newlines.
RELAY_LIMIT_BYTES = 2560 * 1024
# Records written to the refusing home, WRITE_CHUNK at a time: together they make a data file over the limit,
# about half their text after compression.
BIG_RECORDS, BIG_NOTE_BYTES, WRITE_CHUNK = 200, 30_000, 40
# The refused snapshot is not tried again while this long passes, many sync turns.
QUIET_SECONDS = 12
FIELDS = ("state", "taken_at_ms", "files", "bytes", "relay", "manifest_sha256", "through_seq", "snapshot_seq",
          "streams", "message")


def status_lines(device):
    """The lines of `lastdb status` for the running daemon of `device`."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("LASTDB")}
    env.update(LASTDB_SOCKET_PATH=str(device.node.socket), LASTDB_HOME=str(device.node.home))
    result = subprocess.run([str(device.cli), "status"], capture_output=True, text=True, timeout=120, check=False, env=env)
    require(result.returncode == 0, f"{device.name}: lastdb status failed: {result.stderr[-500:]}")
    return result.stdout.splitlines()


def snapshot_object(device):
    """The `cloud_sync.snapshot` object of /api/status."""
    sync = device.sync_status()
    require(isinstance(sync, dict) and "snapshot" in sync, f"{device.name}: no cloud_sync.snapshot in {sync!r}")
    snapshot = sync["snapshot"]
    require(set(snapshot) == set(FIELDS), f"{device.name}: snapshot fields {sorted(snapshot)}")
    # The fields that were there before stay.
    require({"connected", "environment", "databases", "last_error"} <= set(sync), f"{device.name}: cloud_sync lost a field")
    return snapshot


def size_text(length):
    """A length with one decimal in the largest binary unit that fits, as the CLI prints it."""
    for name, unit in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if length >= unit:
            tenths = length * 10 // unit
            return f"{tenths // 10}.{tenths % 10} {name}"
    return f"{length} B"


def line_for(snapshot):
    when = datetime.fromtimestamp(snapshot["taken_at_ms"] // 1000).astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
    return (f"Snapshot: {when}, {snapshot['files']} files, {size_text(snapshot['bytes'])}, on {snapshot['relay']}, "
            f"covers this device through entry {snapshot['through_seq']}")


def snapshot_lines(device):
    """The snapshot lines of the status, which must follow the cloud sync line."""
    lines = status_lines(device)
    found = [i for i, line in enumerate(lines) if line.startswith("Snapshot")]
    require(found, f"{device.name}: status has no snapshot line: {lines!r}")
    # A node with a saved cloud connection prints a cloud sync line, and the snapshot lines follow it.
    # The proof nodes sync through LASTDB_RELAY_URL and have none.
    sync = [i for i, line in enumerate(lines) if line.startswith("Cloud sync:")]
    require(not sync or found[0] == sync[0] + 1, f"{device.name}: the snapshot line does not follow: {lines!r}")
    return [lines[i] for i in found]


def swept(device, snapshot_seq, seconds=150):
    """The `cloud_sync.reclaim` object of /api/status once the sweep that follows snapshot `snapshot_seq` is done.

    After a snapshot is named and trimmed, the next sync turns drop every stored file that the snapshot and the
    entries after it do not name. The object names the snapshot the last finished sweep kept.
    """
    def probe():
        sync = device.sync_status()
        reclaim = sync.get("reclaim") if isinstance(sync, dict) else None
        return reclaim if reclaim and reclaim.get("snapshot_seq") == snapshot_seq else None
    return wait(f"{device.name} to sweep the relay after snapshot {snapshot_seq}", probe, seconds=seconds)


def reclaim_now(device):
    """`lastdb backup reclaim --wait`: ask the running daemon for a sweep now. Returns its report."""
    return device.run(["backup", "reclaim", "--wait"],
                      env={"LASTDB_SOCKET_PATH": str(device.node.socket), "LASTDB_HOME": str(device.node.home)})


def storage_line(reclaim):
    """The cloud storage line of `lastdb status` for a `cloud_sync.reclaim` object."""
    return (f"Cloud storage: {reclaim['cloud_files']} files, {size_text(reclaim['cloud_bytes'])} on the relay after the "
            f"last sweep (the snapshot it kept: {reclaim['snapshot_files']} files, {size_text(reclaim['snapshot_bytes'])})")


def prove_none_yet(device):
    """A fresh home: no snapshot yet, in the text and in the JSON."""
    snapshot = snapshot_object(device)
    require(snapshot["state"] == "none" and all(snapshot[name] is None for name in FIELDS[1:]), f"fresh: {snapshot!r}")
    require(snapshot_lines(device) == [NONE_YET], f"fresh status lines: {snapshot_lines(device)!r}")
    reclaim = device.sync_status()["reclaim"]
    require(all(value is None for value in reclaim.values()), f"fresh: a sweep is reported: {reclaim!r}")
    require(not any(line.startswith("Cloud storage:") for line in status_lines(device)), "fresh: a cloud storage line")
    print("15. a fresh home says 'Snapshot: none yet' in `lastdb status` and state none in /api/status", flush=True)


def prove_taken(device, report, entry, relay, state):
    """After a snapshot of this device: the line and the object match the report and the entry on the relay."""
    seq, signed = entry
    snapshot = snapshot_object(device)
    own = {position["device_id"]: position["writer_seq"] for position in signed["through"]}
    expected = {"state": state, "taken_at_ms": signed["authored_at_ms"], "files": report["files"],
                "bytes": report["snapshot_bytes"], "relay": relay.url, "manifest_sha256": signed["manifest_sha256"],
                "through_seq": own.get(device.device_id(), 0), "snapshot_seq": seq, "streams": len(signed["through"]),
                "message": None}
    require(snapshot == expected, f"{device.name}: status object {snapshot!r}, expected {expected!r}")
    require(snapshot_lines(device) == [line_for(expected)], f"{device.name}: status lines {snapshot_lines(device)!r}")
    return snapshot


def prove_installed(device, published):
    """A device restored from the snapshot reports the same snapshot, as installed."""
    snapshot = snapshot_object(device)
    expected = {**published, "state": "installed", "through_seq": 0}
    require(snapshot == expected, f"{device.name}: installed status {snapshot!r}, expected {expected!r}")
    require(snapshot_lines(device) == [line_for(expected)], f"{device.name}: status lines {snapshot_lines(device)!r}")


def refused_snapshot(relay, device):
    """A relay that refuses a file over its limit: a clear report, no loop, and the entries still go up."""
    require(refusals(relay) == 0, "the refusing relay refused a blob before the snapshot")
    system_seed(device.node, definition(ALPHA, "Hash", RECORD_FIELDS, description_prefix="Personal proof "))
    for start in range(0, BIG_RECORDS, WRITE_CHUNK):
        device.write([create_big(i) for i in range(start, min(start + WRITE_CHUNK, BIG_RECORDS))])
    warned = len(device.warnings(all_lines=True))
    wait("the refusing home to upload its writes", device.synced_up)
    env = {k: v for k, v in os.environ.items() if not k.startswith("LASTDB")}
    env.update(LASTDB_SOCKET_PATH=str(device.node.socket), LASTDB_HOME=str(device.node.home))
    asked = subprocess.run([str(device.cli), "backup", "snapshot", "--wait"], capture_output=True, text=True,
                           timeout=600, check=False, env=env)
    require(asked.returncode != 0 and "snapshot cannot be published" in asked.stderr,
            f"the snapshot request did not fail with the report: {asked.returncode} {asked.stderr[-800:]!r}")
    message = asked.stderr.strip()
    require("bytes" in message and "refuses a file that large" in message and "limit" in message and relay.url in message,
            f"the report lacks the size, the relay or the limit: {message!r}")
    require(str(device.node.home) not in message and "disk-" not in message, f"the report names a path: {message!r}")
    snapshot = snapshot_object(device)
    require(snapshot["state"] == "blocked" and snapshot["message"] is not None
            and snapshot["message"] in message, f"blocked status object: {snapshot!r}")
    lines = snapshot_lines(device)
    require(lines == [NONE_YET, f"Snapshot blocked: {snapshot['message']}"], f"blocked status lines: {lines!r}")
    # The daemon log holds severity and category only, never message text, so the evidence there is one
    # new WARN event. The text is in the status and in the error of the request.
    logged = len(device.warnings(all_lines=True))
    require(logged > warned, "the daemon log holds no new WARN event for the refusal")
    # No loop: a few seconds of sync turns send no further refused request, and writes still go up.
    refused = refusals(relay)
    require(refused == 1, f"the relay refused {refused} blob.put requests for one snapshot request")
    time.sleep(QUIET_SECONDS)
    again = refusals(relay)
    require(again == refused, f"the node tried the refused snapshot again: {again} refusals, was {refused}")
    require(len(device.warnings(all_lines=True)) == logged, "the daemon log kept warning while the snapshot waits")
    device.write([create_big(BIG_RECORDS)])
    wait("the entries to go up while the snapshot is refused", device.synced_up)
    require(snapshot_object(device)["state"] == "blocked", "the blocked state was lost")
    print(f"15. a relay that refuses a blob over {RELAY_LIMIT_BYTES // 1024} KiB: the node reports the run file and "
          "its size, says the relay limit is smaller, tries once, and still uploads entries", flush=True)
    return {"message": snapshot["message"], "refused_requests": refused, "relay_limit_bytes": RELAY_LIMIT_BYTES,
            "request_exit": asked.returncode}


def refusals(relay):
    """How many blob.put requests the relay refused with HTTP 400."""
    return sum(1 for line in relay.lines() if "route=blob/put status=400" in line)


def create_big(index):
    """A record with a note of BIG_NOTE_BYTES of hex, so that the data file passes the limit of the relay."""
    note = os.urandom(BIG_NOTE_BYTES // 2).hex()
    return v1.create_item(ALPHA, {"hash": f"big-{index}"}, {"id": f"big-{index}", "note": note})
