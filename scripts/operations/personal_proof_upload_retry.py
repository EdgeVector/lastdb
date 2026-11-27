"""Step 19 of the personal database proof: an upload that fails before an answer is sent again; an answer is final.

A record with a file attached sends the file as one sealed blob, the call a snapshot file uses. The
reference relay is run with a fault for large uploads:

    close-once:  the relay drops the connection of the first large upload after part of its body, once,
                 as an object store does that closed a connection. The node sends the file again, from
                 its start, to the same URL, and the home is synced up. The relay log shows the closed
                 upload and one completed upload of the file.
    400:         the relay answers every large upload with HTTP 400 (BadDigest). The node does not send
                 the file again within the call: each sync turn makes one `blob.put` begin and one
                 upload, so the 400 answers equal the begins, and the error says "blob upload HTTP 400".
"""
from pathlib import Path
import json
import os
import subprocess
import time

from personal_proof_cleanup import cleanup_owned
from personal_proof_nodes import FIELDS, GAMMA, Device, Relay, require, wait
from personal_proof_steps import gamma_create
from personal_proof_uplink import home_with_file
from record_runs import RANGED, definition, system_seed

FILE_BYTES = 1_000_000


def count(relay, text):
    return sum(1 for line in relay.lines() if text in line)


def large_uploads(relay):
    """Completed uploads of at least FILE_BYTES: the file, not the log segments that carry the entries."""
    return sum(1 for line in relay.lines()
               if "upload_bytes=" in line and int(line.split("upload_bytes=")[1].split()[0]) >= FILE_BYTES)


def file_begins(relay):
    """(refused uploads, begins of the file), from one read of the relay log.

    A `blob/put` begin is a put that is followed by an upload before the next put. A commit has no
    upload after it, so it is not a begin. Log segments that sync before the large file complete
    (begin, upload, commit); subtracting their successful uploads leaves only the file begins. A
    sync turn can be between its begin and its upload when the log is read, so a last put with no
    upload after it is left out.
    """
    lines = relay.lines()
    puts = [i for i, line in enumerate(lines) if "route=blob/put status=200" in line]
    uploads = [i for i, line in enumerate(lines) if "route=blob/upload" in line]
    begins = 0
    for index, at in enumerate(puts):
        until = puts[index + 1] if index + 1 < len(puts) else len(lines)
        if any(at < upload < until for upload in uploads):
            begins += 1
    uploaded = sum(1 for line in lines if "route=blob/upload status=200" in line)
    refused = sum(1 for line in lines if "route=blob/upload status=400" in line)
    return refused, begins - uploaded


def home_with_file_and_phrase(device, size):
    """Initialize a home, write one record with an attached file of `size` bytes, and return its phrase."""
    phrase = device.init()
    device.start()
    system_seed(device.node, definition(GAMMA, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Personal proof "))
    source = device.out / "attachment.bin"
    source.write_bytes(os.urandom(size))
    attached = {"path": str(source), "name": "uplink.bin", "media_type": "application/x-proof"}
    device.write([gamma_create("f1", "grp-f", "uplink", file=attached)])
    return phrase


def prove_pull_preserves_blocked_upload(args, out, refusing, two, phrase):
    """A subsequent successful download from a sibling device never clears or masks a blocked upload."""
    peer = Device("device-u", args, out, refusing)
    try:
        peer.restore(phrase)
        peer.start()
        system_seed(peer.node, definition(GAMMA, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Personal proof "))
        peer.write([gamma_create("peer-ok", "grp-f", "small peer write")])
        wait("the peer device to upload its small write", peer.synced_up)
        wait("the refused home to pull the peer write while its own upload stays blocked",
             lambda: two.has_schema(GAMMA) and "peer-ok" in two.keys(GAMMA))
        sync = two.sync_status()
        require(isinstance(sync, dict), f"sync status is not a dict: {sync!r}")
        top_err = sync.get("last_error") or ""
        require("blob upload HTTP 400" in top_err,
                f"a successful pull cleared top-level cloud_sync.last_error: {sync!r}")
        db = (sync.get("databases") or {}).get(two.database) or {}
        upload = db.get("upload") or {}
        download = db.get("download") or {}
        oldest = upload.get("oldest_blocked") or {}
        require("blob upload HTTP 400" in (upload.get("last_error") or ""),
                f"upload.last_error lost after pull: {db!r}")
        require((oldest.get("writer_seq") or 0) >= 1 and "blob upload HTTP 400" in (oldest.get("reason") or ""),
                f"upload.oldest_blocked missing or wrong: {oldest!r}")
        require(download.get("last_error") is None and (download.get("applied_through") or 0) >= 1,
                f"download progress did not record the clean pull: {download!r}")
        require(isinstance(db.get("snapshot"), dict) and isinstance(db.get("cleanup"), dict),
                f"per-database snapshot or cleanup progress missing: {db!r}")
        env = {k: v for k, v in os.environ.items() if not k.startswith("LASTDB")}
        env.update(LASTDB_SOCKET_PATH=str(two.node.socket), LASTDB_HOME=str(two.node.home))
        cli_status = subprocess.run([str(two.cli), "status"], capture_output=True, text=True, timeout=60, check=False, env=env)
        require(cli_status.returncode == 0, f"lastdb status failed: {cli_status.stderr}")
        lines = cli_status.stdout.splitlines()
        require(any(line.startswith("Cloud sync:") and "blob upload HTTP 400" in line for line in lines),
                f"lastdb status Cloud sync line hid the upload error: {lines!r}")
        require(any(line.startswith("Cloud upload blocked (") and "blob upload HTTP 400" in line for line in lines),
                f"lastdb status missing blocked upload line: {lines!r}")
    finally:
        cleanup_owned((peer,))


def prove_upload_retry(args):
    out = args.output
    roots = {name: out / name for name in ("relay7", "relay8")}
    for root in roots.values():
        root.mkdir()
    closing = Relay(Path(args.relay_bin), roots["relay7"], extra=["--upload-fault", "close-once"])
    refusing = Relay(Path(args.relay_bin), roots["relay8"], extra=["--upload-fault", "400"])
    one, two = Device("device-s", args, out, closing), Device("device-t", args, out, refusing)
    try:
        closing.start()
        refusing.start()
        started = time.monotonic()
        home_with_file(one, FILE_BYTES)
        phrase = home_with_file_and_phrase(two, FILE_BYTES)
        wait("the home to sync up after its connection was closed once", one.synced_up)
        seconds = time.monotonic() - started
        closed, completed = count(closing, "relay upload_closed"), large_uploads(closing)
        require(closed == 1 and completed == 1, f"closed uploads {closed}, completed uploads {completed}")
        wait("the refused upload to be reported", lambda: "blob upload HTTP 400" in json.dumps(two.sync_status()))
        time.sleep(6)
        refused, begins = file_begins(refusing)
        require(refused >= 1 and refused == begins, f"{refused} refused uploads for {begins} begins: a 400 was sent again")
        require(not two.synced_up(), "the refused home reports its writes as uploaded")
        prove_pull_preserves_blocked_upload(args, out, refusing, two, phrase)
        print(f"19. a connection closed in the middle of the first upload of a file: the node sent the file again and "
              f"synced up in {seconds:.0f} s (1 closed upload, 1 completed); a 400 answer was not retried "
              f"({refused} refused uploads for {begins} begins), and a subsequent pull kept the blocked upload reported",
              flush=True)
        return {"closed": closed, "completed": completed, "refused": refused, "begins": begins}
    except Exception:
        for device in (one, two):
            print(f"{device.name} sync status: {device.sync_status()}", flush=True)
        raise
    finally:
        cleanup_owned((one, two), (closing, refusing))
