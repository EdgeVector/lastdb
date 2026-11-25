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
import time

from personal_proof_cleanup import cleanup_owned
from personal_proof_nodes import Device, Relay, require, wait
from personal_proof_uplink import home_with_file

FILE_BYTES = 1_000_000


def count(relay, text):
    return sum(1 for line in relay.lines() if text in line)


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
        home_with_file(two, FILE_BYTES)
        wait("the home to sync up after its connection was closed once", one.synced_up)
        seconds = time.monotonic() - started
        closed, completed = count(closing, "relay upload_closed"), count(closing, "upload_ms=")
        require(closed == 1 and completed == 1, f"closed uploads {closed}, completed uploads {completed}")
        wait("the refused upload to be reported", lambda: "blob upload HTTP 400" in json.dumps(two.sync_status()))
        time.sleep(6)
        refused, begins = count(refusing, "route=blob/upload status=400"), count(refusing, "route=blob/put status=200")
        require(refused >= 1 and refused == begins, f"{refused} refused uploads for {begins} begins: a 400 was sent again")
        require(not two.synced_up(), "the refused home reports its writes as uploaded")
        print(f"19. a connection closed in the middle of the first upload of a file: the node sent the file again and "
              f"synced up in {seconds:.0f} s (1 closed upload, 1 completed); a 400 answer was not retried "
              f"({refused} refused uploads for {begins} begins)", flush=True)
        return {"closed": closed, "completed": completed, "refused": refused, "begins": begins}
    except Exception:
        for device in (one, two):
            print(f"{device.name} sync status: {device.sync_status()}", flush=True)
        raise
    finally:
        cleanup_owned((one, two), (closing, refusing))
