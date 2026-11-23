#!/usr/bin/env python3
"""The personal database on the signed log, across real nodes and the reference relay.

Devices A, B, C and D are real `lastdbd` processes. They sync through one `reference_relay` that
this script stops and starts again. Each device signs the writes of its spool of accepted writes
as its own stream and applies the other streams through the store's last-writer stamp.

1. A writes records in two schemas: singles and batches. The relay then holds one signed entry
   for each write, and the spool of A is empty.
2. B starts from the recovery phrase alone (`lastdb backup restore`) and holds every record.
3. Order by edit time. With the relay stopped, A makes more writes than B and edits a record,
   and B edits the same record later. Both hold the value of B afterwards, whichever of the two
   syncs first. A record that A deleted does not come back when an older edit of B arrives after.
4. A third home restored from the phrase equals A and B, key by key.
5. The bytes the relay stored hold no record key, no record value and no schema name.
6. Alternate keys and files: B, C and a fourth home D read the records of A by their alternate
   key, after a move and a delete, and read the file of a record with the relay stopped.
7. A restarts with the relay stopped, and signs the next entry on the right sequence. The relay
   accepts it once it is back.
8. Restart replays nothing: after a restart with everything synced, the first turns read no entry
   from the relay and write nothing to the store. An idle turn reads no entry.
9. Kill -9 under write load: the relay holds each write once, a second device equals the first,
   and the spool is empty.
21. The relay holds what the nodes say they put there, and weighs what they say (`personal_proof_reconcile.py`,
    after the trim step and at the end). Every stream holds exactly its floor to its acknowledged head with an intact
    hash chain, every blob is accounted for, and the stored bytes equal the accounts the nodes keep. A deployed relay
    gets the same questions through `log/streams`, `log/read` and `blob.get`.
11. The snapshot is the checkpoint. A takes a snapshot, then writes more. A home restored from the
    phrase installs the snapshot and reads only the entries after it, and equals a home restored by
    full replay from a relay with no snapshot, key by key, alternate keys and file bytes included.
12. A restore killed at several points leaves no home or a complete one, and a rerun completes it.
13. A second snapshot uploads only the files that changed, the newest is the one restored, and no
    byte the relay stored holds a marker written into a record or a file.
14. A restored device writes and A receives it; A writes and the restored device receives it.
15. The status names the snapshot. A fresh home says `Snapshot: none yet`. After a snapshot the line
    and the `cloud_sync.snapshot` object of `/api/status` match the snapshot just taken; a restored
    home reports the snapshot it installed. A relay that refuses a file over its limit (as Exemem
    does, HTTP 400 `invalid_request` on `bytes`) gets one try, a report that names the file and its
    size, and no loop; entries still upload.
16. The relay drops what a snapshot covers (`log/trim`, called by the node after each snapshot for the
    second newest) and still restores. After two snapshots the relay holds no entry at or before the
    positions of the older one, on any stream, and keeps the birth, both snapshots and what follows.
    A device that stopped before those positions is told it is behind and applies nothing. A restore
    from the phrase installs the newest snapshot, replays only the entries after it, and equals A key
    by key, also after the relay restarts. A late repeat of an append below the floor is answered as a duplicate, not stored, and a
    read there is 410. A third snapshot drops the first snapshot entry and no
    blob (its manifest and files stay), and a restore from the second and from the third snapshot each equal A. A, B and F are never told they
    are behind.
17. A relay answer that says try again is tried again. The first `log/streams` request of a relay gets
    HTTP 429 `rate_limited` (`provider.dynamo.conflict`, as when two transactions collide): a running
    daemon and a restore still succeed, and each request signs again. A quota refusal (`quota.bytes`)
    is also a 429, and the restore fails at once with the relay's message.
18. A transfer fails when the connection stalls, not when it is slow. A snapshot whose file reaches the
    relay at about 32 KiB/s takes longer than the old allowance (30 s) and completes; an upload that the
    relay stops reading fails after 30 s with a message that says how many of how many bytes were sent.
19. An upload that fails before an answer (the store closed the connection) is sent again from its start to
    the same URL, at most 3 tries in all; an HTTP status from the store (400) is final and is not sent again.
20. Entries go on while a snapshot's files upload. With the relay reading a large upload at 24 KiB/s, the
    writes of A are acknowledged and read by B before the relay holds the snapshot entry. A kill -9 in the
    upload loses and repeats nothing, and the published snapshot names the positions of its cut.
22. A record and its file written on several devices at once (`personal_proof_merge.py`). The newest
    text wins, and the file is the one of the newest write that set a file or deleted the record, on
    every device and whichever device syncs first.
10. Scale: tens of thousands of writes. The peak resident memory of the daemons does not grow with
    the number of entries, the spool returns to near empty, and an idle turn reads nothing.

With `--relay-url` the relay is a parameter: a deployed relay (the Exemem DEV relay) instead of a
`reference_relay` process. The identity is fresh, so the proof writes to a personal database of its
own. `personal_proof_deployed.py` runs the steps that need no access to the relay process, and says
which steps it does not run.

A failure prints each node's spool state and its recent daemon warnings.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import time

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from harness import append_step_summary  # noqa: E402
from personal_proof_load import idle_turn_reads_nothing, kill_under_load, scale  # noqa: E402
from personal_proof_deployed import prove_deployed, render  # noqa: E402
from personal_proof_nodes import DELTA, DeployedRelay, Relay, converged, prove_hidden, require, wait  # noqa: E402
from personal_proof_reconcile import reconcile_reference, remember  # noqa: E402
from personal_proof_trim import trimmed_relay_restores  # noqa: E402
from personal_proof_usage import AccountedDevice as Device  # noqa: E402
from personal_proof_status import prove_none_yet, refused_snapshot, RELAY_LIMIT_BYTES  # noqa: E402
from personal_proof_retries import prove_retries  # noqa: E402
from personal_proof_lane import prove_lane  # noqa: E402
from personal_proof_merge import prove_merge  # noqa: E402
from personal_proof_uplink import prove_uplink  # noqa: E402
from personal_proof_upload_retry import prove_upload_retry  # noqa: E402
from personal_proof_snapshot import (  # noqa: E402
    exchange_writes, kill_the_restore, scale_snapshot, second_snapshot, snapshot_restore_equals_replay)
from personal_proof_steps import (  # noqa: E402
    alternates_and_files, order_by_edit_time, restart_a, single_and_batched_writes)


# Run ---------------------------------------------------------------------------------------

def prove(relay, relay2, relay3, relay4, a, b, c, d, e, f, g, h, j, k, m, n, p, out, args):
    report = {}
    phrase = a.init()
    a.start()
    prove_none_yet(a)
    # The relay answers 403 to a list of the streams of a database whose birth it does not hold.
    # For an owner who has not written anywhere that is an empty relay, not a failed turn.
    time.sleep(6)
    require("personal database sync failed" not in "\n".join(a.warnings(all_lines=True)),
            "A's first turns with an empty relay failed")
    state = {}

    signed = single_and_batched_writes(a, relay, state)

    # 2. B starts from the recovery phrase alone.
    restored = b.restore(phrase)
    require(restored["devices"] == 1 and restored["replayed"] == signed, f"B restore: {restored!r}")
    b.start()
    require(b.database == a.database, "B has another personal database id than A")
    wait("B to hold every record", lambda: b.snapshot() == state)
    report["restore_b"] = restored
    print("2. B restored from the phrase alone, replayed every entry of A, and holds every record", flush=True)

    report["order"] = order_by_edit_time(relay, a, b, state)

    # 4. A third home from the phrase equals both.
    wait("A and B to upload", lambda: a.synced_up() and b.synced_up())
    restored = c.restore(phrase)
    require(restored["devices"] == 2, f"C restore: {restored!r}")
    c.start()
    wait("C to equal A and B", lambda: converged((a, b, c), state))
    report["restore_c"] = restored
    print(f"4. C restored from the phrase ({restored['devices']} device streams) and equals A and B, key by key", flush=True)

    # 5. The relay cannot read any of it.
    wait("A and B to upload", lambda: a.synced_up() and b.synced_up())
    report["relay"] = prove_hidden(relay, state, signed)
    print(f"5. no key, value or schema name in {report['relay']['relay_files']} relay files", flush=True)

    report["alternates_and_files"] = alternates_and_files(relay, a, b, c, d, phrase, out)
    report["merge"] = prove_merge(relay, a, b, c, out)
    restart_a(relay, a, b, c, state)
    # Only A and B are needed from here on.
    c.stop()
    d.stop()
    report["snapshot"], folders, expected = snapshot_restore_equals_replay(relay, relay2, (a, b, e, f, g, h, j), phrase, state)
    report["restore_killed"] = kill_the_restore(g, phrase, a, state, folders, expected)
    # A trim drops entries and keeps the blobs they name: note the files they name first.
    remember(relay, a.database)
    report["second_snapshot"] = second_snapshot(relay, a, b, f, h, phrase, state, folders, expected)
    exchange_writes(relay, a, f, state, folders, expected)
    report["trim"] = trimmed_relay_restores(relay, relay2, a, b, c, f, g, phrase, state, folders, expected)
    on_relay = [a, b, c, d, f, g, h, j]
    report["reconcile_after_trim"] = reconcile_reference(relay, a, on_relay, "21. after the trim")
    # The relay that refuses a large blob, with a home of its own.
    k.init()
    k.start()
    report["refused_snapshot"] = refused_snapshot(relay3, k)
    k.stop()
    relay3.stop()
    report["retries"] = prove_retries(relay4, m, n, p)
    relay4.stop()
    report["uplink"] = prove_uplink(args)
    report["upload_retry"] = prove_upload_retry(args)
    report["lane"] = prove_lane(args)
    # Only A and B again: the homes restored for the proof are done.
    for device in (e, f, h):
        device.stop()
    relay2.stop()
    report["idle"] = idle_turn_reads_nothing(relay, a)
    report["kill"] = kill_under_load(relay, a, b, args.kills)
    report["scale"] = scale(relay, a, b, args.scale_writes)
    report["scale_snapshot"] = scale_snapshot(relay, a, j, phrase, DELTA)
    report["reconcile"] = reconcile_reference(relay, a, on_relay, "21. at the end")
    (out / "summary.json").write_text(json.dumps(report, indent=2))


def dump(devices):
    for device in devices:
        print(f"{device.name} spool:", file=sys.stderr)
        print("\n".join(device.entries()), file=sys.stderr)
        print(f"{device.name} log (last 25 warnings):", file=sys.stderr)
        print("\n".join(device.warnings()), file=sys.stderr)


def run_deployed(args):
    """The proof against a deployed relay, then its summary (printed, and appended to the job summary)."""
    relay = DeployedRelay(args.relay_url)
    devices = [Device(name, args, args.output, relay) for name in ("device-a", "device-b", "device-c", "device-d")]
    failure = None
    try:
        report = prove_deployed(relay, devices, args.output, args.entry_audit)
        text = render(report)
        (args.output / "summary.md").write_text(text)
        append_step_summary(text)
        print(text, flush=True)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        dump(devices)
    finally:
        for device in devices:
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}", file=sys.stderr)
    if failure is not None:
        raise SystemExit(1)
    print("Personal database proof passed against the deployed relay", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--daemon", required=True)
    parser.add_argument("--cli", required=True)
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument("--relay-bin", help="run the scenarios against a reference_relay process of this binary")
    where.add_argument("--relay-url", help="run the deployed-relay scenarios against this relay origin")
    parser.add_argument("--entry-audit", type=Path, help="the entry_audit binary (deployed relay run; the relay run finds it beside --relay-bin)")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scale-writes", type=int, default=int(os.environ.get("PROOF_SCALE_WRITES", "20000")),
                        help="writes of the scale step (default 20000, or PROOF_SCALE_WRITES)")
    parser.add_argument("--kills", type=int, default=5, help="kill -9 rounds under write load")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.relay_url:
        return run_deployed(args)
    relay = Relay(Path(args.relay_bin), args.output)
    relay.start()
    # The relay with no snapshot stream: a copy of the first, made when the proof asks for it.
    relay2_root = args.output / "relay2"
    relay2_root.mkdir()
    relay2 = Relay(Path(args.relay_bin), relay2_root)
    # The relay that refuses a blob over a limit, as Exemem does, for the oversized snapshot file.
    relay3_root = args.output / "relay3"
    relay3_root.mkdir()
    relay3 = Relay(Path(args.relay_bin), relay3_root, max_blob_bytes=RELAY_LIMIT_BYTES)
    relay3.start()
    # The relay that answers a first request with a 429 that says try again, for step 17.
    relay4_root = args.output / "relay4"
    relay4_root.mkdir()
    relay4 = Relay(Path(args.relay_bin), relay4_root)
    devices = [Device(name, args, args.output, relay) for name in ("device-a", "device-b", "device-c", "device-d")]
    devices.append(Device("device-e", args, args.output, relay2))
    devices += [Device(name, args, args.output, relay) for name in ("device-f", "device-g", "device-h", "device-j")]
    devices.append(Device("device-k", args, args.output, relay3))
    devices += [Device(name, args, args.output, relay4) for name in ("device-m", "device-n", "device-p")]
    failure = None
    try:
        prove(relay, relay2, relay3, relay4, *devices, args.output, args)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        dump(devices)
    finally:
        for device in devices:
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}", file=sys.stderr)
        relay.stop()
        relay2.stop()
        relay3.stop()
        relay4.stop()
    if failure is not None:
        raise SystemExit(1)
    print("Personal database proof passed", flush=True)


if __name__ == "__main__":
    main()
