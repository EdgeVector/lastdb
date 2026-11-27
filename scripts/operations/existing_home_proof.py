#!/usr/bin/env python3
"""A home that already holds data gets a relay and publishes its first snapshot.

Real processes only: a `lastdbd` and a `reference_relay`. The home is made by `backup init` and
written with no relay at all, so it holds data and no spool. The same daemon then starts on the
same home with the relay set. Because the home held data before it began to capture writes, it
publishes its files as a first snapshot before any later write goes up.

1. The home holds records in two schemas, one of them with an alternate key and a file, after
   creates, replaces and deletes. The daemon stops cleanly.
2. The daemon starts with the relay. It names the first snapshot once and releases the preserved
   files. Every record reads by primary key and by alternate key, and the file's bytes match.
3. Writes, replaces and deletes after the snapshot are exactly the entries on the relay: one each,
   none for the records written before.
4. A restore from the recovery phrase equals the home key by key, installs the snapshot and
   replays only the entries after it.
5. The relay holds no record key, record value, schema name or file marker.
"""
import argparse
import hashlib
import os
from pathlib import Path
import sys

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from personal_proof_nodes import (  # noqa: E402
    ALPHA, BETA, FIELDS, FILE_MARK, GAMMA, GROUPS, MARK, NAME_MARK, Device, Relay, converged, create, delete, expect,
    relay_holds_none, replace, wait)
from personal_proof_steps import gamma_create, gamma_replace, gamma_view  # noqa: E402
from record_runs import RANGED, definition, require, system_seed  # noqa: E402

RECORDS = 400
BATCH = 100


def first_snapshot_done(device):
    """The home named its first snapshot and released the preserved files. Read from the running daemon and its home."""
    state = device.spool_state()
    return (state is not None and state.get("snapshot_required") is False
            and (device.log_dir() / "snapshot-last.json").is_file()
            and not (device.node.home / ".signed-initial-snapshot").exists())


def snapshots_on(relay, database):
    found = relay.data / "logs" / database
    if not found.is_dir():
        return 0
    total = 0
    for path in found.glob("*/*/*.json"):
        try:
            text = path.read_text()
        except FileNotFoundError:
            continue
        if '"kind":"snapshot"' in text:
            total += 1
    return total


def write_before(device, folder, source, state):
    """1. Records with no relay: nothing is captured, so the home holds data and no spool."""
    for schema in (ALPHA, BETA):
        system_seed(device.node, definition(schema, "Hash", FIELDS, description_prefix="Existing home "))
    system_seed(device.node, definition(GAMMA, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Existing home "))
    keys = [f"k{n:04d}" for n in range(RECORDS)]
    for start in range(0, RECORDS, BATCH):
        device.write([create(ALPHA, key, "one") for key in keys[start:start + BATCH]])
    for key in keys:
        expect(state, ALPHA, key, "one")
    device.write([create(BETA, key, "one") for key in keys[:50]])
    for key in keys[:50]:
        expect(state, BETA, key, "one")
    for key in keys[::7]:
        device.change([replace(ALPHA, key, "two")])
        expect(state, ALPHA, key, "two")
    for key in keys[::11]:
        device.change([delete(ALPHA, key)])
        state.pop((ALPHA, f"{MARK}-key-{key}"), None)
    content = FILE_MARK.encode() + os.urandom(200_000)
    path = source / "attachment.bin"
    path.write_bytes(content)
    attached = {"path": str(path), "name": f"{NAME_MARK}.bin", "media_type": "application/x-proof"}
    device.write([gamma_create("g1", "grp-x", "g1-one"), gamma_create("g2", "grp-x", "g2-one"),
                  gamma_create("f1", "grp-f", "f1-one", file=attached)])
    device.change([gamma_replace("g1", "grp-y", "g1-two")])
    gamma = {
        "groups": {"grp-x": ["g2"], "grp-y": ["g1"], "grp-f": ["f1"]},
        "records": {"g1": {"id": "g1", "note": "g1-two"}, "g2": {"id": "g2", "note": "g2-one"}, "g3": None,
                    "f1": {"id": "f1", "note": "f1-one"}},
        "file": {"name": f"{NAME_MARK}.bin", "media_type": "application/x-proof", "size": len(content),
                 "sha256": hashlib.sha256(content).hexdigest()},
    }
    require(device.snapshot() == state, "the home does not hold what it was given")
    require(gamma_view(device, folder) == gamma, "the home does not read its alternate keys and file")
    return keys, gamma


def write_after(device, keys, state):
    """3. Replaces, deletes and creates on the home after the snapshot. Returns the count of writes."""
    count = 0
    for key in keys[1:60:5]:
        if (ALPHA, f"{MARK}-key-{key}") not in state:
            continue
        device.change([replace(ALPHA, key, "later")])
        expect(state, ALPHA, key, "later")
        count += 1
    for key in keys[2:60:9]:
        if (ALPHA, f"{MARK}-key-{key}") in state:
            device.change([delete(ALPHA, key)])
            state.pop((ALPHA, f"{MARK}-key-{key}"))
            count += 1
    fresh = [f"new{n}" for n in range(10)]
    device.write([create(ALPHA, key, "new") for key in fresh])
    for key in fresh:
        expect(state, ALPHA, key, "new")
    return count + len(fresh)


def prove(relay, home, restored, out):
    folder = out / "file-dir"
    folder.mkdir(mode=0o700)
    source = out / "source-files"
    source.mkdir(mode=0o700)
    state = {}
    home.node.environment.pop("LASTDB_RELAY_URL")
    phrase = home.init()
    home.start()
    keys, gamma = write_before(home, folder, source, state)
    home.stop()
    print(f"1. the home holds {len(state)} records in two schemas and a third with an alternate key and a file, "
          "and has no relay and no spool", flush=True)

    home.node.environment["LASTDB_RELAY_URL"] = relay.url
    home.start()
    wait("the first snapshot of the home", lambda: first_snapshot_done(home), seconds=300)
    wait("the home to read what it held", lambda: converged((home,), state))
    require(snapshots_on(relay, home.database) == 1, "the first snapshot was named more than once")
    require(gamma_view(home, folder) == gamma, "after the snapshot the home does not read its alternate keys and file")
    print("2. the first snapshot is on the relay; every record reads by key and by alternate key, and the file matches",
          flush=True)

    writes = write_after(home, keys, state)
    wait("the home to upload", home.synced_up)
    spool = home.spool_state()
    require(spool["head"] == writes and spool["acked"] == writes, f"the spool holds {spool!r} for {writes} writes")
    sequences = relay.stream_entries(home.database, home.device_id())
    require(sequences == list(range(1, writes + 1)),
            f"the relay holds {len(sequences)} entries of the home for {writes} writes")
    print(f"3. {writes} writes after the snapshot are {writes} entries on the relay, one each, none for the old records",
          flush=True)

    mark = len(relay.lines())
    report = restored.restore(phrase)
    read = sum(count for _, count in relay.requests_since(mark, "log/read"))
    require(report["snapshot"] is not None, f"the restore found no snapshot: {report!r}")
    require(report["replayed"] == writes, f"the restore replayed {report['replayed']} entries, expected {writes}")
    require(read <= writes + 4 and len(state) > 4 * writes, f"the restore read {read} entries for {writes} later writes")
    restored.start()
    wait("the restored home to equal the home", lambda: converged((restored,), state))
    restored_folder = restored.out / "file-dir"
    restored_folder.mkdir(mode=0o700)
    require(gamma_view(restored, restored_folder) == gamma, "the restored home differs in alternate keys or file")
    print(f"4. a restore from the phrase installed the snapshot, replayed the {writes} later entries, and equals the home",
          flush=True)

    relay_holds_none(relay, [ALPHA, BETA, GAMMA, NAME_MARK, FILE_MARK, f"{MARK}-key-", f"{MARK}-value-", *GROUPS])
    print("5. the relay holds no record key, record value, schema name or file marker", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("daemon", "cli", "relay-bin", "revision"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    relay = Relay(Path(args.relay_bin), args.output)
    relay.start()
    home = Device("existing-home", args, args.output, relay)
    restored = Device("restored-home", args, args.output, relay)
    failure = None
    try:
        prove(relay, home, restored, args.output)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        for device in (home, restored):
            print(f"--- {device.name}: {device.entries()}", file=sys.stderr)
            print("\n".join(line[:400] for line in device.warnings()), file=sys.stderr)
    finally:
        for device in (home, restored):
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}", file=sys.stderr)
        relay.stop()
    if failure is not None:
        raise SystemExit(1)
    print("Existing home proof passed", flush=True)


if __name__ == "__main__":
    main()
