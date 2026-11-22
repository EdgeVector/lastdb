"""Steps 1 to 7 of the personal database proof: writes, order by edit time, restores, the relay's
blindness, alternate keys and files, and a restart with the relay stopped."""
import hashlib
import os
from pathlib import Path
import time

from personal_proof_nodes import (
    ALPHA, BETA, ENVELOPE_OVERHEAD, FIELDS, FILE_MARK, GAMMA, GAMMA_KEYS, GROUPS, MARK, NAME_MARK, SCHEMAS,
    converged, create, delete, expect, relay_files, relay_holds_none, replace, wait)
from record_runs import RANGED, definition, held, require, system_seed
import v1_records as v1


# 1. Writes ---------------------------------------------------------------------------------

def single_and_batched_writes(a, relay, state):
    """1. Singles and batches in two schemas: one signed entry for each write."""
    for schema in SCHEMAS:
        system_seed(a.node, definition(schema, "Hash", FIELDS, description_prefix="Personal proof "))
    schema_writes = a.spool_state()["head"]
    mutations = 0
    for call in ([create(ALPHA, "a1", "one")], [create(ALPHA, "a2", "one")],
                 [create(ALPHA, "b1", "base"), create(ALPHA, "b2", "base"), create(ALPHA, "b3", "base"),
                  create(BETA, "c1", "one"), create(BETA, "c2", "one")],
                 [create(BETA, "d1", "one")]):
        a.write(call)
        mutations += len(call)
    a.change([replace(ALPHA, "a1", "two")])
    a.change([delete(ALPHA, "a2")])
    a.write([create(BETA, "e1", "one"), create(BETA, "e2", "one"), create(BETA, "e3", "one")])
    mutations += 5
    for key, text in (("a1", "two"), ("b1", "base"), ("b2", "base"), ("b3", "base")):
        expect(state, ALPHA, key, text)
    for key in ("c1", "c2", "d1", "e1", "e2", "e3"):
        expect(state, BETA, key, "one")
    require(a.snapshot() == state, "A does not hold what it wrote")
    signed = schema_writes + mutations
    wait("A to upload every entry", a.synced_up)
    spool = a.spool_state()
    require(spool["head"] == signed, f"A's spool holds {spool['head']} writes, expected {signed}")
    sequences = relay.stream_entries(a.database, a.device_id())
    require(sequences == list(range(1, signed + 1)),
            f"the relay holds {len(sequences)} entries of A, expected one for each of {signed} writes")
    require(a.checkpoint()["writer_seq"] == signed, f"A confirmed {a.checkpoint()!r}")
    require(a.spool_bytes()[1] == 0, "A's spool still holds entries after the relay confirmed them")
    print(f"1. A wrote singles and batches in two schemas: {signed} writes, {signed} signed entries on the "
          "relay, and an empty spool", flush=True)
    return signed


# 3. Order by edit time ---------------------------------------------------------------------

def offline_pair(relay, first, second, edits):
    """Both devices edit while the relay is away: `first` earlier in real time, `second` later.

    `edits` is (first's mutations, second's mutations). The device with more writes is `first`, so
    a count of writes cannot decide. Returns nothing: the caller syncs in the order it wants.
    """
    relay.stop()
    first.change(edits[0])
    time.sleep(1.5)
    second.change(edits[1])


def sync_in_order(relay, uploads_first, other):
    """Bring the relay back. `uploads_first` signs and uploads while `other` is stopped, then `other` joins."""
    other.stop()
    relay.start()
    wait(f"{uploads_first.name} to upload", uploads_first.synced_up)
    other.start()
    wait(f"{other.name} to upload", other.synced_up)


def order_by_edit_time(relay, a, b, state):
    """3. The later edit wins whichever syncs first, and a delete is not undone by an older edit."""
    # A has made many more writes than B, so a count of writes would pick A.
    a.write([create(ALPHA, f"filler-{i}", "x") for i in range(8)])
    for i in range(8):
        expect(state, ALPHA, f"filler-{i}", "x")
    wait("A to upload", a.synced_up)
    wait("B to hold the filler", lambda: converged((b,), state))
    outcomes = {}
    for key, label, uploads_first, other in (("order-first", "earlier edit syncs first", a, b),
                                             ("order-last", "earlier edit syncs last", b, a)):
        a.write([create(ALPHA, key, "base")])
        expect(state, ALPHA, key, "base")
        wait("B to hold the base", lambda: converged((b,), state))
        offline_pair(relay, a, b, ([replace(ALPHA, key, "from-A-earlier")], [replace(ALPHA, key, "from-B-later")]))
        sync_in_order(relay, uploads_first, other)
        expect(state, ALPHA, key, "from-B-later")
        wait(f"both to hold the later edit ({label})", lambda: converged((a, b), state))
        outcomes[label] = "from-B-later"
    # A delete of A, then an older edit of B that arrives after the delete was applied.
    a.write([create(ALPHA, "doomed", "base")])
    expect(state, ALPHA, "doomed", "base")
    wait("B to hold the doomed record", lambda: converged((b,), state))
    relay.stop()
    b.change([replace(ALPHA, "doomed", "older-edit-of-B")])
    time.sleep(1.5)
    a.change([delete(ALPHA, "doomed")])
    del state[(ALPHA, f"{MARK}-key-doomed")]
    sync_in_order(relay, a, b)
    wait("the delete to stay", lambda: converged((a, b), state))
    time.sleep(8)
    require(converged((a, b), state), "an older edit brought a deleted record back")
    print("3. The later edit won whichever device synced first, with A holding more writes than B; "
          "a deleted record stayed deleted when an older edit arrived after it", flush=True)
    return outcomes


# 6. Alternate keys and files ---------------------------------------------------------------

def gamma_create(key, group, text, file=None):
    address = {"by_group": {"hash": group, "range": key}}
    return v1.create_item(GAMMA, {"hash": key}, {"id": key, "note": text}, alternates=address, file=file)


def gamma_replace(key, group, text):
    """A replace names every declared alternate key. It names no file, so the record keeps its file."""
    return {"schema": GAMMA, "op": "replace", "key": {"hash": key}, "record": {"id": key, "note": text},
            "alternates": {"by_group": {"hash": group, "range": key}}}


def gamma_view(device, directory):
    """What this device reads: each group by the alternate key, each record, and the file of f1."""
    groups = {}
    for group in GROUPS:
        page = v1.read_items(device.node, [v1.range_read(GAMMA, {"hash": group}, via="by_group", limit=100)])[0]
        groups[group] = sorted(row["key"]["hash"] for row in page["records"])
    pages = v1.read_all(device.node, [v1.point(GAMMA, {"hash": key}) for key in GAMMA_KEYS])
    records = {key: (held(page["records"][0]["record"]) if page["records"] else None)
               for key, page in zip(GAMMA_KEYS, pages)}
    item = v1.point(GAMMA, {"hash": "f1"}, file=True)
    item["file_dir"] = str(directory)
    rows = v1.read_items(device.node, [item])[0]["records"]
    if not rows or not rows[0].get("file") or not rows[0]["file"].get("path"):
        return {"groups": groups, "records": records, "file": None}
    info = rows[0]["file"]
    data = Path(info["path"]).read_bytes()
    return {"groups": groups, "records": records,
            "file": {"name": info["name"], "media_type": info["media_type"], "size": len(data),
                     "sha256": hashlib.sha256(data).hexdigest()}}


def gamma_seen(device, directory):
    """The view, or None while the device does not hold the schema or the file yet."""
    try:
        return gamma_view(device, directory)
    except RuntimeError:
        return None


def alternates_and_files(relay, a, b, c, d, phrase, out):
    """6. Alternate keys and files reach B, C and a home restored afterwards."""
    source = out / "source-files"
    source.mkdir(mode=0o700)
    content = FILE_MARK.encode() + os.urandom(200_000)
    path = source / "attachment.bin"
    path.write_bytes(content)
    system_seed(a.node, definition(GAMMA, "Hash", FIELDS, {"by_group": RANGED}, description_prefix="Personal proof "))
    a.write([gamma_create("g1", "grp-x", "g1-one")])
    a.write([gamma_create("g2", "grp-x", "g2-one"), gamma_create("g3", "grp-y", "g3-one")])
    attached = {"path": str(path), "name": f"{NAME_MARK}.bin", "media_type": "application/x-proof"}
    a.write([gamma_create("f1", "grp-f", "f1-one", file=attached)])
    a.change([gamma_replace("g1", "grp-y", "g1-two")])
    a.change([{"schema": GAMMA, "op": "delete", "key": {"hash": "g3"}}])
    a.change([gamma_replace("f1", "grp-f", "f1-two")])
    expected = {
        "groups": {"grp-x": ["g2"], "grp-y": ["g1"], "grp-f": ["f1"]},
        "records": {"g1": {"id": "g1", "note": "g1-two"}, "g2": {"id": "g2", "note": "g2-one"}, "g3": None,
                    "f1": {"id": "f1", "note": "f1-two"}},
        "file": {"name": f"{NAME_MARK}.bin", "media_type": "application/x-proof", "size": len(content),
                 "sha256": hashlib.sha256(content).hexdigest()},
    }
    wait("A to upload every entry", a.synced_up)
    folders = {}
    for device in (a, b, c, d):
        folders[device.name] = device.out / "file-dir"
        folders[device.name].mkdir(mode=0o700, exist_ok=True)
    require(gamma_view(a, folders[a.name]) == expected, f"A reads {gamma_view(a, folders[a.name])!r}")
    for device in (b, c):
        wait(f"{device.name} to read the alternate keys and the file of A",
             lambda device=device: gamma_seen(device, folders[device.name]) == expected)
    restored = d.restore(phrase)
    d.start()
    # The restore fetched the file blob and put the file into the store before it returned.
    relay.stop()
    for device in (a, b, c, d):
        found = gamma_view(device, folders[device.name])
        require(found == expected, f"{device.name} reads {found!r} with the relay stopped, expected {expected!r}")
    relay.start()
    # The relay saw neither the names, nor the alternate addresses, nor the file.
    relay_holds_none(relay, [GAMMA, NAME_MARK, FILE_MARK, "grp-x", "grp-y", "grp-f", "g1-two", "f1-two"])
    sealed = [file for file in relay_files(relay) if file.stat().st_size == len(content) + ENVELOPE_OVERHEAD]
    require(len(sealed) == 1, f"the relay holds {len(sealed)} blobs of the file's sealed size, expected one")
    print("6. B, C and the restored home D read the records by their alternate key and read the file "
          "with the relay stopped; the relay holds one sealed blob and no name or marker", flush=True)
    return {"restore_d": restored, "file_bytes": len(content), "sealed_blob_bytes": sealed[0].stat().st_size}


# 7. Restart --------------------------------------------------------------------------------

def restart_a(relay, a, b, c, state):
    """7. A restarts without the relay and signs the next entry on the right sequence."""
    wait("A to upload", a.synced_up)
    relay.stop()
    before = a.checkpoint()["writer_seq"]
    a.stop()
    a.start()
    require(a.snapshot() == state, "A lost records across its restart")
    a.write([create(BETA, "after-restart", "x")])
    expect(state, BETA, "after-restart", "x")
    relay.start()
    wait("A to upload the entry after its restart", a.synced_up)
    after = a.checkpoint()["writer_seq"]
    require(after == before + 1, f"A continued its stream at {after} after {before}")
    require(relay.stream_entries(a.database, a.device_id())[-1] == after, "the relay lacks the entry A signed after its restart")
    wait("B and C to hold A's write after its restart", lambda: converged((a, b, c), state))
    print(f"7. A restarted with the relay stopped and signed entry {after} after {before}; B and C hold it", flush=True)
