"""Step 22 of the personal database proof: a record and its file, written on several devices at once.

A record has three parts that a write can set on its own: the fields, the file and the inline
file. A write that names no file keeps the file of the record. The newest write to each part
wins, on every device, whichever device syncs first:

- the fields are those of the write with the newest edit time;
- the file is the one of the newest write that set or cleared a file, or of the newest delete;
- a create or a replace that names no file does not change the file.

The cases, each with the relay stopped while the devices write:

a. A sets a file, B sets another file later, A edits the text last. Every device holds A's text
   and B's file, with A syncing first and with B syncing first.
b. A file arrives at a device that holds a later text edit it has not sent yet. A sets a file, B
   sets another file later, C edits the text after both. C syncs, then A. A edits the text again
   and stops before it sends. B syncs, then A. Every device holds A's last text and B's file.
c. Two devices create the same record, the earlier one with a file. Every device holds the text of
   the later create and the file of the earlier one, with either device syncing first.
d. A deletes a record that has a file, and B edits its text later. Every device holds B's text and
   no file: the delete is the newest write to the file.
e. A sets a new file, B clears the file (`"file": null`) later, and A edits the text last. Every
   device holds A's text and no file, with A syncing first and with B syncing first.
"""
import hashlib
import os
from pathlib import Path
import time

from personal_proof_nodes import FIELDS, wait
from personal_proof_steps import sync_in_order
from record_runs import definition, held, require, system_seed
import v1_records as v1

ZETA = "proofmarkerzeta"
THETA = "proofmarkertheta"
IOTA = "proofmarkeriota"
KAPPA = "proofmarkerkappa"
# Long enough for a turn that would change a settled record to run.
SETTLE_SECONDS = 8
_KEEP = object()


def zeta_create(key, text, file=None):
    return v1.create_item(ZETA, {"hash": key}, {"id": key, "note": text}, file=file)


def zeta_replace(key, text, file=_KEEP):
    """A replace that names no file keeps the file of the record; `file=None` clears it."""
    item = {"schema": ZETA, "op": "replace", "key": {"hash": key}, "record": {"id": key, "note": text}}
    if file is not _KEEP:
        item["file"] = file
    return item


def zeta_delete(key):
    return {"schema": ZETA, "op": "delete", "key": {"hash": key}}


def zeta_view(device, key, directory):
    """(text, SHA-256 of the file or None) of one record, or None when the device holds no record."""
    item = v1.point(ZETA, {"hash": key}, file=True)
    item["file_dir"] = str(directory)
    rows = v1.read_items(device.node, [item])[0]["records"]
    if not rows:
        return None
    text = held(rows[0]["record"]).get("note")
    info = rows[0].get("file")
    if not info or not info.get("path"):
        return (text, None)
    return (text, hashlib.sha256(Path(info["path"]).read_bytes()).hexdigest())


def zeta_seen(device, key, directory):
    """The view, or the error text while the device does not hold the schema or the file yet."""
    try:
        return zeta_view(device, key, directory)
    except RuntimeError as error:
        return f"unreadable: {error}"


class Merge:
    """The devices, their read folders and the two files of the step."""

    def __init__(self, relay, devices, out):
        self.relay, self.devices = relay, devices
        self.folders = {}
        for device in devices:
            self.folders[device.name] = device.out / "merge-dir"
            self.folders[device.name].mkdir(mode=0o700, exist_ok=True)
        source = out / "merge-files"
        source.mkdir(mode=0o700)
        self.files, self.digests = {}, {}
        for index, name in enumerate(("one", "two", "lose")):
            data = os.urandom(40_000 + index * 1_000)
            path = source / f"{name}.bin"
            path.write_bytes(data)
            self.files[name] = {"path": str(path), "name": f"merge-{name}.bin", "media_type": "application/x-proof"}
            self.digests[name] = hashlib.sha256(data).hexdigest()

    def views(self, key, devices=None):
        return {device.name: zeta_seen(device, key, self.folders[device.name]) for device in devices or self.devices}

    def holds(self, key, text, file, devices=None):
        """True when every device reads this text and this file (a name of `files`, or None)."""
        expected = (text, self.digests[file] if file else None)
        return all(view == expected for view in self.views(key, devices).values())

    def settled(self, case, key, text, file):
        """Every device reaches the expected record, and still holds it after more turns."""
        try:
            wait(f"every device to hold {text!r} with file {file!r} ({case})", lambda: self.holds(key, text, file))
        except RuntimeError as error:
            raise RuntimeError(f"{error}; the devices read {self.views(key)!r}, files {self.digests!r}") from error
        time.sleep(SETTLE_SECONDS)
        require(self.holds(key, text, file),
                f"{case}: the devices changed after they agreed and now read {self.views(key)!r}, files {self.digests!r}")

    def base(self, key, file=None):
        """A record that every device holds before the case begins."""
        a = self.devices[0]
        a.write([zeta_create(key, "base", file=self.files[file] if file else None)])
        wait(f"every device to hold the base of {key}", lambda: self.holds(key, "base", file))


def file_under_a_later_edit(merge, a, b):
    """a. The older of two files does not come back when the text is edited after both."""
    for key, uploads_first, other in (("under-edit-a-first", a, b), ("under-edit-b-first", b, a)):
        merge.base(key)
        merge.relay.stop()
        a.change([zeta_replace(key, "base", file=merge.files["one"])])
        time.sleep(1.5)
        b.change([zeta_replace(key, "base", file=merge.files["two"])])
        time.sleep(1.5)
        a.change([zeta_replace(key, "text-of-A")])
        sync_in_order(merge.relay, uploads_first, other)
        merge.settled(f"a, {uploads_first.name} syncs first", key, "text-of-A", "two")


def file_while_an_edit_waits(merge, a, b, c):
    """b. A file of another device arrives while this device holds a later edit it has not sent."""
    key = "file-while-edit-waits"
    merge.base(key)
    merge.relay.stop()
    a.change([zeta_replace(key, "base", file=merge.files["one"])])
    time.sleep(1.5)
    b.change([zeta_replace(key, "base", file=merge.files["two"])])
    time.sleep(1.5)
    c.change([zeta_replace(key, "text-of-C")])
    a.stop()
    b.stop()
    merge.relay.start()
    wait("C to upload its edit", c.synced_up)
    a.start()
    wait("A to upload its file and hold the edit of C",
         lambda: a.synced_up() and merge.holds(key, "text-of-C", "one", (a,)))
    merge.relay.stop()
    time.sleep(1.5)
    a.change([zeta_replace(key, "last-text-of-A")])
    a.stop()
    merge.relay.start()
    b.start()
    wait("B to upload its file", b.synced_up)
    a.start()
    wait("A to upload its last edit", a.synced_up)
    merge.settled("b", key, "last-text-of-A", "two")


def two_creates(merge, a, b):
    """c. The later of two creates gives the text, and the file of the earlier one stays."""
    for key, uploads_first, other in (("two-creates-a-first", a, b), ("two-creates-b-first", b, a)):
        merge.relay.stop()
        a.write([zeta_create(key, "create-of-A", file=merge.files["one"])])
        time.sleep(1.5)
        b.write([zeta_create(key, "create-of-B")])
        sync_in_order(merge.relay, uploads_first, other)
        merge.settled(f"c, {uploads_first.name} syncs first", key, "create-of-B", "one")


def delete_under_a_later_edit(merge, a, b):
    """d. A later edit brings a deleted record back with its text and without the file."""
    for key, uploads_first, other in (("delete-a-first", a, b), ("delete-b-first", b, a)):
        merge.base(key, file="one")
        merge.relay.stop()
        a.change([zeta_delete(key)])
        time.sleep(1.5)
        b.change([zeta_replace(key, "text-of-B")])
        sync_in_order(merge.relay, uploads_first, other)
        merge.settled(f"d, {uploads_first.name} syncs first", key, "text-of-B", None)


def clear_under_a_later_edit(merge, a, b):
    """e. Clearing the file (`"file": null`) beats an earlier file write and stays cleared under a later edit."""
    for key, uploads_first, other in (("clear-a-first", a, b), ("clear-b-first", b, a)):
        merge.base(key, file="one")
        merge.relay.stop()
        a.change([zeta_replace(key, "base", file=merge.files["two"])])
        time.sleep(1.5)
        b.change([zeta_replace(key, "cleared-by-B", file=None)])
        time.sleep(1.5)
        a.change([zeta_replace(key, "text-of-A")])
        sync_in_order(merge.relay, uploads_first, other)
        merge.settled(f"e, {uploads_first.name} syncs first", key, "text-of-A", None)


def schema_conflicts_do_not_stall(merge, a, b, c):
    """f. Incompatible and additive schema conflicts with attachments advance every stream and apply later edits."""
    for idx, (uploads_first, other) in enumerate(((a, b), (b, a))):
        theta = f"{THETA}{idx}"
        iota = f"{IOTA}{idx}"
        kappa = f"{KAPPA}{idx}"
        after_a = f"after-conflict-a-{idx}"
        after_b = f"after-conflict-b-{idx}"
        merge.relay.stop()
        # Earlier offline device defines incompatible schema `theta` (field `legacy`) + attachment `lose`,
        # v1 schema `iota` (fields `id`, `note`), and v1 schema `kappa` with a pre-conflict record.
        system_seed(a.node, definition(theta, "Hash", ["id", "legacy"], description_prefix="Personal proof "))
        a.write([v1.create_item(theta, {"hash": "t-lose"}, {"id": "t-lose", "legacy": "old-shape"},
                                file=merge.files["lose"])])
        system_seed(a.node, definition(iota, "Hash", FIELDS, description_prefix="Personal proof "))
        a.write([v1.create_item(iota, {"hash": "i-v1"}, {"id": "i-v1", "note": "from-a"})])
        system_seed(a.node, definition(kappa, "Hash", FIELDS, description_prefix="Personal proof "))
        a.write([v1.create_item(kappa, {"hash": "k-early"}, {"id": "k-early", "note": "pre-conflict"})])
        time.sleep(1.5)
        # Later offline device defines incompatible winning schema `theta` (fields `id`, `note`) + attachment `two`,
        # compatible additive v2 schema `iota`, and an incompatible intermediate schema `kappa`.
        system_seed(b.node, definition(theta, "Hash", FIELDS, description_prefix="Personal proof "))
        b.write([v1.create_item(theta, {"hash": "t-win"}, {"id": "t-win", "note": "new-shape"},
                                file=merge.files["two"])])
        system_seed(b.node, definition(iota, "Hash", ["id", "note", "extra"], description_prefix="Personal proof "))
        b.write([v1.create_item(iota, {"hash": "i-v2"}, {"id": "i-v2", "note": "from-b", "extra": "kept"})])
        system_seed(b.node, definition(kappa, "Hash", ["id", "note", "alt_only"], description_prefix="Personal proof "))
        time.sleep(1.5)
        # Device A upgrades `kappa` additively after B's incompatible schema, writes `k-late`, and writes
        # offline against its losing `theta` plus unrelated ZETA edits.
        system_seed(a.node, definition(kappa, "Hash", ["id", "note", "extra"], description_prefix="Personal proof "))
        a.write([v1.create_item(kappa, {"hash": "k-late"}, {"id": "k-late", "note": "kept", "extra": "v2"})])
        a.write([v1.create_item(theta, {"hash": "t-late-lose"}, {"id": "t-late-lose", "legacy": "stale"})])
        a.write([zeta_create(after_a, "unrelated-after-a", file=merge.files["one"])])
        b.write([zeta_create(after_b, "unrelated-after-b", file=merge.files["two"])])
        sync_in_order(merge.relay, uploads_first, other)
        merge.settled(f"f-after-a-{idx}", after_a, "unrelated-after-a", "one")
        merge.settled(f"f-after-b-{idx}", after_b, "unrelated-after-b", "two")
        expected_theta = {"t-win": {"id": "t-win", "note": "new-shape"}}
        expected_iota = {
            "i-v1": {"id": "i-v1", "note": "from-a"},
            "i-v2": {"id": "i-v2", "note": "from-b", "extra": "kept"},
        }
        expected_kappa = {"k-late": {"id": "k-late", "note": "kept", "extra": "v2"}}
        wait(f"every device to converge on {theta}, {iota}, and {kappa}",
             lambda th=theta, io=iota, ka=kappa, et=expected_theta, ei=expected_iota, ek=expected_kappa: all(
                 dev.has_schema(th) and dev.records(th) == et
                 and dev.has_schema(io) and dev.records(io) == ei
                 and dev.has_schema(ka) and dev.records(ka) == ek
                 and dev.sync_status().get("databases", {}).get(dev.database, {}).get("download", {}).get("last_error") is None
                 for dev in (a, b, c)))


def prove_merge(relay, a, b, c, out):
    """22. Fields and files written on several devices at once end the same on every device."""
    merge = Merge(relay, (a, b, c), out)
    system_seed(a.node, definition(ZETA, "Hash", FIELDS, description_prefix="Personal proof "))
    file_under_a_later_edit(merge, a, b)
    file_while_an_edit_waits(merge, a, b, c)
    two_creates(merge, a, b)
    delete_under_a_later_edit(merge, a, b)
    clear_under_a_later_edit(merge, a, b)
    schema_conflicts_do_not_stall(merge, a, b, c)
    wait("A, B and C to upload", lambda: a.synced_up() and b.synced_up() and c.synced_up())
    print("22. A record and its file written on two and three devices at once ended the same on every "
          "device: the newest text, the file of the newest attachment write, and schema conflicts converged",
          flush=True)
    return {"cases": ["file under a later edit", "file while an edit waits", "two creates",
                      "delete under a later edit", "clear under a later edit",
                      "schema conflicts do not stall"]}
