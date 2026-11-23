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
# Long enough for a turn that would change a settled record to run.
SETTLE_SECONDS = 8


def zeta_create(key, text, file=None):
    return v1.create_item(ZETA, {"hash": key}, {"id": key, "note": text}, file=file)


def zeta_replace(key, text, file=None):
    """A replace that names no file keeps the file of the record."""
    item = {"schema": ZETA, "op": "replace", "key": {"hash": key}, "record": {"id": key, "note": text}}
    if file is not None:
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
        for index, name in enumerate(("one", "two")):
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


def prove_merge(relay, a, b, c, out):
    """22. Fields and files written on several devices at once end the same on every device."""
    merge = Merge(relay, (a, b, c), out)
    system_seed(a.node, definition(ZETA, "Hash", FIELDS, description_prefix="Personal proof "))
    file_under_a_later_edit(merge, a, b)
    file_while_an_edit_waits(merge, a, b, c)
    two_creates(merge, a, b)
    delete_under_a_later_edit(merge, a, b)
    wait("A, B and C to upload", lambda: a.synced_up() and b.synced_up() and c.synced_up())
    print("22. A record and its file written on two and three devices at once ended the same on every "
          "device: the newest text, and the file of the newest write that set a file or deleted the record",
          flush=True)
    return {"cases": ["file under a later edit", "file while an edit waits", "two creates", "delete under a later edit"]}
