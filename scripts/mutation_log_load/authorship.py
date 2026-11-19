"""Prove created_by on the private DEV devices, with cloud sync on.

The record-home CI home points the cloud at a dead endpoint, so capture stays
off and a device never settles its own replay. That settle is the path that
dropped created_by. This section runs on the two devices the private recovery
operation already starts.

Every device uses the one DEV identity. created_by is owner.public.hex(), the
public key of the seed written to identity.key. updated_by is that same key,
so a kept original and a value set to the writer cannot be told apart. The
check requires the key to be present after every live edit.
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_SCRIPTS = _HERE.parent
sys.path[:0] = [str(_HERE), str(_SCRIPTS), str(_SCRIPTS / "cloud_backup")]

from finish import decimal  # noqa: E402
from support import OperationError, read_json, retry, running, summary  # noqa: E402
import v1_records as v1  # noqa: E402

SCHEMA = "Authorship"
ALTERNATE = "by_status"
RECORD_ID = "authorship-1"
OPEN = "open"
ARCHIVED = "archived"
# A schema put is one delivery entry. Each record write is one entry.
SCHEMA_PUT = 1
RECORD_WRITE = 1


def identity_key(owner):
    """The node key the operation already holds. Not a share-contact call."""
    key = owner.public.hex()
    if len(key) != 64 or any(char not in "0123456789abcdef" for char in key):
        raise OperationError("authorship_identity_key")
    return key


def record_fields(note, status):
    return {"id": RECORD_ID, "note": note, "status": status}


def seed(node, database):
    names = ("id", "note", "status")
    body = {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {ALTERNATE: {"kind": "hash"}}},
        "fields": list(names), "field_types": dict.fromkeys(names, "String"),
        "field_descriptions": {name: f"{SCHEMA} {name}" for name in names},
    }}
    write_step("seed", lambda: node.call(f"/api/databases/{database}/schemas/seed-system", body))


def _shown(value):
    if value is None or value == "":
        return "absent"
    if isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value):
        return value
    return "invalid"


def _fail(step, message):
    raise OperationError(f"authorship step={step} {message}")


def read_page(node, database, step, *, alternate=None):
    if alternate is None:
        item = v1.point(SCHEMA, {"hash": RECORD_ID}, database=database)
    else:
        item = v1.point(SCHEMA, {"hash": alternate}, via=ALTERNATE, database=database)
    try:
        return v1.read_items(node, [item])[0]
    except (OperationError, RuntimeError) as error:
        _fail(step, f"read failed: {error}")


def live_row(node, database, step):
    page = read_page(node, database, step)
    rows = page.get("records") if isinstance(page, dict) else None
    if not isinstance(rows, list) or len(rows) != 1 or page.get("next_cursor") is not None:
        count = len(rows) if isinstance(rows, list) else "unreadable"
        _fail(step, f"primary read returned {count} records")
    row = rows[0]
    key = row.get("key") if isinstance(row, dict) else None
    if not isinstance(key, dict) or key.get("hash") != RECORD_ID or key.get("range") is not None:
        _fail(step, "primary key differs")
    return row


def expect_authors(node, database, step, created_by, updated_by, fields):
    """created_by is the value saved at create. updated_by is the writer of this step."""
    row = live_row(node, database, step)
    found_created = row.get("created_by")
    found_updated = row.get("updated_by")
    if found_created != created_by or found_updated != updated_by:
        _fail(step, "created_by expected={0} found={1} updated_by expected={2} found={3}".format(
            created_by, _shown(found_created), updated_by, _shown(found_updated)))
    found = row.get("record")
    if found != fields:
        _fail(step, f"record fields expected={fields} found={found}")
    revision = row.get("revision")
    if not isinstance(revision, str) or not revision:
        _fail(step, "revision missing")
    return revision


def expect_absent(node, database, step):
    page = read_page(node, database, step)
    rows = page.get("records") if isinstance(page, dict) else None
    if not isinstance(rows, list) or rows or page.get("next_cursor") is not None:
        found = rows[0].get("created_by") if isinstance(rows, list) and rows and isinstance(rows[0], dict) else None
        _fail(step, f"record still present created_by found={_shown(found)}")


def expect_alternate(node, database, step, status, present):
    page = read_page(node, database, step, alternate=status)
    rows = page.get("records") if isinstance(page, dict) else None
    count = len(rows) if isinstance(rows, list) else -1
    wanted = 1 if present else 0
    if count != wanted or page.get("next_cursor") is not None:
        _fail(step, f"alternate {status} expected={wanted} records found={count}")


def write_step(step, action):
    try:
        action()
    except OperationError as error:
        raise OperationError(
            f"authorship step={step} write failed: {error}",
            socket_refusal=error.socket_refusal, cli_refusal=error.cli_refusal,
        ) from None
    except RuntimeError as error:
        _fail(step, f"write failed: {error}")


def _create(node, database, fields):
    v1.write_items(node, [v1.create_item(
        SCHEMA, {"hash": RECORD_ID}, fields, alternates={ALTERNATE: {"hash": fields["status"]}}, database=database)])


def _replace(node, database, fields, revision):
    v1.write_items(node, [v1.replace_item(
        SCHEMA, {"hash": RECORD_ID}, fields, revision,
        alternates={ALTERNATE: {"hash": fields["status"]}}, database=database)])


def _delete(node, database, revision):
    v1.write_items(node, [v1.delete_item(SCHEMA, {"hash": RECORD_ID}, revision, database=database)])


def cloud_head(call):
    value = call("head", {})["cursor"]
    return 0 if value is None else decimal(value)


def wait_through(call, node, database, expected, stage):
    """The operation's sync wait: cloud head, then this device's applied prefix."""
    def head():
        return cloud_head(call)

    try:
        retry(stage, head, lambda found: found == expected)
    except OperationError:
        _fail(stage, f"cloud head expected={expected} found={head()}")
    path = node.home / "log-sync" / database / "prefix.json"

    def mark():
        return read_json(path, 4096) if path.is_file() else {}

    try:
        retry(stage, mark, lambda item: item.get("database_id") == database and item.get("through") == expected)
    except OperationError:
        found = mark().get("through") if path.is_file() else "absent"
        _fail(stage, f"device prefix expected={expected} found={found}")


def create_and_move(call, source, database, key, through):
    """Create, edit the body, and move the alternate address. Settle that move."""
    seed(source, database)
    created = record_fields("created", OPEN)
    write_step("create", lambda: _create(source, database, created))
    revision = expect_authors(source, database, "create", key, key, created)
    edited = record_fields("edited", OPEN)
    write_step("edit-body", lambda: _replace(source, database, edited, revision))
    revision = expect_authors(source, database, "edit-body", key, key, edited)
    # One replace of the same primary key. The alternate hash changes, as a status change does.
    moved = record_fields("edited", ARCHIVED)
    write_step("move-alternate", lambda: _replace(source, database, moved, revision))
    expect_authors(source, database, "move-alternate", key, key, moved)
    expect_alternate(source, database, "move-alternate", ARCHIVED, True)
    expect_alternate(source, database, "move-alternate", OPEN, False)
    settled = through + SCHEMA_PUT + 3 * RECORD_WRITE
    wait_through(call, source, database, settled, "move-alternate after sync")
    expect_authors(source, database, "move-alternate after sync", key, key, moved)
    expect_alternate(source, database, "move-alternate after sync", ARCHIVED, True)
    expect_alternate(source, database, "move-alternate after sync", OPEN, False)
    return settled


def finish_edits(call, source, database, key, through):
    """Move the alternate address back, edit the body again, and settle both."""
    moved = record_fields("edited", ARCHIVED)
    revision = expect_authors(source, database, "move-alternate after sync", key, key, moved)
    restored = record_fields("edited", OPEN)
    write_step("move-alternate-back", lambda: _replace(source, database, restored, revision))
    revision = expect_authors(source, database, "move-alternate-back", key, key, restored)
    expect_alternate(source, database, "move-alternate-back", OPEN, True)
    expect_alternate(source, database, "move-alternate-back", ARCHIVED, False)
    again = record_fields("edited-again", OPEN)
    write_step("edit-body-again", lambda: _replace(source, database, again, revision))
    expect_authors(source, database, "edit-body-again", key, key, again)
    settled = through + 2 * RECORD_WRITE
    wait_through(call, source, database, settled, "edit-body-again on source after sync")
    expect_authors(source, database, "edit-body-again on source after sync", key, key, again)
    return again, settled


def recreate(call, source, peer, database, key, through):
    """Delete, settle, and create again. The new create's created_by is the writer."""
    again = record_fields("edited-again", OPEN)
    revision = expect_authors(source, database, "edit-body-again on source after sync", key, key, again)
    write_step("delete", lambda: _delete(source, database, revision))
    expect_absent(source, database, "delete")
    deleted = through + RECORD_WRITE
    wait_through(call, source, database, deleted, "delete after sync")
    expect_absent(source, database, "delete after sync")
    recreated = record_fields("recreated", OPEN)
    write_step("recreate", lambda: _create(source, database, recreated))
    expect_authors(source, database, "recreate", key, key, recreated)
    final = deleted + RECORD_WRITE
    wait_through(call, source, database, final, "recreate on source after sync")
    wait_through(call, peer, database, final, "recreate on peer")
    expect_authors(source, database, "recreate on source after sync", key, key, recreated)
    expect_authors(peer, database, "recreate on peer", key, key, recreated)
    return final


def prove(programs, call, database, api_key, source, peer, owner, through):
    """Run the edits on device A, then read them on device B after the sync wait."""
    key = identity_key(owner)
    print("authorship: one identity. created_by is owner.public.hex() of the key already "
          "written to identity.key. Every device has that key, so a kept original and a value "
          "set to the writer are the same hex. The check requires the key to be present.",
          flush=True)
    found = cloud_head(call)
    if found != through:
        _fail("start", f"cloud head expected={through} found={found}")
    programs.connect_existing(source, api_key, "authorship_source_connection")
    with running(source, remove_saved=True):
        through = create_and_move(call, source, database, key, through)
        fields, through = finish_edits(call, source, database, key, through)
        programs.connect_existing(peer, api_key, "authorship_peer_connection")
        with running(peer, remove_saved=True):
            # Peer was stopped for the whole edit, so it replays an already edited record.
            wait_through(call, peer, database, through, "edit-body-again on peer")
            expect_authors(peer, database, "edit-body-again on peer", key, key, fields)
            through = recreate(call, source, peer, database, key, through)
    summary("Authorship: created_by stayed present through a body edit, an alternate-address "
            "move, a move back, another body edit, and a delete plus create. The writing device "
            "read it after each step and again after its own replay settled. The second device "
            "first saw the record already edited, then saw the new create. Both devices share one "
            "identity key, so this run cannot tell a kept original from a value set to the writer.")
    return through


def expect_recreated(node, database, owner):
    """The fresh restore replays the log and must still show the new create's author."""
    key = identity_key(owner)
    expect_authors(node, database, "recreate on restored", key, key, record_fields("recreated", OPEN))
