#!/usr/bin/env python3
"""One-off: make a home written by build d06054159 start on the current build.

This is a script for one home, run by hand once, with the daemon stopped. It is not part of
the product: the product holds no code that reads an old layout. What the product does with the
home afterwards (it keeps the files as the cut of a first snapshot, publishes it, and only then
sends later writes) is described in `docs/designs/move-an-old-home.md`.

The current build refuses a home written by that build for three reasons, and the script
removes each reason with the smallest plain-file change:

1. The owner row of `databases/registry.json` holds a random UUID. The current build derives
   the id of the personal database from the identity key and refuses any other id. The script
   writes the derived id (printed by `lastdb admin identity`). The store files do not embed the
   owner id, so nothing else changes: the records stay as they are.
2. `log-sync/device-id` holds a UUID. A device id is 64 hex characters. The script writes a
   fresh random one.
3. The old upload path left `log-sync/<owner UUID>/` (one file for each applied change, tens of
   thousands) and the old spool `data/disk-run/accepted-v1/` (one file for each accepted write).
   They belong to the old cloud log and mean nothing to the signed log. The script moves them,
   whole and unread, into a dated directory beside the home. The owner removes that later.

`cloud-credentials.json` is not changed. The current build syncs with `LASTDB_RELAY_URL`, or
else with the `api_url` of the saved credentials. The script says which of the two will apply
and whether the saved file still parses.

The script refuses to run when the daemon is running, when the home is not the old layout, or when
`data/local-file-refs.json` names a file larger than a snapshot can hold (a snapshot seals each
referenced file as one value of at most 32 MiB, so the first snapshot of such a home would fail).
Every step is safe to run again, and the registry is changed last, so a run that stops half way
leaves a home that the current build still refuses, never half of one that starts.

`--check` only reports what it would do.

Exit status: 0 done, or nothing to do; 2 refused; 1 an unexpected failure.
"""
import argparse
import datetime
import errno
import fcntl
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys

MARKER = b"format=15\nhome=disk-run\ncas=disk-cas\n"
HEX64 = re.compile(r"^[0-9a-f]{64}$")
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
FIRST_BACKUP_INTENT = ".native-first-backup"
# A snapshot seals each referenced file as one value, and a run holds values up to 32 MiB.
LARGEST_REFERENCE = 32 * 1024 * 1024 - 4096
CREDENTIAL_KEYS = {"api_url", "api_key", "connection"}
CONNECTION_KEYS = {"account_id", "source_home", "environment", "endpoint"}


class Refused(Exception):
    """The home is not one this script may change. Nothing was changed."""


def say(check, text):
    print(("would: " if check else "done:  ") + text, flush=True)


def lastdbd_commands():
    """The command lines of the running `lastdbd` processes."""
    listing = subprocess.run(["ps", "-axo", "args="], capture_output=True, text=True, check=False).stdout
    return [line for line in listing.splitlines() if re.search(r"(^|/)lastdbd( |$)", line.strip())]


def daemon_running(home):
    """True when a daemon holds the store lock, answers on a socket of the home, or names the home."""
    lock = home / "data" / "disk-run" / "LOCK"
    if lock.exists():
        with lock.open("rb") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                if error.errno in (errno.EAGAIN, errno.EACCES):
                    return True
                raise
    for path in (home / "data").glob("*.sock"):
        with socket.socket(socket.AF_UNIX) as probe:
            probe.settimeout(2)
            try:
                probe.connect(str(path))
                return True
            except OSError:
                continue
    for command in lastdbd_commands():
        # A daemon that names no home in its command line runs on the default home.
        if str(home) in command or ("--data-dir" not in command and home == Path.home() / ".lastdb"):
            return True
    return False


def derived_identity(home, lastdb):
    """The personal database id and the node key that identity.key derives, from the product."""
    result = subprocess.run([lastdb, "--data-dir", str(home), "admin", "identity", "--json"],
                            capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise Refused(f"lastdb admin identity failed: {result.stderr.strip()[-300:]}")
    found = json.loads(result.stdout)
    if not HEX64.match(found.get("database_id", "")):
        raise Refused(f"lastdb admin identity printed an unexpected id: {found!r}")
    return found["database_id"], found["node_key"]


def read_registry(home):
    path = home / "databases" / "registry.json"
    try:
        registry = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise Refused(f"{path} is missing or unreadable ({error}): this is not a home of the old build") from error
    owner = registry.get("owner") if isinstance(registry, dict) else None
    if (set(registry) - {"format", "owner", "owned"} or registry.get("format") != 1
            or not isinstance(owner, dict) or owner.get("kind") != "personal"
            or owner.get("name") != "personal"):
        raise Refused(f"{path} does not have the expected shape: {registry!r}")
    if registry.get("owned"):
        raise Refused(
            f"{path} lists owned databases with their own ids ({len(registry['owned'])}): the "
            "current build needs a 64-hex id for each, and this script maps only the personal "
            "database. Nothing was changed.")
    return path, registry


def require_layout(home):
    """The files an old home must have. Anything else is not a home this script knows."""
    identity = home / "identity.key"
    if not identity.is_file() or identity.stat().st_size != 32:
        raise Refused(f"{identity} is missing or is not 32 bytes")
    marker = home / "data" / "laststore-pack-node-v1"
    if not marker.is_file() or marker.read_bytes() != MARKER:
        raise Refused(f"{marker} is missing or is not format 15: the current build cannot open this home")
    if not (home / "data" / "disk-run" / "CURRENT").is_file():
        raise Refused(f"{home}/data/disk-run/CURRENT is missing")


def require_small_references(home):
    """Files the home holds by path are sealed into every snapshot, one value each. A value larger
    than a run holds cannot be sealed, and the first snapshot of the home would never be taken."""
    path = home / "data" / "local-file-refs.json"
    if not path.is_file():
        return
    try:
        files = json.loads(path.read_text())["files"]
        large = sorted((entry["size_bytes"], entry["path"]) for entry in files.values()
                       if entry["size_bytes"] > LARGEST_REFERENCE)
    except (ValueError, KeyError, TypeError) as error:
        raise Refused(f"{path} does not have the expected shape ({error!r})") from error
    if large:
        shown = ", ".join(f"{name} ({size} bytes)" for size, name in large[:5])
        raise Refused(f"{path} names {len(large)} file(s) larger than {LARGEST_REFERENCE} bytes, which no "
                      f"snapshot can hold: {shown}. Import them into the store or drop the reference, then "
                      "run this again. Nothing was changed.")


def plan(home, derived):
    """What is left to do, as (action, detail) pairs, in the order they are done."""
    _, registry = read_registry(home)
    owner = registry["owner"]["id"]
    if owner == derived:
        # The registry is changed last, so this home was adopted. The current build may have
        # run on it since, and then `accepted-v1` and `log-sync/<id>` are ITS files: leave them.
        return []
    if HEX64.match(owner):
        raise Refused(f"the owner row holds the 64-hex id {owner}, which the identity key does not "
                      f"derive ({derived}): this home belongs to another identity")
    steps = []
    log_sync = home / "log-sync"
    device = log_sync / "device-id"
    if device.is_file() and not HEX64.match(device.read_text().strip()):
        steps.append(("device-id", device))
    for child in sorted(log_sync.iterdir()) if log_sync.is_dir() else []:
        if child.is_dir() and UUID.match(child.name):
            steps.append(("set-aside", child))
    spool = home / "data" / "disk-run" / "accepted-v1"
    if spool.is_dir():
        steps.append(("set-aside", spool))
    if (home / FIRST_BACKUP_INTENT).exists():
        steps.append(("set-aside", home / FIRST_BACKUP_INTENT))
    steps.append(("registry", owner))
    return steps


def describe_old_spool(spool):
    """What the old spool held, for the record. The writes are in the store either way."""
    try:
        state = json.loads((spool / "state.json").read_text())
    except (OSError, ValueError):
        return "state unreadable"
    ids = sum(1 for _ in (spool / "ids").iterdir()) if (spool / "ids").is_dir() else 0
    entries = sum(1 for _ in (spool / "entries").iterdir()) if (spool / "entries").is_dir() else 0
    return (f"head {state.get('head')}, acknowledged {state.get('acked')}, {entries} entries not yet "
            f"retired, {ids} id records")


def write_private(path, data):
    """Write a file whole: a temporary file in the same directory, flushed, then renamed."""
    temporary = path.with_name(path.name + ".adopt-tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.rename(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def set_aside_root(home, given):
    if given is not None:
        return given
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d-%H%M%S")
    return home.parent / f"{home.name}.old-build-{stamp}"


def move_aside(source, root, home):
    """Move one path into the dated directory, keeping its position under the home in its name."""
    root.mkdir(mode=0o700, exist_ok=True)
    name = "__".join(source.relative_to(home).parts)
    target = root / name
    if target.exists():
        raise Refused(f"{target} already exists: the set-aside directory was used before; name another")
    os.rename(source, target)
    return target


def credentials_report(home):
    """Which relay the current build will use, and whether the saved credentials still parse."""
    explicit = os.environ.get("LASTDB_RELAY_URL", "").strip()
    saved = home / "cloud-credentials.json"
    note = "no cloud-credentials.json"
    api_url = None
    if saved.is_file():
        try:
            body = json.loads(saved.read_text())
            connection = body.get("connection")
            fits = (isinstance(body, dict) and set(body) <= CREDENTIAL_KEYS
                    and isinstance(body.get("api_url"), str) and isinstance(body.get("api_key"), str)
                    and (connection is None or (isinstance(connection, dict) and set(connection) == CONNECTION_KEYS)))
        except ValueError:
            fits = False
        if fits:
            api_url = body["api_url"].strip().rstrip("/") or None
            note = f"cloud-credentials.json parses; api_url is {api_url!r}"
        else:
            note = ("cloud-credentials.json does NOT parse as the current shape: the node ignores it and "
                    "has no relay unless LASTDB_RELAY_URL is set. Run `lastdb backup connect`, or set "
                    "LASTDB_RELAY_URL for the daemon")
    relay = explicit or api_url
    print(f"relay: the current build will use {relay!r} "
          f"({'LASTDB_RELAY_URL of this shell' if explicit else 'the saved api_url' if api_url else 'none'}); {note}.",
          flush=True)
    print("       The daemon needs the relay in ITS environment: LASTDB_RELAY_URL, or a parsing "
          "cloud-credentials.json. The api_key is not used by the signed log.", flush=True)


def adopt(home, lastdb, check, given_root):
    require_layout(home)
    require_small_references(home)
    if daemon_running(home):
        raise Refused("a daemon is running on this home. Stop it, then run this again.")
    database_id, node_key = derived_identity(home, lastdb)
    steps = plan(home, database_id)
    print(f"home {home}\nderived personal database id {database_id}\nnode key {node_key}", flush=True)
    if not steps:
        print("nothing to do: this home already holds the derived id and none of the old files.", flush=True)
        credentials_report(home)
        return
    root = set_aside_root(home, given_root)
    spool = home / "data" / "disk-run" / "accepted-v1"
    if spool.is_dir():
        print(f"old spool: {describe_old_spool(spool)} (its writes are in the store, and the first "
              "snapshot covers the store)", flush=True)
    for action, detail in steps:
        if action == "device-id":
            say(check, f"replace {detail} (not 64 hex) with a fresh 64-hex device id")
            if not check:
                write_private(detail, os.urandom(32).hex().encode() + b"\n")
        elif action == "set-aside":
            say(check, f"move {detail} to {root}")
            if not check:
                print(f"       now at {move_aside(detail, root, home)}", flush=True)
    for action, detail in steps:
        if action == "registry":
            say(check, f"write the owner id {database_id} in databases/registry.json (was {detail})")
            if not check:
                path, registry = read_registry(home)
                registry["owner"]["id"] = database_id
                write_private(path, (json.dumps(registry, indent=2) + "\n").encode())
    credentials_report(home)
    if not check:
        print(f"Set-aside files are in {root}. Keep them until the new home is proven, then remove them.",
              flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--home", required=True, type=Path, help="the home directory (holds identity.key)")
    parser.add_argument("--lastdb", default="lastdb", help="the current `lastdb` CLI, to derive the ids")
    parser.add_argument("--set-aside", type=Path, default=None,
                        help="directory for the files that move (default: a dated directory beside the home)")
    parser.add_argument("--check", action="store_true", help="only report what would change")
    args = parser.parse_args()
    try:
        adopt(args.home.resolve(), args.lastdb, args.check, args.set_aside)
    except Refused as error:
        print(f"refused: {error}", file=sys.stderr)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
