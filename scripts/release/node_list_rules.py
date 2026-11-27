"""Rules of the signed node release list: canonical JSON, times, and section 3 of
docs/designs/node-self-update.md. Used by node_list.py."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re

TARGETS = ("aarch64-apple-darwin", "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu")
CHANNELS = ("stable", "nightly")
MAX_RELEASES = 20
HEX64 = re.compile(r"[0-9a-f]{64}\Z")
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\Z")
STABLE_VERSION = re.compile(r"\d+\.\d+\.\d+\Z")
NIGHTLY_VERSION = re.compile(r"\d+\.\d+\.\d+-nightly\.\d{8}\.g([0-9a-f]{9})\Z")
LIST_KEYS = {"list_version", "kind", "channel", "target", "generated_at", "seq", "releases"}
ENTRY_KEYS = {"version", "source_git_oid", "published_at", "archive_url", "archive_sha256",
              "archive_bytes", "binaries", "signing", "rollout", "status"}
WITHDRAWN_KEYS = {"withdrawn_reason", "withdrawn_at"}


class ListError(Exception):
    """A list, signature, release, or request breaks a rule of the contract."""


# ---------------------------------------------------------------- canonical JSON

def _canon(value):
    if value is None:
        return "null"
    if value is True:
        return "true"
    if value is False:
        return "false"
    if type(value) is int:
        return str(value)
    if type(value) is str:
        return json.dumps(value, ensure_ascii=False)
    if type(value) is list:
        return "[" + ",".join(_canon(item) for item in value) + "]"
    if type(value) is dict:
        # RFC 8785: members sorted by the UTF-16 code units of the name.
        keys = sorted(value, key=lambda name: name.encode("utf-16-be"))
        return "{" + ",".join(_canon(name) + ":" + _canon(value[name]) for name in keys) + "}"
    raise ListError(f"Value of type {type(value).__name__} has no canonical form here")


def canonical(value):
    """RFC 8785 (JCS) bytes, equal to app_identity_crypto::canonicalize for these values."""
    return _canon(value).encode("utf-8")


# ---------------------------------------------------------------- time

def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0)


def fmt(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(text, what="time"):
    if type(text) is not str or not TIME.fullmatch(text):
        raise ListError(f"{what} is not a UTC time of the form YYYY-MM-DDTHH:MM:SSZ")
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise ListError(f"{what} is not a real time") from error


# ---------------------------------------------------------------- list rules

def is_int(value):
    return type(value) is int


def check_entry(entry, channel, target):
    if type(entry) is not dict:
        raise ListError("A release entry is not an object")
    keys = set(entry)
    withdrawn = entry.get("status") == "withdrawn"
    allowed = ENTRY_KEYS | (WITHDRAWN_KEYS if withdrawn else set())
    if keys != allowed:
        raise ListError(f"Release entry fields differ from the contract: {sorted(keys ^ allowed)}")
    version = entry["version"]
    oid = entry["source_git_oid"]
    if type(oid) is not str or not HEX40.fullmatch(oid):
        raise ListError("source_git_oid is not 40 lowercase hex characters")
    if type(version) is not str:
        raise ListError("version is not a string")
    if channel == "stable":
        if not STABLE_VERSION.fullmatch(version):
            raise ListError(f"Stable version {version!r} is invalid")
    else:
        match = NIGHTLY_VERSION.fullmatch(version)
        if match is None:
            raise ListError(f"Nightly version {version!r} is invalid")
        if match[1] != oid[:9]:
            raise ListError("The nightly version and source_git_oid differ")
    parse_time(entry["published_at"], "published_at")
    url = entry["archive_url"]
    if (type(url) is not str
            or not re.fullmatch(r"https://github\.com/EdgeVector/(lastdb|homebrew-lastdb)/releases/download/"
                                r"[A-Za-z0-9._-]+/" + re.escape(f"lastdb-{target}.tar.gz"), url)):
        raise ListError("archive_url is outside the fixed release locations")
    if type(entry["archive_sha256"]) is not str or not HEX64.fullmatch(entry["archive_sha256"]):
        raise ListError("archive_sha256 is not 64 lowercase hex characters")
    if not is_int(entry["archive_bytes"]) or entry["archive_bytes"] <= 0:
        raise ListError("archive_bytes is not a positive integer")
    binaries = entry["binaries"]
    if type(binaries) is not list or len(binaries) != 2:
        raise ListError("binaries must name exactly lastdb and lastdbd")
    for item in binaries:
        if type(item) is not dict or set(item) != {"name", "sha256", "size"}:
            raise ListError("A binary entry has fields other than name, sha256, and size")
        if (type(item["sha256"]) is not str or not HEX64.fullmatch(item["sha256"])
                or not is_int(item["size"]) or item["size"] <= 0):
            raise ListError("A binary entry has an invalid hash or size")
    if sorted(item["name"] for item in binaries) != ["lastdb", "lastdbd"]:
        raise ListError("binaries must name exactly lastdb and lastdbd")
    expected = "developer-id" if target.endswith("-apple-darwin") else "none"
    if entry["signing"] != expected:
        raise ListError(f"signing must be {expected!r} for {target}")
    rollout = entry["rollout"]
    if type(rollout) is not dict or set(rollout) != {"percent", "started_at"}:
        raise ListError("rollout has fields other than percent and started_at")
    if not is_int(rollout["percent"]) or not 0 <= rollout["percent"] <= 100:
        raise ListError("rollout.percent is not an integer from 0 to 100")
    parse_time(rollout["started_at"], "rollout.started_at")
    if entry["status"] not in ("active", "withdrawn"):
        raise ListError("status is not active or withdrawn")
    if withdrawn:
        reason = entry["withdrawn_reason"]
        if (type(reason) is not str or not reason.strip() or len(reason) > 200
                or any(ord(c) < 0x20 or ord(c) == 0x7F for c in reason)):
            raise ListError("withdrawn_reason must be one line of 1 to 200 characters")
        parse_time(entry["withdrawn_at"], "withdrawn_at")


def check_list(doc, channel, target):
    if type(doc) is not dict or set(doc) != LIST_KEYS:
        raise ListError("The list has fields other than those of the contract")
    if doc["list_version"] != 1 or type(doc["list_version"]) is not int:
        raise ListError("list_version is not 1")
    if doc["kind"] != "lastdb-node":
        raise ListError("kind is not lastdb-node")
    if doc["channel"] != channel or doc["target"] != target:
        raise ListError("The list channel or target differs from its path")
    parse_time(doc["generated_at"], "generated_at")
    if not is_int(doc["seq"]) or doc["seq"] <= 0:
        raise ListError("seq is not a positive integer")
    releases = doc["releases"]
    if type(releases) is not list or not 1 <= len(releases) <= MAX_RELEASES:
        raise ListError(f"releases must have 1 to {MAX_RELEASES} entries")
    seen = set()
    previous = None
    for entry in releases:
        check_entry(entry, channel, target)
        if entry["version"] in seen:
            raise ListError(f"Duplicate version {entry['version']}")
        seen.add(entry["version"])
        stamp = parse_time(entry["published_at"])
        if previous is not None and stamp > previous:
            raise ListError("releases are not ordered by published_at, newest first")
        previous = stamp
