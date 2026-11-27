#!/usr/bin/env python3
"""Write and verify the signed LastDB node release lists.

The contract is docs/designs/node-self-update.md, sections 2 to 7. One list covers one
channel and one target. This script reads and verifies the current list, applies one
change, writes canonical JSON with seq + 1, signs the SHA-256 of the bytes with the
environment's KMS key, uploads the .json and then the .sig, and downloads both again to
verify them with the pinned key before it exits 0.

Subcommands: publish, advance, withdraw, verify.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

PROFILE = Path(__file__).resolve().parents[2] / "lastdb_node" / "environments.json"
TARGETS = ("aarch64-apple-darwin", "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu")
CHANNELS = ("stable", "nightly")
ENVS = ("prod", "dev")
REPOS = {"nightly": "EdgeVector/lastdb", "stable": "EdgeVector/homebrew-lastdb"}
MAX_OBJECT = 256 * 1024
MAX_MANIFEST = 1024 ** 2
MAX_RELEASES = 20
ROLLOUT_START = {"nightly": 100, "stable": 10}
SPKI_ED25519_PREFIX = bytes.fromhex("302a300506032b6570032100")

HEX64 = re.compile(r"[0-9a-f]{64}\Z")
HEX40 = re.compile(r"[0-9a-f]{40}\Z")
TIME = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ\Z")
STABLE_VERSION = re.compile(r"\d+\.\d+\.\d+\Z")
NIGHTLY_VERSION = re.compile(r"\d+\.\d+\.\d+-nightly\.\d{8}\.g([0-9a-f]{9})\Z")
NIGHTLY_TAG = re.compile(r"nightly-\d{8}-[0-9a-f]{9}\Z")
STABLE_TAG = re.compile(r"v\d+\.\d+\.\d+\Z")
LIST_KEYS = {"list_version", "kind", "channel", "target", "generated_at", "seq", "releases"}
ENTRY_KEYS = {"version", "source_git_oid", "published_at", "archive_url", "archive_sha256",
              "archive_bytes", "binaries", "signing", "rollout", "status"}
WITHDRAWN_KEYS = {"withdrawn_reason", "withdrawn_at"}
ENVELOPE_KEYS = {"alg", "key_id", "payload_sha256", "sig"}


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


# ---------------------------------------------------------------- list rules (section 3)

def _int(value):
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
    if not _int(entry["archive_bytes"]) or entry["archive_bytes"] <= 0:
        raise ListError("archive_bytes is not a positive integer")
    binaries = entry["binaries"]
    if type(binaries) is not list or len(binaries) != 2:
        raise ListError("binaries must name exactly lastdb and lastdbd")
    for item in binaries:
        if type(item) is not dict or set(item) != {"name", "sha256", "size"}:
            raise ListError("A binary entry has fields other than name, sha256, and size")
        if (type(item["sha256"]) is not str or not HEX64.fullmatch(item["sha256"])
                or not _int(item["size"]) or item["size"] <= 0):
            raise ListError("A binary entry has an invalid hash or size")
    if sorted(item["name"] for item in binaries) != ["lastdb", "lastdbd"]:
        raise ListError("binaries must name exactly lastdb and lastdbd")
    expected = "developer-id" if target.endswith("-apple-darwin") else "none"
    if entry["signing"] != expected:
        raise ListError(f"signing must be {expected!r} for {target}")
    rollout = entry["rollout"]
    if type(rollout) is not dict or set(rollout) != {"percent", "started_at"}:
        raise ListError("rollout has fields other than percent and started_at")
    if not _int(rollout["percent"]) or not 0 <= rollout["percent"] <= 100:
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
    if not _int(doc["seq"]) or doc["seq"] <= 0:
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


# ---------------------------------------------------------------- profile and URLs

def profile(env):
    return json.loads(PROFILE.read_text())["environments"][env]


def node_index(env):
    data = profile(env)
    if data.get("node_index"):
        return data["node_index"].rstrip("/")
    base = data["registry_index"].rstrip("/")
    if not base.endswith("/v1/lists"):
        raise ListError("registry_index does not end in /v1/lists, so node_index cannot be derived")
    return base[: -len("/v1/lists")] + "/v1/node"


def pinned_keys(env, extra=()):
    texts = list(extra) or profile(env).get("node_release_keys") or []
    if not 1 <= len(texts) <= 2:
        raise ListError(f"environments.json has no node_release_keys for {env} (need one or two)")
    keys = []
    for text in texts:
        try:
            raw = base64.b64decode(text, validate=True)
        except ValueError as error:
            raise ListError("A pinned key is not base64") from error
        if len(raw) != 32:
            raise ListError("A pinned key is not 32 bytes")
        keys.append(raw)
    return keys


def object_url(index, channel, target, suffix):
    return f"{index}/{channel}/{target}.json{suffix}"


def s3_location(index):
    parts = urllib.parse.urlsplit(index)
    match = re.fullmatch(r"([a-z0-9][a-z0-9.-]+[a-z0-9])\.s3\.([a-z0-9-]+)\.amazonaws\.com", parts.netloc)
    if parts.scheme != "https" or match is None:
        raise ListError("node_index is not an S3 virtual-host HTTPS URL")
    return match[1], match[2], parts.path.strip("/")


# ---------------------------------------------------------------- reading

class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fetch(url):
    """HTTPS only, no redirect, at most 256 KiB. A missing object (403 or 404) gives None."""
    if not url.startswith("https://"):
        raise ListError("A list is read over HTTPS only")
    opener = urllib.request.build_opener(_NoRedirect)
    try:
        with opener.open(urllib.request.Request(url, headers={"User-Agent": "lastdb-node-list"}), timeout=30) as reply:
            raw = reply.read(MAX_OBJECT + 1)
    except urllib.error.HTTPError as error:
        if error.code in (403, 404):
            return None
        raise ListError(f"Reading {url} answered HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise ListError(f"Reading {url} failed: {error.reason}") from error
    if len(raw) > MAX_OBJECT:
        raise ListError("A list object exceeds 256 KiB")
    return raw


def verify_pair(raw, sig_raw, keys, channel, target):
    """Verify the exact bytes before they are parsed, then apply every rule. Returns the list."""
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    try:
        envelope = json.loads(sig_raw)
    except ValueError as error:
        raise ListError("The signature object is not JSON") from error
    if type(envelope) is not dict or set(envelope) != ENVELOPE_KEYS or envelope["alg"] != "ed25519":
        raise ListError("The signature envelope differs from the contract")
    digest = hashlib.sha256(raw).hexdigest()
    if envelope["payload_sha256"] != digest:
        raise ListError("The signature covers other bytes than the list")
    matching = [key for key in keys if hashlib.sha256(key).hexdigest() == envelope["key_id"]]
    if not matching:
        raise ListError("The list is signed by a key that is not pinned")
    try:
        signature = base64.b64decode(envelope["sig"], validate=True)
        if len(signature) != 64:
            raise ValueError
        Ed25519PublicKey.from_public_bytes(matching[0]).verify(signature, digest.encode("ascii"))
    except (ValueError, InvalidSignature, TypeError) as error:
        raise ListError("The list signature does not verify") from error
    try:
        doc = json.loads(raw)
    except ValueError as error:
        raise ListError("The list is not JSON") from error
    check_list(doc, channel, target)
    if canonical(doc) != raw:
        raise ListError("The list bytes are not the canonical form")
    return doc


def read_pair(index, channel, target):
    raw = fetch(object_url(index, channel, target, ""))
    sig = fetch(object_url(index, channel, target, ".sig"))
    return raw, sig


def read_verified(index, channel, target, keys):
    """The verified list, or None when both objects are missing. One retry after a mismatch."""
    for attempt in (1, 2):
        raw, sig = read_pair(index, channel, target)
        if raw is None and sig is None:
            return None
        if raw is None or sig is None:
            if attempt == 1:
                time.sleep(2)
                continue
            raise ListError(f"{channel}/{target}: only one of .json and .sig exists")
        try:
            return verify_pair(raw, sig, keys, channel, target), raw
        except ListError:
            if attempt == 2:
                raise
            time.sleep(2)
    raise ListError("unreachable")


# ---------------------------------------------------------------- signing and writing

def aws(*args):
    try:
        done = subprocess.run(["aws", *args], check=True, capture_output=True, text=True, timeout=120)
    except subprocess.CalledProcessError as error:
        raise ListError(f"aws {args[0]} {args[1]} failed: {error.stderr.strip()[:500]}") from error
    return done.stdout.strip()


def kms_public_key(key_id, region):
    der = base64.b64decode(aws("kms", "get-public-key", "--key-id", key_id, "--region", region,
                               "--query", "PublicKey", "--output", "text"))
    if len(der) != 44 or not der.startswith(SPKI_ED25519_PREFIX):
        raise ListError("The KMS key is not an Ed25519 key")
    return der[12:]


def kms_sign(key_id, region, hex_digest):
    with tempfile.TemporaryDirectory() as folder:
        message = Path(folder) / "message"
        message.write_bytes(hex_digest.encode("ascii"))
        return aws("kms", "sign", "--key-id", key_id, "--region", region,
                   "--message", f"fileb://{message}", "--message-type", "RAW",
                   "--signing-algorithm", "ED25519_SHA_512", "--query", "Signature", "--output", "text")


def put_object(bucket, region, key, data):
    with tempfile.TemporaryDirectory() as folder:
        body = Path(folder) / "body"
        body.write_bytes(data)
        aws("s3api", "put-object", "--bucket", bucket, "--region", region, "--key", key,
            "--body", str(body), "--content-type", "application/json", "--cache-control", "max-age=60")


def write_list(args, channel, target, doc, keys):
    """Sign and upload one list, then read it back from the public URL and verify it."""
    index = node_index(args.env)
    bucket, bucket_region, prefix = s3_location(index)
    region = args.region or os.environ.get("AWS_REGION") or bucket_region
    key_id = args.kms_key or f"alias/lastdb-node-release-{args.env}"
    check_list(doc, channel, target)
    raw = canonical(doc)
    digest = hashlib.sha256(raw).hexdigest()
    public = kms_public_key(key_id, region)
    if public not in keys:
        raise ListError("The KMS signing key is not one of the pinned node_release_keys")
    envelope = {"alg": "ed25519", "key_id": hashlib.sha256(public).hexdigest(),
                "payload_sha256": digest, "sig": kms_sign(key_id, region, digest)}
    sig_raw = canonical(envelope)
    verify_pair(raw, sig_raw, keys, channel, target)  # refuse to upload what would not verify
    path = f"{prefix}/{channel}/{target}.json"
    put_object(bucket, bucket_region, path, raw)
    put_object(bucket, bucket_region, path + ".sig", sig_raw)
    last = None
    for _ in range(5):
        got_raw, got_sig = read_pair(index, channel, target)
        if got_raw == raw and got_sig == sig_raw:
            verify_pair(got_raw, got_sig, keys, channel, target)
            return
        last = "the downloaded bytes differ from the uploaded bytes"
        time.sleep(2)
    raise ListError(f"{channel}/{target}: read-back failed: {last}")


def next_doc(current, channel, target, releases):
    return {"list_version": 1, "kind": "lastdb-node", "channel": channel, "target": target,
            "generated_at": fmt(now_utc()), "seq": (current["seq"] if current else 0) + 1,
            "releases": releases}


def load_current(args, channel, target, keys):
    found = read_verified(node_index(args.env), channel, target, keys)
    return found[0] if found else None


def commit(args, channel, target, current, releases, keys, what):
    doc = next_doc(current, channel, target, releases)
    if args.dry_run:
        print(f"[dry-run] {args.env} {channel}/{target}: {what}; would write seq {doc['seq']}")
        return
    write_list(args, channel, target, doc, keys)
    print(f"{args.env} {channel}/{target}: {what}; wrote seq {doc['seq']} and verified it")


# ---------------------------------------------------------------- publish

def gh_json(path):
    done = subprocess.run(["gh", "api", path], check=True, capture_output=True, text=True, timeout=120)
    return json.loads(done.stdout)


def channel_of_tag(tag):
    if NIGHTLY_TAG.fullmatch(tag):
        return "nightly"
    if STABLE_TAG.fullmatch(tag):
        return "stable"
    raise ListError(f"Tag {tag!r} is neither a nightly nor a stable release tag")


def find_release(args):
    if args.tag:
        channel = channel_of_tag(args.tag)
        release = gh_json(f"repos/{REPOS[channel]}/releases/tags/{args.tag}")
        return channel, release
    channel = args.latest
    rows = gh_json(f"repos/{REPOS[channel]}/releases?per_page=100")
    pattern = NIGHTLY_TAG if channel == "nightly" else STABLE_TAG
    rows = [row for row in rows if not row.get("draft") and pattern.fullmatch(row.get("tag_name", ""))
            and row.get("published_at")]
    if not rows:
        raise ListError(f"No published {channel} release was found")
    return channel, max(rows, key=lambda row: row["published_at"])


def download_manifest(repo, tag, name):
    with tempfile.TemporaryDirectory() as folder:
        subprocess.run(["gh", "release", "download", tag, "--repo", repo, "--pattern", name,
                        "--dir", folder], check=True, capture_output=True, text=True, timeout=180)
        path = Path(folder) / name
        if path.is_symlink() or not 0 < path.stat().st_size <= MAX_MANIFEST:
            raise ListError(f"{name} has an invalid size")
        return json.loads(path.read_text())


def build_entry(channel, repo, release, target):
    """The list entry for one target, or None when the release has no asset for it."""
    tag = release["tag_name"]
    if release.get("draft"):
        raise ListError(f"Release {tag} is a draft")
    manifest_name, archive_name = f"lastdb-{target}.manifest.json", f"lastdb-{target}.tar.gz"
    assets = {row["name"]: row for row in release.get("assets", [])}
    if manifest_name not in assets:
        return None
    if archive_name not in assets:
        raise ListError(f"Release {tag} has {manifest_name} but not {archive_name}")
    manifest = download_manifest(repo, tag, manifest_name)
    artifact = manifest.get("artifact", {})
    if (manifest.get("schema") != "lastdb.mini.bundle.v1" or manifest.get("target") != target
            or artifact.get("file") != archive_name or not HEX64.fullmatch(str(artifact.get("sha256")))
            or not _int(artifact.get("size")) or artifact["size"] <= 0):
        raise ListError(f"The {target} manifest of {tag} differs from lastdb.mini.bundle.v1")
    row = assets[archive_name]
    if row.get("size") != artifact["size"]:
        raise ListError("The archive size differs from its manifest")
    if row.get("digest") and row["digest"] != "sha256:" + artifact["sha256"]:
        raise ListError("The archive digest on GitHub differs from its manifest")
    binaries = {item.get("name"): item for item in manifest.get("binaries", [])}
    if set(binaries) != {"lastdb", "lastdbd"} or len(manifest["binaries"]) != 2:
        raise ListError("The manifest must name exactly lastdb and lastdbd")
    entry = {
        "version": manifest["version"],
        "source_git_oid": manifest["source_git_oid"],
        "published_at": fmt(parse_time(release["published_at"], "release published_at")),
        "archive_url": f"https://github.com/{repo}/releases/download/{tag}/{archive_name}",
        "archive_sha256": artifact["sha256"],
        "archive_bytes": artifact["size"],
        "binaries": [{"name": name, "sha256": binaries[name].get("sha256"), "size": binaries[name].get("size")}
                     for name in ("lastdb", "lastdbd")],
        "signing": manifest.get("signing"),
        "rollout": {"percent": ROLLOUT_START[channel], "started_at": fmt(now_utc())},
        "status": "active",
    }
    if entry["signing"] == "ad-hoc":
        raise ListError(f"{tag} {target} is ad-hoc signed; only developer-id releases are listed")
    check_entry(entry, channel, target)
    return entry


def cmd_publish(args):
    keys = pinned_keys(args.env)
    channel, release = find_release(args)
    repo = REPOS[channel]
    listed = 0
    for target in (args.target or TARGETS):
        entry = build_entry(channel, repo, release, target)
        if entry is None:
            print(f"{release['tag_name']}: no {target} asset; that list is unchanged")
            continue
        current = load_current(args, channel, target, keys)
        releases = list(current["releases"]) if current else []
        same = [item for item in releases if item["version"] == entry["version"]]
        if same:
            if same[0]["archive_sha256"] != entry["archive_sha256"] or same[0]["source_git_oid"] != entry["source_git_oid"]:
                raise ListError(f"{entry['version']} is already listed with different bytes")
            print(f"{args.env} {channel}/{target}: {entry['version']} is already listed")
            listed += 1
            continue
        releases.append(entry)
        releases.sort(key=lambda item: parse_time(item["published_at"]), reverse=True)  # stable sort
        if entry not in releases[:MAX_RELEASES]:
            raise ListError(f"{entry['version']} is older than the {MAX_RELEASES} newest releases")
        commit(args, channel, target, current, releases[:MAX_RELEASES], keys, f"listed {entry['version']}")
        listed += 1
    if listed == 0:
        raise ListError(f"{release['tag_name']} has no asset for any requested target")


# ---------------------------------------------------------------- advance and withdraw

def cmd_advance(args):
    keys = pinned_keys(args.env)
    moment = now_utc()
    for target in (args.target or TARGETS):
        current = load_current(args, "stable", target, keys)
        if current is None:
            print(f"{args.env} stable/{target}: no list yet")
            continue
        releases = json.loads(json.dumps(current["releases"]))
        moved = []
        for entry in releases:
            percent = entry["rollout"]["percent"]
            if entry["status"] != "active" or percent >= 100:
                continue
            age = moment - parse_time(entry["rollout"]["started_at"])
            wanted = 100 if age >= timedelta(hours=48) else 50 if age >= timedelta(hours=24) else percent
            if wanted > percent:
                entry["rollout"]["percent"] = wanted
                moved.append(f"{entry['version']} {percent}->{wanted}")
        if not moved:
            print(f"{args.env} stable/{target}: no rollout change")
            continue
        commit(args, "stable", target, current, releases, keys, "advanced " + ", ".join(moved))


def cmd_withdraw(args):
    keys = pinned_keys(args.env)
    pattern = STABLE_VERSION if args.channel == "stable" else NIGHTLY_VERSION
    if not pattern.fullmatch(args.version):
        raise ListError(f"{args.version!r} is not a {args.channel} version")
    reason = args.reason
    if not reason.strip() or len(reason) > 200 or any(ord(c) < 0x20 or ord(c) == 0x7F for c in reason):
        raise ListError("The reason must be one line of 1 to 200 characters")
    found = 0
    for target in (args.target or TARGETS):
        current = load_current(args, args.channel, target, keys)
        entries = [] if current is None else [e for e in current["releases"] if e["version"] == args.version]
        if not entries:
            print(f"{args.env} {args.channel}/{target}: {args.version} is not listed")
            continue
        found += 1
        if entries[0]["status"] == "withdrawn":
            print(f"{args.env} {args.channel}/{target}: {args.version} is already withdrawn")
            continue
        releases = json.loads(json.dumps(current["releases"]))
        for entry in releases:
            if entry["version"] == args.version:
                entry["status"] = "withdrawn"
                entry["withdrawn_reason"] = reason
                entry["withdrawn_at"] = fmt(now_utc())
        commit(args, args.channel, target, current, releases, keys, f"withdrew {args.version}")
    if found == 0:
        raise ListError(f"{args.version} is not in any {args.channel} list of {args.env}")


# ---------------------------------------------------------------- verify

def cmd_verify(args):
    keys = pinned_keys(args.env, args.trust_key)
    index = (args.index_url or node_index(args.env)).rstrip("/")
    bad = 0
    for channel in (args.channel and [args.channel] or CHANNELS):
        for target in (args.target or TARGETS):
            label = f"{args.env} {channel}/{target}"
            try:
                found = read_verified(index, channel, target, keys)
            except ListError as error:
                print(f"FAIL {label}: {error}")
                bad += 1
                continue
            if found is None:
                print(f"{'ok  ' if args.allow_missing else 'FAIL'} {label}: no list")
                bad += 0 if args.allow_missing else 1
                continue
            doc = found[0]
            versions = [e["version"] for e in doc["releases"]]
            line = (f"ok   {label}: seq {doc['seq']}, {len(versions)} entries, newest {versions[0]} "
                    f"({doc['releases'][0]['status']}, {doc['releases'][0]['rollout']['percent']}%), "
                    f"generated {doc['generated_at']}")
            if args.require_version and args.require_version not in versions:
                line = f"FAIL {label}: {args.require_version} is not listed (seq {doc['seq']})"
                bad += 1
            print(line)
    if bad:
        raise ListError(f"{bad} list(s) failed verification")


# ---------------------------------------------------------------- command line

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def common(p, writer=True):
        p.add_argument("--env", choices=ENVS, required=True)
        p.add_argument("--target", action="append", choices=TARGETS,
                       help="limit to this target (repeatable); default all")
        if writer:
            p.add_argument("--dry-run", action="store_true", help="read and plan; sign and upload nothing")
            p.add_argument("--region", help="AWS region for KMS (default AWS_REGION, then the bucket region)")
            p.add_argument("--kms-key", help="KMS key id (default alias/lastdb-node-release-<env>)")

    publish = sub.add_parser("publish", help="list a release")
    common(publish)
    which = publish.add_mutually_exclusive_group(required=True)
    which.add_argument("--tag", help="release tag: nightly-YYYYMMDD-<9 hex> or vX.Y.Z")
    which.add_argument("--latest", choices=CHANNELS, help="the newest published release of a channel")
    publish.set_defaults(run=cmd_publish)

    advance = sub.add_parser("advance", help="advance the stable rollout percentages")
    common(advance)
    advance.set_defaults(run=cmd_advance)

    withdraw = sub.add_parser("withdraw", help="withdraw one release; never undone")
    common(withdraw)
    withdraw.add_argument("--channel", choices=CHANNELS, required=True)
    withdraw.add_argument("--version", required=True)
    withdraw.add_argument("--reason", required=True)
    withdraw.set_defaults(run=cmd_withdraw)

    verify = sub.add_parser("verify", help="read lists from their public URLs and verify them")
    common(verify, writer=False)
    verify.add_argument("--channel", choices=CHANNELS)
    verify.add_argument("--trust-key", action="append", default=[],
                        help="base64 public key to trust instead of environments.json (verify only)")
    verify.add_argument("--index-url", help="read from this node_index instead of the environment's")
    verify.add_argument("--allow-missing", action="store_true")
    verify.add_argument("--require-version", help="fail unless this version is in each list read")
    verify.set_defaults(run=cmd_verify)

    args = parser.parse_args()
    try:
        args.run(args)
    except (ListError, subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError, KeyError) as error:
        detail = error.stderr.strip()[:500] if isinstance(error, subprocess.CalledProcessError) and error.stderr else error
        print(f"node_list: {type(error).__name__}: {detail}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
