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
import json
import subprocess
import sys
from datetime import timedelta

from node_list_read import node_index, pinned_keys, read_verified
from node_list_release import REPOS, build_entry, find_release
from node_list_rules import (CHANNELS, MAX_RELEASES, NIGHTLY_VERSION, STABLE_VERSION, TARGETS,
                             ListError, fmt, now_utc, parse_time)
from node_list_write import commit, load_current

ENVS = ("prod", "dev")


def cmd_publish(args):
    keys = pinned_keys(args.env)
    channel, release = find_release(args)
    repo = REPOS[channel]
    listed = 0
    for target in (args.target or TARGETS):
        entry = build_entry(channel, repo, release, target)
        if entry is None:
            print(f"{release['tag_name']}: no listable {target} asset; that list is unchanged")
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
