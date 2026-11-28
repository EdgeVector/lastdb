#!/usr/bin/env python3
"""Updater proof: the real `lastdb upgrade` works on a real service before PROD lists a release.

Contract: docs/designs/node-self-update.md section 1 (M6). The proof reads the DEV list that
node-list-publish.yml has just signed with the DEV role (a binary reads the DEV list when
LASTDB_ENV=dev; nothing overrides the pinned keys). It writes nothing to any list and runs on
a fresh synthetic home under /tmp of a CI runner.

  targets  print the matrix of targets whose DEV list holds the build under test
  run      1. install the previous listed release as a service and write a known record
           2. `lastdb upgrade` to the build under test; require exit 0, the new build, the record,
              current/previous and a history line
           3. `lastdb upgrade --pair` to a candidate whose daemon exits at once; require exit 3,
              the good build running again, the record, and the version in upgrade/rejected

When no older listed release holds `lastdb service install` (the first release with the updater,
or the first entry of a target), step 1 installs the build under test itself and step 2 only
proves that `lastdb upgrade` reads and verifies the list and finds nothing newer. The summary
says "bootstrap". Step 3 always runs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from node_list_read import node_index, pinned_keys, read_verified  # noqa: E402
from node_updater_host import (Fail, Host, broken_pair, read_record, require, running_build,  # noqa: E402
                               say, seed_and_write)

TARGETS = {
    "x86_64-unknown-linux-gnu": "ubuntu-24.04",
    "aarch64-unknown-linux-gnu": "ubuntu-24.04-arm",
    "aarch64-apple-darwin": "macos-15",
}


def summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(text + "\n")


# ---------------------------------------------------------------- the list

def read_list(channel, target):
    found = read_verified(node_index("dev"), channel, target, pinned_keys("dev"))
    return None if found is None else found[0]


def select(doc, tag):
    """The entry under test: the one `tag` names, else the newest."""
    if not tag:
        return doc["releases"][0]
    if tag.startswith("v"):
        wanted = tag[1:]
        hits = [e for e in doc["releases"] if e["version"] == wanted]
    else:
        suffix = ".g" + tag.rsplit("-", 1)[1]
        hits = [e for e in doc["releases"] if e["version"].endswith(suffix)]
    return hits[0] if hits else None


def cmd_targets(args):
    found = []
    for target, runner in TARGETS.items():
        doc = read_list(args.channel, target)
        entry = select(doc, args.tag) if doc else None
        if entry and entry["status"] == "active":
            found.append({"target": target, "runner": runner, "version": entry["version"]})
    print(json.dumps(found, separators=(",", ":")))


# ---------------------------------------------------------------- archives

def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch_pair(entry, folder):
    """Download a listed archive, check its hashes, unpack the two binaries into `folder`."""
    folder.mkdir(parents=True)
    archive = folder / "archive.tar.gz"
    with urllib.request.urlopen(entry["archive_url"], timeout=300) as reply, open(archive, "wb") as out:
        shutil.copyfileobj(reply, out)
    require(archive.stat().st_size == entry["archive_bytes"], f"{entry['version']}: archive size differs from the list")
    require(sha256(archive) == entry["archive_sha256"], f"{entry['version']}: archive hash differs from the list")
    with tarfile.open(archive) as tar:
        names = sorted(m.name.lstrip("./") for m in tar.getmembers())
        require(names == ["lastdb", "lastdbd"], f"{entry['version']}: archive holds {names}")
        tar.extractall(folder, filter="data")
    archive.unlink()
    for binary in entry["binaries"]:
        path = folder / binary["name"]
        require(sha256(path) == binary["sha256"], f"{entry['version']}: {binary['name']} hash differs from the list")
        path.chmod(0o755)
    return folder


def can_install_service(folder):
    done = subprocess.run([str(folder / "lastdb"), "service", "install", "--help"],
                          capture_output=True, text=True, timeout=60, check=False)
    return done.returncode == 0

# ---------------------------------------------------------------- the proof

def step_install(host, base, base_dir, bootstrap, channel, results):
    say(f"1. install {base['version']} as a service" + (" (bootstrap: the build under test)" if bootstrap else ""))
    done = host.lastdb(base_dir, "service", "install", "--json")
    require(done.returncode == 0, f"service install exited {done.returncode}")
    host.set_soak(10)
    # Without this the upgrade in step 2 follows the default stable channel, not the list under test.
    host.set_channel(channel)
    require(host.link("current") == f"programs/{base['version']}", f"current is {host.link('current')}")
    note = f"proof-{os.urandom(6).hex()}"
    seed_and_write(host.sock, note)
    require(read_record(host.sock)["note"] == note, "the known record does not read back")
    require(running_build(host.sock) == base["version"], "the installed build is not running")
    results.append(f"1 install {base['version']}: ok")
    return note


def step_upgrade(host, under, under_dir, base, bootstrap, note, results):
    say("2. lastdb upgrade finds nothing newer (bootstrap)" if bootstrap else "2. lastdb upgrade to the build under test")
    done = host.lastdb(under_dir, "upgrade", "--json")
    require(done.returncode == 0, f"upgrade exited {done.returncode}, not 0")
    require(running_build(host.sock) == under["version"], "the build under test is not running")
    require(host.link("current") == f"programs/{under['version']}", f"current is {host.link('current')}")
    if not bootstrap:
        require(host.link("previous") == f"programs/{base['version']}", f"previous is {host.link('previous')}")
        last = host.history()[-1]
        require(last["result"] == "upgraded" and last["to"] == under["version"] and last["from"] == base["version"],
                f"history says {last!r}")
    require(read_record(host.sock)["note"] == note, "the known record changed")
    results.append(f"2 upgrade to {under['version']}: ok" + (" (bootstrap: no newer release, list verified)" if bootstrap else ""))


def step_rollback(host, under, under_dir, note, results):
    good, before_previous = host.link("current"), host.link("previous")
    broken_version = f"0.0.0-proofbroken.{int(time.time())}"
    say(f"3. lastdb upgrade --pair a candidate whose daemon exits at once ({broken_version})")
    pair = broken_pair(host, host.root / "broken", broken_version)
    started = time.monotonic()
    done = host.lastdb(under_dir, "upgrade", "--pair", str(pair), "--json")
    require(done.returncode == 3, f"upgrade --pair exited {done.returncode}, not 3 (rolled back)")
    require(host.link("current") == good, f"current is {host.link('current')}, not {good}")
    require(host.link("previous") == before_previous, f"previous is {host.link('previous')}")
    require(running_build(host.sock) == under["version"], "the good build does not run after the rollback")
    require(read_record(host.sock)["note"] == note, "the known record changed after the rollback")
    require(broken_version in host.rejected(), f"{broken_version} is not in upgrade/rejected: {host.rejected()}")
    last = host.history()[-1]
    require(last["result"] == "rolled-back" and last["to"] == broken_version, f"history says {last!r}")
    results.append(f"3 broken candidate rolled back in {int(time.monotonic() - started)} s: ok")


def choose_base(host, older, under, under_dir):
    """The newest older listed release that holds `lastdb service install`, else the build under test."""
    for index, entry in enumerate(older):
        folder = fetch_pair(entry, host.root / f"base-{index}")
        if can_install_service(folder):
            return entry, folder, False
        say(f"{entry['version']} has no `lastdb service install`; skipped as the previous release")
    return under, under_dir, True


def cmd_run(args):
    doc = read_list(args.channel, args.target)
    require(doc is not None, f"the DEV {args.channel}/{args.target} list does not exist")
    under = select(doc, args.tag)
    require(under is not None and under["status"] == "active", "the build under test is not an active entry of the DEV list")
    older = [e for e in doc["releases"] if e["published_at"] < under["published_at"] and e["status"] == "active"]
    host, results, bootstrap = Host(), [], True
    try:
        say(f"build under test {under['version']} ({args.channel}, {args.target}); {len(older)} older listed release(s)")
        under_dir = fetch_pair(under, host.root / "under")
        require(can_install_service(under_dir), "the build under test has no `lastdb service install`")
        base, base_dir, bootstrap = choose_base(host, older, under, under_dir)
        host.prepare()
        note = step_install(host, base, base_dir, bootstrap, args.channel, results)
        step_upgrade(host, under, under_dir, base, bootstrap, note, results)
        step_rollback(host, under, under_dir, note, results)
    except BaseException:
        host.diagnostics()
        summary(f"### Updater proof {args.target} FAILED\n\n" + "\n".join(f"- {r}" for r in results))
        raise
    finally:
        host.cleanup()
    summary(f"### Updater proof {args.target}: passed\n\nBuild under test `{under['version']}` ({args.channel}). "
            + ("Bootstrap: no older listed release holds the updater, so step 2 only proves the list path.\n\n" if bootstrap else "\n\n")
            + "\n".join(f"- {r}" for r in results))
    say("passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name, run in (("targets", cmd_targets), ("run", cmd_run)):
        p = sub.add_parser(name)
        p.add_argument("--channel", choices=("stable", "nightly"), required=True)
        p.add_argument("--tag", default="", help="release tag under test; default the newest entry")
        if name == "run":
            p.add_argument("--target", choices=sorted(TARGETS), required=True)
        p.set_defaults(run=run)
    args = parser.parse_args()
    try:
        args.run(args)
    except Fail as error:
        print(f"node_updater_proof: FAILED: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
