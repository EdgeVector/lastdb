#!/usr/bin/env python3
"""A home written and connected by the previous published build continues on the build under test.

Every future upgrade of a primary node is this: the home that the PREVIOUS build made and wrote
goes on with the NEW build. The other proofs start from an empty home made by the build under
test, so none of them reads a file that an older build wrote. This one does.

The previous build is the newest published, non-draft `nightly-*` release of EdgeVector/lastdb
that has `lastdb-x86_64-unknown-linux-gnu.tar.gz` and was not built from the commit under test.
Its tarball is checked against `SHA256SUMS.txt` and its binaries against the manifest. The tag,
its source commit and its version are printed and go to the job summary. Real processes only:
the two builds' `lastdbd` and `lastdb`, and the `reference_relay` built from the source under test.

1. Previous build: `backup init`, `backup connect --existing` (saves `cloud-credentials.json` with
   its own code, offline), a daemon with the relay set. Records in three schemas (single writes,
   batches, replaces, deletes, an alternate key, a file). Every write is acknowledged and on the
   relay, a snapshot is taken with `backup snapshot --wait`, a tail of writes follows, and the
   daemon stops cleanly.
   If the previous build keeps its spool in `data/disk-run/accepted-v1/`, the one manual upgrade
   step follows: its state shows every write acknowledged and nothing pending, and the folder is
   moved out of the home. The build under test refuses a home that still has it.
2. Build under test: a daemon on the SAME home with the same environment. It starts, `lastdb
   status` reports cloud sync connected with no error and the two builds agree, every record
   reads back equal by key and by alternate key, the file matches, new writes are acknowledged
   and follow the head, and `backup snapshot --wait` succeeds.
3. Build under test: a restore from the recovery phrase into a fresh home, from the previous
   build's snapshot and the entries after it, equals the upgraded home key by key.
4. Build under test: after its own snapshot and a tail, a second restore installs that snapshot,
   replays only the tail and equals the upgraded home.

A step that the previous build cannot run fails the proof. It never skips.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from harness import append_step_summary  # noqa: E402
from personal_proof_cleanup import cleanup_owned  # noqa: E402
from existing_home_proof import snapshots_on, write_after, write_before  # noqa: E402
from personal_proof_nodes import ALPHA, MARK, Device, Relay, converged, create, delete, expect, replace, wait  # noqa: E402
from personal_proof_status import swept  # noqa: E402
from personal_proof_steps import gamma_view  # noqa: E402
from record_runs import require  # noqa: E402

REPOSITORY = "EdgeVector/lastdb"
TARGET = "x86_64-unknown-linux-gnu"
ASSET = f"lastdb-{TARGET}.tar.gz"
# A syntactically valid key for `backup connect --existing`, which saves it and asks no one. The
# relay in this proof is the reference relay that LASTDB_RELAY_URL names, so no request carries it.
PLACEHOLDER_KEY = "em_" + "ab" * 24
CANDIDATES = 60


def gh(*arguments):
    result = subprocess.run(["gh", *arguments], capture_output=True, text=True, check=False, timeout=600)
    require(result.returncode == 0, f"gh {' '.join(arguments[:3])} failed: {result.stderr[-800:]}")
    return result.stdout


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def listed_sums(path):
    """`SHA256SUMS.txt`: file name -> digest."""
    found = {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            found[parts[1].lstrip("*")] = parts[0]
    return found


def check_sum(folder, sums, name):
    require(name in sums, f"SHA256SUMS.txt does not list {name}")
    require(sha256(folder / name) == sums[name], f"{name} does not match SHA256SUMS.txt")


def fetch_previous(work, excluded):
    """The newest published nightly that is not the commit under test: its binaries, tag, commit and version."""
    releases = json.loads(gh("release", "list", "-R", REPOSITORY, "--limit", str(CANDIDATES),
                             "--json", "tagName,isDraft"))
    skipped = []
    for release in releases:
        tag = release["tagName"]
        if release["isDraft"] or not tag.startswith("nightly-"):
            continue
        names = {asset["name"] for asset in json.loads(gh("release", "view", tag, "-R", REPOSITORY,
                                                          "--json", "assets"))["assets"]}
        if not {ASSET, "nightly.json", "SHA256SUMS.txt"} <= names:
            skipped.append(f"{tag} (no {ASSET})")
            continue
        folder = work / tag
        folder.mkdir(mode=0o700, parents=True)
        for name in ("nightly.json", "SHA256SUMS.txt"):
            gh("release", "download", tag, "-R", REPOSITORY, "-p", name, "-D", str(folder))
        sums = listed_sums(folder / "SHA256SUMS.txt")
        check_sum(folder, sums, "nightly.json")
        manifest = json.loads((folder / "nightly.json").read_text())["platforms"][TARGET]["manifest"]
        if manifest["source_git_oid"] in excluded:
            skipped.append(f"{tag} (is the commit under test)")
            continue
        gh("release", "download", tag, "-R", REPOSITORY, "-p", ASSET, "-D", str(folder))
        check_sum(folder, sums, ASSET)
        binaries = folder / "bin"
        binaries.mkdir(mode=0o700)
        wanted = {entry["name"]: entry for entry in manifest["binaries"]}
        with tarfile.open(folder / ASSET, "r:gz") as archive:
            for member in archive.getmembers():
                name = Path(member.name).name
                if member.isfile() and name in ("lastdb", "lastdbd"):
                    (binaries / name).write_bytes(archive.extractfile(member).read())
                    (binaries / name).chmod(0o755)
        for name in ("lastdb", "lastdbd"):
            require((binaries / name).is_file(), f"{ASSET} of {tag} holds no {name}")
            require(sha256(binaries / name) == wanted[name]["sha256"], f"{name} of {tag} does not match its manifest")
        return {"tag": tag, "source_git_oid": manifest["source_git_oid"], "version": manifest["version"],
                "daemon": binaries / "lastdbd", "cli": binaries / "lastdb", "skipped": skipped}
    raise RuntimeError(f"no published nightly of {REPOSITORY} among the newest {CANDIDATES} releases has {ASSET} "
                       f"and a commit other than the one under test; skipped: {skipped}")


def status_env(device):
    return {"LASTDB_SOCKET_PATH": str(device.node.socket), "LASTDB_HOME": str(device.node.home)}


def check_account(device, had_account):
    """The account of the build under test says whether it began before this home's first upload.

    A home that the previous build had already uploaded from (no account then) starts the account at
    its first entry on this build, so it must say it is not complete, since when, and `lastdb status`
    must say that earlier uploads are not counted and must not claim what the home holds on the relay.
    """
    usage = wait(f"{device.name} to show its cloud usage", lambda: (device.sync_status().get("databases", {})
                 .get(device.database, {}).get("usage")))
    require(usage["since_ms"] > 0 and usage["entries"] >= 8, f"{device.name}: the account {usage!r}")
    require(usage["complete"] is had_account, f"{device.name}: complete is {usage['complete']}, the previous build "
            f"{'kept' if had_account else 'kept no'} account: {usage!r}")
    text = subprocess.run([str(device.cli), "status"], capture_output=True, text=True, timeout=120, check=False,
                          env={**device.cli_env(), **status_env(device)}).stdout
    line = next((line for line in text.splitlines() if line.startswith("Cloud usage ")), "")
    if had_account:
        require("on the relay from this device" in line and "since" not in line, f"status line: {line!r}")
    else:
        require("since " in line and "earlier uploads are not counted" in line and "on the relay from this device" not in line,
                f"status line: {line!r}")
    print(f"   the account of the build under test: complete={usage['complete']}, {usage['entries']} entries put since "
          f"{usage['since_ms']}; status says: {line}", flush=True)


def settled_status(device):
    """The cloud sync status once this database's upload has run a turn: connected, no error, captures on."""
    def probe():
        sync = device.sync_status()
        return (isinstance(sync, dict) and device.database in sync.get("databases", {})
                and not sync.get("last_error") and sync.get("captures_accepted") is True and sync)
    sync = wait(f"{device.name} to report its cloud sync", probe)
    require(sync.get("connected") is True and sync.get("environment") == "dev",
            f"{device.name}: the saved credentials were not accepted: {sync!r}")
    report = device.run(["status", "--json"], env=status_env(device))
    require(report.get("running") is True and report.get("build_agreement") == "match",
            f"{device.name}: lastdb status reports {report!r}")
    cloud = report.get("cloud_sync") or {}
    require(cloud.get("connected") is True and not cloud.get("last_error"),
            f"{device.name}: lastdb status reports cloud sync {cloud!r}")
    return sync


def heads(device):
    state = device.spool_state()
    return state["head"], state["acked"]


def leave_old_spool(device, out):
    """The manual upgrade step: the stopped previous build sent every write, so its spool folder goes."""
    folder = device.old_spool_dir()
    if not folder.exists():
        return False
    state = device.spool_state()
    require(state is not None and state["head"] == state["acked"] and not state["pending"],
            f"the stopped previous build left a spool that is not fully sent: {state!r}")
    folder.rename(out / "previous-build-spool")
    return True


def prove(relay, previous, devices, out, summary):
    old, upgraded, first, second = devices
    folder, source = out / "file-dir", out / "source-files"
    folder.mkdir(mode=0o700)
    source.mkdir(mode=0o700)
    state = {}

    # 1. The previous build makes, connects and writes the home.
    phrase = old.init()
    old.run(["--data-dir", str(old.node.home), "backup", "connect", "--existing", "--env", "dev",
             "--api-key-stdin", "--json"], text=PLACEHOLDER_KEY)
    require((old.node.home / "cloud-credentials.json").is_file(), "the previous build saved no cloud-credentials.json")
    old.start()
    keys, gamma = write_before(old, folder, source, state)
    wait("the previous build to upload every write", old.synced_up)
    snapshot_head, _ = heads(old)
    old.snapshot_now()
    require(snapshots_on(relay, old.database) == 1, "the previous build's snapshot is not on the relay once")
    tail = write_after(old, keys, state)
    wait("the previous build to upload its tail", old.synced_up)
    head, acked = heads(old)
    require(head == acked == snapshot_head + tail, f"the previous build holds head {head}, acknowledged {acked}")
    require(old.snapshot() == state and gamma_view(old, folder) == gamma,
            "the previous build does not hold what it was given")
    old.stop()
    moved = leave_old_spool(old, out)
    print(f"1. {previous['tag']} (commit {previous['source_git_oid'][:9]}) made a connected home of {len(state)} records "
          f"with a snapshot at head {snapshot_head} and a tail of {tail} writes, all acknowledged"
          + ("; its sent spool folder was moved out of the home (the manual upgrade step)" if moved else ""),
          flush=True)
    summary.append(f"The previous build wrote {len(state)} records in three schemas, a file, an alternate key, "
                   f"a snapshot at head {snapshot_head} and a tail of {tail} writes.")

    # 2. The build under test continues on the same home with the same environment.
    upgraded.node.home = old.node.home
    # The previous build may or may not keep an account of what it put on the relay (`cloud-usage.json`).
    had_account = (old.log_dir() / "cloud-usage.json").exists()
    upgraded.start()
    require(upgraded.database == old.database, "the build under test opened another database")
    sync = settled_status(upgraded)
    wait("the home to read what it held", lambda: converged((upgraded,), state))
    view = upgraded.out / "file-dir"
    view.mkdir(mode=0o700)
    require(gamma_view(upgraded, view) == gamma, "the upgraded home differs in alternate keys or file")
    wait("the upgraded home to hold its spool", upgraded.synced_up)
    before, _ = heads(upgraded)
    require(before == head, f"the upgraded home holds head {before}, the previous build left {head}")
    fresh = [f"up{n}" for n in range(6)]
    upgraded.write([create(ALPHA, key, "after") for key in fresh])
    for key in fresh:
        expect(state, ALPHA, key, "after")
    upgraded.change([replace(ALPHA, fresh[0], "after-two")])
    expect(state, ALPHA, fresh[0], "after-two")
    upgraded.change([delete(ALPHA, fresh[1])])
    state.pop((ALPHA, f"{MARK}-key-{fresh[1]}"))
    wait("the upgraded home to upload", upgraded.synced_up)
    after, acked = heads(upgraded)
    require(after == before + 8 and acked == after, f"after 8 writes the spool holds head {after}, acknowledged {acked}")
    floor = relay.stream_floor(upgraded.database, upgraded.device_id())
    require(0 <= floor <= snapshot_head, "the relay trimmed beyond the previous build's snapshot")
    require(relay.stream_entries(upgraded.database, upgraded.device_id()) == list(range(floor + 1, after + 1)),
            "the relay does not hold every entry after its snapshot-covered floor across both builds")
    require(upgraded.snapshot() == state, "the upgraded home does not read its old and new records")
    check_account(upgraded, had_account)
    print(f"2. the build under test started on the same home: cloud sync connected with no error ({sync['environment']}), "
          f"every record read by key and alternate key, 8 new writes took head {before} to {after}, all acknowledged",
          flush=True)

    # 3. A restore now reads the previous build's snapshot and the entries after it.
    report = restore_and_compare(first, phrase, upgraded, state, gamma, replayed=after - snapshot_head)
    first.stop()
    print(f"3. a restore from the phrase installed the previous build's snapshot, replayed the {report['replayed']} "
          "entries after it, and equals the upgraded home key by key", flush=True)

    # 4. The build under test takes its own snapshot; a restore installs it and replays only the tail after it.
    count = snapshots_on(relay, upgraded.database)
    taken = upgraded.snapshot_now()
    # The build under test keeps one snapshot on the relay: its own replaces the previous build's.
    require(taken["snapshot_seq"] == count + 1 and snapshots_on(relay, upgraded.database) == 1,
            f"the snapshot of the build under test is not the one snapshot on the relay: {taken!r}")
    swept(upgraded, taken["snapshot_seq"])
    for key in fresh[2:5]:
        upgraded.change([replace(ALPHA, key, "tail")])
        expect(state, ALPHA, key, "tail")
    wait("the upgraded home to upload its tail", upgraded.synced_up)
    restore_and_compare(second, phrase, upgraded, state, gamma, replayed=3)
    print("4. the build under test took a snapshot; a second restore installed it, replayed the 3 later entries "
          "and equals the upgraded home", flush=True)
    summary.append("The build under test continued on the home with its credentials, acknowledged new writes, "
                   "took a snapshot, and two restores from the recovery phrase equal it.")


def restore_and_compare(device, phrase, upgraded, state, gamma, replayed):
    report = device.restore(phrase)
    require(report["snapshot"] is not None, f"{device.name}: the restore found no snapshot: {report!r}")
    require(report["replayed"] == replayed,
            f"{device.name}: the restore replayed {report['replayed']} entries, expected {replayed}")
    device.start()
    wait(f"{device.name} to equal the upgraded home", lambda: converged((device,), state))
    require(device.snapshot() == upgraded.snapshot(), f"{device.name} differs from the upgraded home key by key")
    folder = device.out / "file-dir"
    folder.mkdir(mode=0o700)
    require(gamma_view(device, folder) == gamma, f"{device.name} differs in alternate keys or file")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    for name in ("daemon", "cli", "relay-bin", "revision"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--exclude-oid", action="append", default=[],
                        help="a commit that a nightly must not be built from: the commit under test")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    previous = fetch_previous(args.output / "previous-builds", {args.revision, *args.exclude_oid})
    print(f"previous build: {previous['tag']}, commit {previous['source_git_oid']}, version {previous['version']}"
          + (f" (skipped {previous['skipped']})" if previous["skipped"] else ""), flush=True)
    header = (f"## A home from the previous nightly continues on the build under test\n"
              f"Previous build: `{previous['tag']}`, source commit `{previous['source_git_oid']}`, "
              f"version `{previous['version']}`. Build under test: `{args.revision}`.\n")
    if previous["skipped"]:
        header += f"Newer nightlies skipped: {', '.join(previous['skipped'])}.\n"
    append_step_summary(header + "\n")
    relay = Relay(Path(args.relay_bin), args.output)
    relay.start()
    old_args = argparse.Namespace(daemon=str(previous["daemon"]), cli=str(previous["cli"]),
                                  revision=previous["source_git_oid"])
    old = Device("previous-build", old_args, args.output, relay)
    old.node.version_marker = previous["version"]
    upgraded = Device("upgraded-home", args, args.output, relay)
    first = Device("restored-from-previous-snapshot", args, args.output, relay)
    second = Device("restored-after-upgrade", args, args.output, relay)
    devices = (old, upgraded, first, second)
    summary, failure = [], None
    try:
        prove(relay, previous, devices, args.output, summary)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        for device in devices:
            print(f"--- {device.name}: {device.entries()}", file=sys.stderr)
            print("\n".join(line[:400] for line in device.warnings()), file=sys.stderr)
        summary.append(f"FAILED: {error}")
    finally:
        # The upgraded node shares the home of the previous one: its root is removed first, the home last.
        cleanup_owned((second, first, upgraded, old), (relay,))
    append_step_summary("\n".join(summary) + "\n")
    if failure is not None:
        raise SystemExit(1)
    print("Upgrade proof passed", flush=True)


if __name__ == "__main__":
    main()
