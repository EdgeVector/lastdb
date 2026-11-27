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
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from node_list_read import node_index, pinned_keys, read_verified  # noqa: E402

TARGETS = {
    "x86_64-unknown-linux-gnu": "ubuntu-24.04",
    "aarch64-unknown-linux-gnu": "ubuntu-24.04-arm",
    "aarch64-apple-darwin": "macos-14",
}
LABEL = "com.edgevector.lastdbd"
SCHEMA = "updaterproof"
KEY = "known-record"
BROKEN_C = r"""
#include <stdio.h>
#include <string.h>
int main(int argc, char **argv) {
  if (argc > 1 && strcmp(argv[1], "--version") == 0) { printf("%s %s\n", NAME, VERSION); return 0; }
  if (argc > 1 && strcmp(argv[1], "--accepted-home-format") == 0) {
    FILE *f = fopen(MARKER, "rb"); char buf[256]; size_t n;
    if (!f) return 1;
    n = fread(buf, 1, sizeof buf, f); fclose(f); fwrite(buf, 1, n, stdout); return 0;
  }
  fprintf(stderr, "deliberately broken candidate: the daemon exits at once\n");
  return 1;
}
"""


class Fail(Exception):
    pass


def require(ok, message):
    if not ok:
        raise Fail(message)


def say(text):
    print(f"proof: {text}", flush=True)


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


# ---------------------------------------------------------------- the node

class Unix(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=60)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(60)
        self.sock.connect(self.path)


def call(sock, path, body=None):
    conn = Unix(sock)
    try:
        conn.request("GET" if body is None else "POST", path,
                     None if body is None else json.dumps(body),
                     {"Content-Type": "application/json", "X-LastDB-Client": "node-updater-proof"})
        reply = conn.getresponse()
        raw = reply.read()
    finally:
        conn.close()
    require(reply.status == 200, f"{path}: HTTP {reply.status}: {raw[:300]!r}")
    doc = json.loads(raw)
    return doc.get("data", doc)


def seed_and_write(sock, value):
    fields = ["id", "note"]
    schema = {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "Hash",
        "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": fields, "field_types": {f: "String" for f in fields},
        "field_descriptions": {f: "Updater proof " + f for f in fields}, "field_versions": {f: 1 for f in fields},
    }
    call(sock, "/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})
    reply = call(sock, "/v1/write", {"mutations": [
        {"schema": SCHEMA, "op": "create", "key": {"hash": KEY}, "record": {"id": KEY, "note": value}}]})
    require(reply.get("written") == 1, f"the known record was not written: {reply!r}")


def read_record(sock):
    reply = call(sock, "/v1/read", {"reads": [{"schema": SCHEMA, "key": {"hash": KEY}}]})
    records = reply["results"][0]["records"]
    require(len(records) == 1, f"the known record reads as {records!r}")
    return records[0]["record"]


def running_build(sock):
    return call(sock, "/api/system/boot-identity")["build"].split()[0]


class Host:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="lu-", dir="/tmp")).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.mac = sys.platform == "darwin"
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(("LASTDB", "FOLD", "EXEMEM"))}
        self.env.update(LASTDB_HOME=str(self.home), LASTDB_ENV="dev", RUST_LOG="off")
        self.sock = self.home / "data" / "folddb.sock"

    def manager(self, *args):
        return subprocess.run(args, capture_output=True, text=True, timeout=120, check=False)

    def prepare(self):
        """The daemon of the service needs these to accept a seeded schema and stay quiet."""
        daemon = {"LASTDB_ISOLATED_COPY": "1", "LASTDB_SUPPRESS_RECOVERY_PHRASE": "1", "LASTDB_ENV": "dev",
                  "RUST_MIN_STACK": "16777216"}
        if self.mac:
            for name, value in daemon.items():
                require(self.manager("launchctl", "setenv", name, value).returncode == 0, f"launchctl setenv {name} failed")
            return
        uid = os.getuid()
        self.env.setdefault("XDG_RUNTIME_DIR", f"/run/user/{uid}")
        self.env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path=/run/user/{uid}/bus")
        os.environ.update({k: self.env[k] for k in ("XDG_RUNTIME_DIR", "DBUS_SESSION_BUS_ADDRESS")})
        state = self.manager("systemctl", "--user", "is-system-running")
        require(state.stdout.strip() in ("running", "degraded"),
                f"no systemd user manager here (is-system-running: {state.stdout.strip()!r} {state.stderr.strip()!r})")
        require(self.manager("systemctl", "--user", "set-environment",
                             *[f"{k}={v}" for k, v in daemon.items()]).returncode == 0, "systemctl set-environment failed")

    def lastdb(self, folder, *args, timeout=1500):
        done = subprocess.run([str(folder / "lastdb"), *args], env=self.env, capture_output=True, text=True,
                              timeout=timeout, check=False)
        say(f"lastdb {' '.join(args)} -> exit {done.returncode}: {done.stdout.strip()[:300]}")
        for line in done.stderr.strip().splitlines()[-12:]:
            print(f"    {line}")
        return done

    def link(self, name):
        path = self.home / name
        return os.readlink(path) if path.is_symlink() else None

    def history(self):
        path = self.home / "upgrade" / "history.jsonl"
        lines = path.read_text().splitlines() if path.exists() else []
        return [json.loads(line) for line in lines]

    def rejected(self):
        path = self.home / "upgrade" / "rejected"
        return path.read_text().split() if path.exists() else []

    def set_soak(self, seconds):
        config = self.home / "upgrade" / "config.toml"
        text = config.read_text()
        text, changed = re.subn(r"^soak_secs = \d+$", f"soak_secs = {seconds}", text, flags=re.M)
        require(changed == 1, "config.toml has no soak_secs line")
        config.write_text(text)

    def diagnostics(self):
        print("---- diagnostics ----")
        for name in ("upgrade/history.jsonl", "logs/lastdbd.err.log", "logs/lastdbd.out.log"):
            path = self.home / name
            if path.exists():
                print(f"== {name}")
                print("\n".join(path.read_text(errors="replace").splitlines()[-40:]))
        if not self.mac:
            print(self.manager("systemctl", "--user", "status", LABEL + ".service", "--no-pager", "-l").stdout[-2500:])
            print(self.manager("journalctl", "--user", "-u", LABEL + ".service", "-n", "40", "--no-pager").stdout[-2500:])

    def cleanup(self):
        """Stop and remove what this proof installed. The runner is a throwaway machine, but a rerun is not."""
        home = Path(os.environ["HOME"])
        if self.mac:
            self.manager("launchctl", "bootout", f"gui/{os.getuid()}/{LABEL}")
            (home / "Library/LaunchAgents" / f"{LABEL}.plist").unlink(missing_ok=True)
            for name in ("LASTDB_ISOLATED_COPY", "LASTDB_SUPPRESS_RECOVERY_PHRASE", "LASTDB_ENV", "RUST_MIN_STACK"):
                self.manager("launchctl", "unsetenv", name)
        else:
            self.manager("systemctl", "--user", "disable", "--now", "lastdb-upgrade.timer")
            self.manager("systemctl", "--user", "stop", LABEL + ".service")
            self.manager("systemctl", "--user", "reset-failed", LABEL + ".service")
            for name in (LABEL + ".service", "lastdb-upgrade.service", "lastdb-upgrade.timer"):
                (home / ".config/systemd/user" / name).unlink(missing_ok=True)
            self.manager("systemctl", "--user", "daemon-reload")
            self.manager("systemctl", "--user", "unset-environment", "LASTDB_ISOLATED_COPY",
                         "LASTDB_SUPPRESS_RECOVERY_PHRASE", "LASTDB_ENV", "RUST_MIN_STACK")
        link = home / ".local/bin/lastdb"
        if link.is_symlink():
            link.unlink()
        shutil.rmtree(self.root, ignore_errors=True)


def broken_pair(host, folder, version):
    folder.mkdir()
    marker = host.home / "data" / "laststore-pack-node-v1"
    source = folder / "broken.c"
    source.write_text(BROKEN_C)
    for name in ("lastdb", "lastdbd"):
        flags = [f'-DNAME="{name}"', f'-DVERSION="{version}"', f'-DMARKER="{marker}"']
        done = subprocess.run(["cc", "-O1", *flags, "-o", str(folder / name), str(source)],
                              capture_output=True, text=True, timeout=120, check=False)
        require(done.returncode == 0, f"cc failed: {done.stderr[-500:]}")
        if sys.platform == "darwin":
            subprocess.run(["codesign", "--force", "--sign", "-", str(folder / name)], check=True, timeout=60)
    source.unlink()
    return folder


# ---------------------------------------------------------------- the proof

def cmd_run(args):
    doc = read_list(args.channel, args.target)
    require(doc is not None, f"the DEV {args.channel}/{args.target} list does not exist")
    under = select(doc, args.tag)
    require(under is not None and under["status"] == "active", "the build under test is not an active entry of the DEV list")
    older = [e for e in doc["releases"] if e["published_at"] < under["published_at"] and e["status"] == "active"]
    host = Host()
    results = []
    try:
        say(f"build under test {under['version']} ({args.channel}, {args.target}); {len(older)} older listed release(s)")
        under_dir = fetch_pair(under, host.root / "under")
        base, base_dir = None, None
        for entry in older:
            folder = fetch_pair(entry, host.root / f"base-{len(results)}-{entry['version'][-12:]}")
            if can_install_service(folder):
                base, base_dir = entry, folder
                break
            say(f"{entry['version']} has no `lastdb service install`; skipped as the previous release")
        bootstrap = base is None
        require(can_install_service(under_dir), "the build under test has no `lastdb service install`")
        if bootstrap:
            base, base_dir = under, under_dir
        host.prepare()

        say(f"1. install {base['version']} as a service" + (" (bootstrap: the build under test)" if bootstrap else ""))
        done = host.lastdb(base_dir, "service", "install", "--json")
        require(done.returncode == 0, f"service install exited {done.returncode}")
        host.set_soak(10)
        require(host.link("current") == f"programs/{base['version']}", f"current is {host.link('current')}")
        note = f"proof-{os.urandom(6).hex()}"
        seed_and_write(host.sock, note)
        require(read_record(host.sock)["note"] == note, "the known record does not read back")
        require(running_build(host.sock) == base["version"], "the installed build is not running")
        results.append(f"1 install {base['version']}: ok")

        say("2. lastdb upgrade to the build under test" if not bootstrap else "2. lastdb upgrade finds nothing newer (bootstrap)")
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

        good = host.link("current")
        before_previous = host.link("previous")
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
