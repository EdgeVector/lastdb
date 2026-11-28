"""The node side of the updater proof: a fresh synthetic home, the service manager, the owner
socket, and the broken candidate. Used by node_updater_proof.py."""
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

LABEL = "com.edgevector.lastdbd"
SCHEMA = "updaterproof"
KEY = "known-record"


class Fail(Exception):
    pass


def require(ok, message):
    if not ok:
        raise Fail(message)


def say(text):
    print(f"proof: {text}", flush=True)

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



# ---------------------------------------------------------------- the node

class Unix(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=60)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(60)
        self.sock.connect(str(self.path))


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
        # Exit 5 is "busy": another upgrade holds the lock, e.g. the update check that
        # `service install` enables. It changes nothing, so the proof waits and asks again.
        for attempt in range(19):
            done = subprocess.run([str(folder / "lastdb"), *args], env=self.env, capture_output=True, text=True,
                                  timeout=timeout, check=False)
            say(f"lastdb {' '.join(args)} -> exit {done.returncode}: {done.stdout.strip()[:300]}")
            for line in done.stderr.strip().splitlines()[-12:]:
                print(f"    {line}")
            if not (args and args[0] == "upgrade" and done.returncode == 5) or attempt == 18:
                return done
            time.sleep(10)

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

    def set_channel(self, channel):
        """`service install` writes the default channel (stable); the proof follows the list under test."""
        config = self.home / "upgrade" / "config.toml"
        text = config.read_text()
        text, changed = re.subn(r'^channel = "[a-z]+"', f'channel = "{channel}"', text, flags=re.M)
        require(changed == 1, "config.toml has no channel line")
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
