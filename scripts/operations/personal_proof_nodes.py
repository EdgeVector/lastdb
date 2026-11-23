"""Real nodes, the reference relay and record helpers for the personal database proof.

`Relay` runs the reference relay on a fixed port so it can stop and come back at one address, and
reads its request log. `Device` is one real `lastdbd` process with a home of its own, and reads the
spool of accepted writes, the confirmed upload position and the peak memory of its daemon.
"""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from benchmarks.node import Node  # noqa: E402
from record_runs import held, require  # noqa: E402
import v1_records as v1  # noqa: E402

WAIT_SECONDS = 150
SCALE_WAIT_SECONDS = 1800
ALPHA, BETA = "proofmarkeralpha", "proofmarkerbeta"
SCHEMAS = (ALPHA, BETA)
FIELDS = ["id", "note"]
MARK = "pdp"
GAMMA = "proofmarkergamma"
DELTA = "proofmarkerdelta"
EPSILON = "proofmarkerepsilon"
GROUPS = ("grp-x", "grp-y", "grp-f")
GAMMA_KEYS = ("g1", "g2", "g3", "f1")
FILE_MARK, NAME_MARK = "FILEMARK-9f3c2a71d4", "NAMEMARK-5be10c88a2"
# A sealed blob is the file inside an AES-GCM envelope: version byte, nonce and tag.
ENVELOPE_OVERHEAD = 29
MEBIBYTE = 1024 * 1024
# The peak resident memory may grow by this much between a quarter of the entries and all of
# them, and may never pass the absolute bound. Memory that grew with the entries would not fit.
MEMORY_GROWTH_BYTES = 256 * MEBIBYTE
MEMORY_BOUND_BYTES = 1536 * MEBIBYTE
BATCH = 200
# The spool keeps one small id record for each of the newest writes (IDS_KEPT in
# fold_db/.../accepted/disk/recover/files.rs) and trims every IDS_TRIM_EVERY acknowledged entries
# (TRIM_EVERY in recover.rs), so the directory never holds much more than the sum.
IDS_KEPT, IDS_TRIM_EVERY = 10_000, 2_500
IDS_BOUND = IDS_KEPT + IDS_TRIM_EVERY + 500



def wait(what, probe, seconds=WAIT_SECONDS):
    """Poll probe() until it returns a truthy value. Name `what` and the last value on timeout."""
    deadline, last = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        last = probe()
        if last:
            return last
        time.sleep(1)
    raise RuntimeError(f"timed out after {seconds}s waiting for {what}; last value {last!r}")


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class DeployedRelay:
    """A relay this script does not run: the deployed Exemem DEV relay. Only its origin is known.

    It cannot be stopped, and its stored bytes and request log cannot be read. A step that needs
    either is not run against it. `OFFLINE_URL` is where a device points to be cut off from it.
    """

    deployed = True
    OFFLINE_URL = "http://127.0.0.1:1"

    def __init__(self, url):
        self.url = url.rstrip("/")


class Relay:
    """The reference relay on a fixed port, so it can stop and come back at one address."""

    deployed = False

    def __init__(self, binary, root, max_blob_bytes=None):
        self.binary, self.data, self.port = binary, root / "relay-data", free_port()
        self.max_blob_bytes = max_blob_bytes
        self.stderr = root / "relay.stderr"
        self.url = f"http://127.0.0.1:{self.port}"
        self.process = None

    def start(self):
        require(self.process is None, "the relay is already running")
        log = self.stderr.open("a")
        self.process = subprocess.Popen(
            [str(self.binary), "--listen", f"127.0.0.1:{self.port}", "--data-dir", str(self.data),
             *(["--max-blob-bytes", str(self.max_blob_bytes)] if self.max_blob_bytes else [])],
            stdout=subprocess.PIPE, stderr=log, text=True)
        require(self.process.stdout.readline().strip(), "the relay printed no address")

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            self.process.wait(timeout=30)
            self.process = None

    def lines(self):
        """The request log lines so far: `relay route=log/read status=200 entries=3`."""
        try:
            return self.stderr.read_text().splitlines()
        except OSError:
            return []

    def requests_since(self, mark, route):
        """The request lines of one route after the first `mark` lines, as (status, entries)."""
        found = []
        for line in self.lines()[mark:]:
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            if fields.get("route") == route:
                found.append((fields.get("status"), int(fields.get("entries", "-1"))))
        return found

    def stream_entries(self, database, device_id):
        """The entries the relay holds of one device stream: logs/<db>/<node>/<device>/<seq>.json."""
        return sorted(int(path.stem) for path in (self.data / "logs").glob(f"{database}/*/{device_id}/*.json"))


class Device:
    """One real node. `home` is made by `backup init` or by `backup restore`, then the daemon starts on it."""

    def __init__(self, name, args, out, relay):
        self.name, self.args, self.relay = name, args, relay
        self.out = out / name
        self.out.mkdir(parents=True)
        self.node = Node(Path(args.daemon), args.revision, self.out,
                         environment={"LASTDB_RELAY_URL": relay.url})
        self.cli = Path(args.cli)
        self.node.home.rmdir()
        self.database = None

    def cli_env(self):
        return {k: v for k, v in os.environ.items() if not k.startswith("LASTDB")}

    def run(self, arguments, text=None, env=None):
        result = subprocess.run([str(self.cli), *arguments], input=text, capture_output=True, text=True,
                                timeout=1800, check=False, env={**self.cli_env(), **(env or {})})
        require(result.returncode == 0, f"{self.name}: lastdb {' '.join(arguments[:3])} failed: {result.stderr[-1500:]}")
        return json.loads(result.stdout[result.stdout.index("{"):])

    def init(self):
        """A fresh home with a new identity. Returns the recovery phrase."""
        home = self.node.home
        self.run(["admin", "backup", "init", "--into", str(home), "--recovery-phrase-out",
                  str(home / "recovery-phrase.txt"), "--env", "dev", "--json"])
        return (home / "recovery-phrase.txt").read_text().strip()

    def restore(self, phrase):
        """A fresh home from the recovery phrase and the relay alone."""
        report = self.run(self.restore_arguments(), text=phrase)
        require(report.get("ok") is True and len(report.get("database_id", "")) == 64, f"{self.name}: restore reported {report!r}")
        return report

    def restore_arguments(self):
        return ["backup", "restore", "--recovery-phrase-stdin", "--relay-url", self.relay.url,
                "--into", str(self.node.home), "--json"]

    def restore_killed(self, phrase, delay):
        """Start a restore, kill -9 it after `delay` seconds. True when it had already finished."""
        process = subprocess.Popen([str(self.cli), *self.restore_arguments()], stdin=subprocess.PIPE,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True,
                                   env=self.cli_env())
        process.stdin.write(phrase)
        process.stdin.close()
        time.sleep(delay)
        finished = process.poll() is not None
        if not finished:
            process.kill()
        process.wait(timeout=60)
        return finished

    def snapshot_now(self):
        """Ask the running daemon for a snapshot and wait until its entry is on the relay. Returns the report."""
        return self.run(["backup", "snapshot", "--wait"],
                        env={"LASTDB_SOCKET_PATH": str(self.node.socket), "LASTDB_HOME": str(self.node.home)})

    def start(self):
        self.node.start()
        registry = json.loads((self.node.home / "databases" / "registry.json").read_text())
        self.database = registry["owner"]["id"]

    def stop(self):
        self.node.stop()

    def point_at(self, url):
        """Restart the daemon with another relay address. Its home and its spool stay as they are."""
        self.stop()
        self.node.environment["LASTDB_RELAY_URL"] = url
        self.start()

    def crash(self):
        """Kill -9 the daemon. It gets no chance to flush or to clean up."""
        process = self.node.process
        require(process is not None, f"{self.name} is not running")
        process.send_signal(signal.SIGKILL)
        process.wait(timeout=30)
        self.node.process = None
        self.node.log.close()
        self.node.log = None

    def log_dir(self):
        return self.node.home / "log-sync" / str(self.database)

    def spool_dir(self):
        return self.node.home / "data" / "disk-run" / "accepted-v1"

    def spool_state(self):
        """The spool's own state file: `head` is the last accepted write, `acked` the last the relay confirmed."""
        try:
            return json.loads((self.spool_dir() / "state.json").read_text())
        except (OSError, ValueError):
            return None

    def checkpoint(self):
        """The confirmed upload position: the last sequence number of this device's stream."""
        try:
            return json.loads((self.spool_dir() / "cloud-upload.json").read_text())
        except (OSError, ValueError):
            return None

    def id_records(self):
        """How many id records the spool holds: one small file for each recent accepted write."""
        try:
            return sum(1 for _ in (self.spool_dir() / "ids").iterdir())
        except OSError:
            return 0

    def spool_bytes(self):
        """Apparent size of every file under the spool directory, and of its pending entries alone."""
        total = pending = 0
        for path in self.spool_dir().rglob("*"):
            if path.is_file():
                total += path.stat().st_size
                if "entries" in path.relative_to(self.spool_dir()).parts:
                    pending += path.stat().st_size
        return total, pending

    def synced_up(self):
        """Every write the spool holds is on the relay, and nothing is saved for a retry."""
        state = self.spool_state()
        return (state is not None and state["head"] > 0 and state["head"] == state["acked"]
                and not (self.log_dir() / "outgoing.json").exists())

    def peak_memory_bytes(self):
        """The peak resident memory of the daemon process since it started (VmHWM)."""
        status = Path(f"/proc/{self.node.process.pid}/status")
        for line in status.read_text().splitlines():
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
        raise RuntimeError(f"{self.name}: no VmHWM in {status}")

    def sync_status(self):
        """The cloud sync section of /api/status: the last error of each database's upload, or None."""
        try:
            return self.node.call("/api/status")[0]["status"]["cloud_sync"]
        except Exception as error:  # noqa: BLE001 the node may be down: the evidence is then the error
            return f"unavailable: {error}"

    def entries(self):
        return [f"  spool {self.spool_state()} checkpoint {self.checkpoint()}",
                f"  sync status {self.sync_status()}"]

    def warnings(self, all_lines=False):
        try:
            log = (self.out / "node.log").read_text().splitlines()
        except OSError:
            return []
        found = [line for line in log if "WARN" in line or "ERROR" in line]
        return found if all_lines else found[-25:]

    def device_id(self):
        return (self.node.home / "log-sync" / "device-id").read_text().strip()

    def write(self, mutations):
        v1.write_items(self.node, mutations)

    def change(self, mutations):
        v1.commit(self.node, mutations)

    def keys(self, schema):
        """Every key of one schema, from its key listing."""
        keys, cursor = [], None
        while True:
            page = v1.read_items(self.node, [v1.key_list(schema, limit=200, cursor=cursor)])[0]
            keys.extend(item["hash"] for item in page["keys"])
            cursor = page.get("next_cursor")
            if cursor is None:
                return keys

    def records(self, schema):
        """Every live record of one schema: key -> fields. Raises while the schema is missing."""
        keys = self.keys(schema)
        rows = v1.flat_records(v1.read_all(self.node, [v1.point(schema, {"hash": key}) for key in keys]))
        return {row["key"]["hash"]: held(row["record"]) for row in rows}

    def snapshot(self):
        """Every live record of both schemas: (schema, key) -> fields. None while a schema is missing."""
        found = {}
        for schema in SCHEMAS:
            try:
                records = self.records(schema)
            except RuntimeError:
                return None
            found.update({(schema, key): fields for key, fields in records.items()})
        return found

    def has_schema(self, schema):
        try:
            self.keys(schema)
            return True
        except RuntimeError:
            return False


def note(schema, key, text):
    return {"id": key, "note": f"{MARK}-value-{schema[-5:]}-{key}-{text}"}


def create(schema, key, text):
    return v1.create_item(schema, {"hash": f"{MARK}-key-{key}"}, note(schema, f"{MARK}-key-{key}", text))


def replace(schema, key, text):
    name = f"{MARK}-key-{key}"
    return {"schema": schema, "op": "replace", "key": {"hash": name}, "record": note(schema, name, text)}


def delete(schema, key):
    return {"schema": schema, "op": "delete", "key": {"hash": f"{MARK}-key-{key}"}}


def expect(state, schema, key, text):
    name = f"{MARK}-key-{key}"
    state[(schema, name)] = note(schema, name, text)


def converged(devices, state):
    """Every device holds exactly `state`."""
    return all(device.snapshot() == state for device in devices)


def relay_files(relay):
    """Every file the relay stored, at most six levels under its data directory: logs/<db>/<node>/<device>/<seq>.json."""
    found, level = [], [relay.data]
    for _ in range(6):
        entries = [child for directory in level for child in directory.iterdir()]
        found += [path for path in entries if path.is_file() and path.name != "presign.secret"]
        level = [path for path in entries if path.is_dir()]
    return found


def relay_holds_none(relay, secrets):
    """No stored relay byte holds any of `secrets`."""
    for path in relay_files(relay):
        data = path.read_bytes()
        for secret in secrets:
            require(secret.encode() not in data, f"the relay file {path.name} holds {secret!r}")


def prove_hidden(relay, state, signed):
    """No record key, no record value and no schema name is in any byte the relay stored."""
    files = relay_files(relay)
    secrets = [ALPHA, BETA, f"{MARK}-key-", f"{MARK}-value-"]
    for (_schema, key), fields in state.items():
        secrets += [key, fields["note"]]
    kinds, sealed = dict.fromkeys(("birth", "schema", "data", "bundle"), 0), 0
    for path in files:
        data = path.read_bytes()
        for secret in secrets:
            require(secret.encode() not in data, f"the relay file {path.name} holds {secret!r}")
        sealed += data.count(b'"namespace":"sealed"')
        for kind in kinds:
            kinds[kind] += data.count(f'"kind":"{kind}"'.encode())
    # The search must have read real entries, or it proves nothing.
    require(sealed > 0, "the relay holds no sealed entry: the search read nothing")
    # Every write is one data entry, schema changes included. Only the birth has another kind.
    require(kinds["birth"] == 1 and kinds["data"] >= signed and kinds["schema"] == 0 and kinds["bundle"] == 0,
            f"the relay holds an unexpected set of entries: {kinds!r}")
    return {"relay_files": len(files), "entries": kinds, "sealed_entries": sealed}
