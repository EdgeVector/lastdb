#!/usr/bin/env python3
"""The personal database on the signed log, across real nodes and the reference relay.

Devices A and B and a restored home C are real `lastdbd` processes. They sync through
one `reference_relay` that this script stops and starts again:

1. A writes records in two schemas: singles, and batches that become bundles.
2. B starts from the recovery phrase alone (`lastdb backup restore`) and holds every record.
3. With the relay stopped, A and B change one record. Afterwards both hold the value of the
   higher position (Lamport number, then device id).
4. A third home restored from the phrase equals A and B, key by key.
5. The bytes the relay stored hold no record key, no record value and no schema name.
6. A restarts with the relay stopped, rebuilds from its files, and signs the next entry on
   the right sequence. The relay accepts it once it is back.

A failure prints each node's entries (kind, sequence, device) and its recent daemon warnings.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from benchmarks.node import Node  # noqa: E402
from record_runs import definition, held, require, system_seed  # noqa: E402
import v1_records as v1  # noqa: E402

WAIT_SECONDS = 150
ALPHA, BETA = "proofmarkeralpha", "proofmarkerbeta"
SCHEMAS = (ALPHA, BETA)
FIELDS = ["id", "note"]
MARK = "pdp"


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


class Relay:
    """The reference relay on a fixed port, so it can stop and come back at one address."""

    def __init__(self, binary, root):
        self.binary, self.data, self.port = binary, root / "relay-data", free_port()
        self.stderr = root / "relay.stderr"
        self.url = f"http://127.0.0.1:{self.port}"
        self.process = None

    def start(self):
        require(self.process is None, "the relay is already running")
        log = self.stderr.open("a")
        self.process = subprocess.Popen(
            [str(self.binary), "--listen", f"127.0.0.1:{self.port}", "--data-dir", str(self.data)],
            stdout=subprocess.PIPE, stderr=log, text=True)
        require(self.process.stdout.readline().strip(), "the relay printed no address")

    def stop(self):
        if self.process is not None:
            self.process.terminate()
            self.process.wait(timeout=30)
            self.process = None


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

    def run(self, arguments, text=None):
        result = subprocess.run([str(self.cli), *arguments], input=text, capture_output=True, text=True,
                                timeout=300, check=False, env=self.cli_env())
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
        report = self.run(["backup", "restore", "--recovery-phrase-stdin", "--relay-url", self.relay.url,
                           "--into", str(self.node.home), "--json"], text=phrase)
        require(report.get("ok") is True and len(report.get("database_id", "")) == 64, f"{self.name}: restore reported {report!r}")
        return report

    def start(self):
        self.node.start()
        registry = json.loads((self.node.home / "databases" / "registry.json").read_text())
        self.database = registry["owner"]["id"]

    def log_dir(self):
        return self.node.home / "log-sync" / str(self.database)

    def authored(self):
        path = self.log_dir() / "authored.json"
        return json.loads(path.read_text()) if path.exists() else []

    def uploaded(self):
        path = self.log_dir() / "uploaded.json"
        return int(path.read_text().strip()) if path.exists() else 0

    def synced_up(self):
        """Every entry this node signed is on the relay."""
        return bool(self.authored()) and self.uploaded() == len(self.authored())

    def entries(self):
        lines = []
        for name in ("authored.json", "received.json"):
            try:
                data = json.loads((self.log_dir() / name).read_text())
            except (OSError, ValueError) as error:
                lines.append(f"  {name}: {error}")
                continue
            for entry in data:
                lines.append("  {} {} seq={} lamport={} device={}".format(
                    name, entry.get("kind"), entry.get("writer_seq"), entry.get("lamport"),
                    str(entry.get("device_id"))[:8]))
        return lines

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

    def snapshot(self):
        """Every live record of both schemas: (schema, key) -> fields. None while a schema is missing."""
        found = {}
        for schema in SCHEMAS:
            keys, cursor = [], None
            while True:
                try:
                    page = v1.read_items(self.node, [v1.key_list(schema, limit=200, cursor=cursor)])[0]
                except RuntimeError:
                    return None
                keys.extend(item["hash"] for item in page["keys"])
                cursor = page.get("next_cursor")
                if cursor is None:
                    break
            rows = v1.flat_records(v1.read_all(self.node, [v1.point(schema, {"hash": key}) for key in keys]))
            for row in rows:
                found[(schema, row["key"]["hash"])] = held(row["record"])
        return found


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
    """Every file the relay stored, at most four levels under its data directory: logs/<db>/<node>/<device>/<seq>.json."""
    found, level = [], [relay.data]
    for _ in range(6):
        entries = [child for directory in level for child in directory.iterdir()]
        found += [path for path in entries if path.is_file() and path.name != "presign.secret"]
        level = [path for path in entries if path.is_dir()]
    return found


def prove_hidden(relay, state):
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
    require(kinds["birth"] == 1 and kinds["schema"] >= 2 and kinds["data"] >= 3 and kinds["bundle"] >= 2,
            f"the relay holds an unexpected set of entries: {kinds!r}")
    require(any(path.parent.name == "blobs" for path in files), "the relay holds no bundle blob")
    return {"relay_files": len(files), "entries": kinds, "sealed_entries": sealed}


def record_entries(device):
    """The data and bundle entries this device signed, in order."""
    return [entry for entry in device.authored() if entry.get("kind") in ("data", "bundle")]


def restart_a(relay, a, b, c, state):
    """6. A restarts without the relay and rebuilds from its files."""
    relay.stop()
    before = a.authored()
    a.node.stop()
    a.node.start()
    require(a.snapshot() == state, "A lost records across its restart")
    a.write([create(BETA, "after-restart", "x")])
    expect(state, BETA, "after-restart", "x")
    after = a.authored()
    require(len(after) == len(before) + 1 and after[-1]["writer_seq"] == before[-1]["writer_seq"] + 1,
            "A did not continue its stream after the restart")
    require("rejected on replay" not in "\n".join(a.warnings()), "A rejected a stored entry on replay")
    relay.start()
    wait("B and C to hold A's write after its restart", lambda: converged((a, b, c), state))
    print("6. A rebuilt from its files with the relay stopped; its next entry reached B and C", flush=True)


def prove(relay, a, b, c, out):
    report = {}
    phrase = a.init()
    a.start()
    # The relay answers 403 to a read of a database whose birth it does not hold. For an owner
    # who has not written anywhere that is an empty relay, not a failed turn.
    time.sleep(6)
    require("personal database pull failed" not in "\n".join(a.warnings(all_lines=True)),
            "A's first turns with an empty relay failed")
    for schema in SCHEMAS:
        system_seed(a.node, definition(schema, "Hash", FIELDS, description_prefix="Personal proof "))
    state = {}

    # 1. Single writes and batches in two schemas.
    a.write([create(ALPHA, "a1", "one")])
    a.write([create(ALPHA, "a2", "one")])
    a.write([create(ALPHA, "b1", "base"), create(ALPHA, "b2", "base"), create(ALPHA, "b3", "base"),
             create(BETA, "c1", "one"), create(BETA, "c2", "one")])
    a.write([create(BETA, "d1", "one")])
    a.change([replace(ALPHA, "a1", "two")])
    a.change([delete(ALPHA, "a2")])
    a.write([create(BETA, "e1", "one"), create(BETA, "e2", "one"), create(BETA, "e3", "one")])
    for key, text in (("a1", "two"), ("b1", "base"), ("b2", "base"), ("b3", "base")):
        expect(state, ALPHA, key, text)
    for key in ("c1", "c2", "d1", "e1", "e2", "e3"):
        expect(state, BETA, key, "one")
    require(a.snapshot() == state, "A does not hold what it wrote")
    kinds = [entry["kind"] for entry in a.authored()]
    require(kinds.count("birth") == 1 and kinds.count("schema") >= 2 and kinds.count("bundle") == 2
            and kinds.count("data") == 5, f"A signed an unexpected set of entries: {kinds!r}")
    wait("A to upload every entry", a.synced_up)
    print("1. A wrote single and batched records in two schemas; one birth, five data, two bundles", flush=True)

    # 2. B starts from the recovery phrase alone.
    restored = b.restore(phrase)
    require(restored["devices"] == 1 and restored["replayed"] >= len(kinds), f"B restore: {restored!r}")
    b.start()
    require(b.database == a.database, "B has another personal database id than A")
    wait("B to hold every record", lambda: b.snapshot() == state)
    report["restore_b"] = restored
    print("2. B restored from the phrase alone and holds every record", flush=True)

    # 3. A and B change one record while the relay is away. The device with the LOWER device id
    # signs one more entry first, so its Lamport number is higher and it must win by that number,
    # not by the device id that breaks a tie.
    relay.stop()
    low, high = (a, b) if a.device_id() < b.device_id() else (b, a)
    low.write([create(ALPHA, f"bump-{low.name}", "x")])
    for device, text in ((a, "from-A"), (b, "from-B")):
        device.change([replace(ALPHA, "b1", text)])
        device.write([create(ALPHA, f"only-{device.name}", device.name)])
    relay.start()
    # Each device signed its replace of b1 and then a create, so the replace is the second last.
    a_replace, b_replace = record_entries(a)[-2], record_entries(b)[-2]
    a_pos, b_pos = (a_replace["lamport"], a_replace["device_id"]), (b_replace["lamport"], b_replace["device_id"])
    low_pos, high_pos = (a_pos, b_pos) if low is a else (b_pos, a_pos)
    require(low_pos[0] > high_pos[0] and low_pos[1] < high_pos[1],
            f"the proof does not separate the Lamport number from the device id: {low_pos} {high_pos}")
    winner = "from-A" if low is a else "from-B"
    expect(state, ALPHA, "b1", winner)
    expect(state, ALPHA, f"bump-{low.name}", "x")
    expect(state, ALPHA, f"only-{a.name}", a.name)
    expect(state, ALPHA, f"only-{b.name}", b.name)
    wait("A and B to converge", lambda: converged((a, b), state))
    report["divergence"] = {"a": list(a_pos), "b": list(b_pos), "winner": winner}
    print(f"3. A {a_pos[0]} and B {b_pos[0]} changed one record apart; both hold {winner}, the higher Lamport number against the higher device id", flush=True)

    # 4. A third home from the phrase equals both.
    wait("A and B to upload", lambda: a.synced_up() and b.synced_up())
    restored = c.restore(phrase)
    require(restored["devices"] == 2, f"C restore: {restored!r}")
    c.start()
    wait("C to equal A and B", lambda: converged((a, b, c), state))
    report["restore_c"] = restored
    print(f"4. C restored from the phrase ({restored['devices']} device streams) and equals A and B, key by key", flush=True)

    # 5. The relay cannot read any of it.
    report["relay"] = prove_hidden(relay, state)
    print(f"5. no key, value or schema name in {report['relay']['relay_files']} relay files", flush=True)

    restart_a(relay, a, b, c, state)
    (out / "summary.json").write_text(json.dumps(report, indent=2))


def dump(devices):
    for device in devices:
        print(f"{device.name} entries:", file=sys.stderr)
        print("\n".join(device.entries()), file=sys.stderr)
        print(f"{device.name} log (last 25 warnings):", file=sys.stderr)
        print("\n".join(device.warnings()), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--daemon", required=True)
    parser.add_argument("--cli", required=True)
    parser.add_argument("--relay-bin", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    relay = Relay(Path(args.relay_bin), args.output)
    relay.start()
    devices = [Device(name, args, args.output, relay) for name in ("device-a", "device-b", "device-c")]
    failure = None
    try:
        prove(relay, *devices, args.output)
    except Exception as error:  # noqa: BLE001 the failure is printed with its evidence, then raised
        failure = error
        print(f"FAILED: {error}", file=sys.stderr)
        dump(devices)
    finally:
        for device in devices:
            try:
                device.node.close()
            except Exception as error:  # noqa: BLE001
                print(f"{device.name}: cleanup failed: {error}", file=sys.stderr)
        relay.stop()
    if failure is not None:
        raise SystemExit(1)
    print("Personal database proof passed", flush=True)


if __name__ == "__main__":
    main()
