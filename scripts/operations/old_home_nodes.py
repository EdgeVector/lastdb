"""Homes written by the old build (d06054159) for the proof that an existing home moves.

`OldNode` runs the old `lastdbd` on a home of its own with no relay and no cloud. `make_old_home`
gives it a known identity, declares two schemas (one with an alternate key), and writes records
with replaces, deletes and one record with a file. It returns what the home must hold, so the
proof can compare the current build against what was written, not against itself.

The variant home saves cloud credentials that point nowhere before the first start. The old build
then captures every write into its spool of accepted writes and its optimistic replay markers, and
its upload cannot succeed: the home is left with unacknowledged spool entries, one id record for
each write, and pending markers in the store. That is the shape of a home that was connected and
had writes the cloud never acknowledged.
"""
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from benchmarks.node import Node  # noqa: E402
from harness import poll_until  # noqa: E402
from personal_proof_nodes import BATCH, FIELDS  # noqa: E402
from record_runs import RANGED, definition, held, require, system_seed  # noqa: E402
import v1_records as v1  # noqa: E402

ALPHA, BETA = "oldhomealpha", "oldhomebeta"
GROUPS = ("old-x", "old-y", "old-z")
FILE_MARK, NOTE_MARK = "OLDFILEMARK-3c71e0aa95", "OLDNOTEMARK-d41f08"
UNREACHABLE = {"api_url": "http://127.0.0.1:1", "api_key": "x"}


class OldNode(Node):
    """The old build's daemon. Same process handling as `Node`, without its benchmark version check."""

    def start(self):
        require(self.process is None, "the old daemon is already running")
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen(
            [str(self.binary), "--data-dir", str(self.home), "--socket-path", str(self.socket), *self.arguments],
            env=self.daemon_env(), stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)

        def ready():
            if self.process.poll() is not None:
                raise RuntimeError("the old daemon exited before health; inspect its node.log")
            if not self.socket.exists():
                return False
            try:
                health, _, _ = self.call("/health")
            except (OSError, http.client.HTTPException):
                return False
            return health.get("ok") is True or health.get("status") == "ok"

        poll_until(self.start_seconds, ready, interval=0.05, timeout_error="the old daemon missed its health deadline")


def recorded(key, text):
    return {"id": key, "note": f"{NOTE_MARK}-{key}-{text}"}


def fresh_identity(cli, scratch):
    """A recovery phrase and the identity.key it belongs to, made by the current CLI."""
    result = subprocess.run(
        [str(cli), "admin", "backup", "init", "--into", str(scratch), "--recovery-phrase-out",
         str(scratch / "recovery-phrase.txt"), "--env", "dev", "--json"],
        capture_output=True, text=True, check=False, env={k: v for k, v in os.environ.items() if not k.startswith("LASTDB")})
    require(result.returncode == 0, f"backup init failed: {result.stderr[-500:]}")
    return (scratch / "recovery-phrase.txt").read_text().strip(), (scratch / "identity.key").read_bytes()


def write_alpha(node, records):
    """Alpha: creates in batches, a replace of every 7th record, a delete of every 11th. Returns the state."""
    state = {}
    keys = [f"a{n:05d}" for n in range(records)]
    for start in range(0, records, BATCH):
        part = keys[start:start + BATCH]
        v1.write_items(node, [v1.create_item(ALPHA, {"hash": key}, recorded(key, "one")) for key in part])
        state.update({key: recorded(key, "one") for key in part})
    replaced = keys[::7]
    for start in range(0, len(replaced), BATCH):
        part = replaced[start:start + BATCH]
        v1.commit(node, [{"schema": ALPHA, "op": "replace", "key": {"hash": key}, "record": recorded(key, "two")}
                         for key in part])
        state.update({key: recorded(key, "two") for key in part})
    deleted = keys[::11]
    for start in range(0, len(deleted), BATCH):
        v1.commit(node, [{"schema": ALPHA, "op": "delete", "key": {"hash": key}} for key in deleted[start:start + BATCH]])
    for key in deleted:
        state.pop(key, None)
    return state


def write_fat(node, count, size=55_000):
    """Alpha records of about 55 KB of incompressible text: the files of the store get large, so a
    snapshot takes long to upload. Returns the state of those records."""
    state = {}
    for start in range(0, count, 16):
        items = []
        for n in range(start, min(start + 16, count)):
            key = f"fat{n:05d}"
            record = {"id": key, "note": f"{NOTE_MARK}-{key}-" + base64.b64encode(os.urandom(size * 3 // 4)).decode()}
            state[key] = record
            items.append(v1.create_item(ALPHA, {"hash": key}, record))
        v1.write_items(node, items)
    return state


def beta_item(key, group, text, file=None):
    return v1.create_item(BETA, {"hash": key}, recorded(key, text), alternates={"by_group": {"hash": group, "range": key}},
                          file=file)


def write_beta(node, source, records, file_bytes=150_000):
    """Beta: records in three groups, moves between groups, a delete and one record with a file.

    Returns (state, groups, file) where `groups` maps a group to its sorted keys and `file` is
    what the record `bf` must read back.
    """
    content = FILE_MARK.encode() + os.urandom(file_bytes)
    path = source / "old-attachment.bin"
    path.write_bytes(content)
    keys = [f"b{n:05d}" for n in range(records)]
    placed = {key: GROUPS[n % 3] for n, key in enumerate(keys)}
    v1.write_items(node, [beta_item(key, placed[key], "one") for key in keys])
    attached = {"path": str(path), "name": "old-attachment.bin", "media_type": "application/x-old"}
    v1.write_items(node, [beta_item("bf", GROUPS[0], "one", file=attached)])
    placed["bf"] = GROUPS[0]
    state = {key: recorded(key, "one") for key in placed}
    for key in keys[::5]:
        alternate = {"by_group": {"hash": GROUPS[1], "range": key}}
        v1.commit(node, [{"schema": BETA, "op": "replace", "key": {"hash": key}, "record": recorded(key, "moved"),
                          "alternates": alternate}])
        placed[key], state[key] = GROUPS[1], recorded(key, "moved")
    for key in keys[3::13]:
        v1.commit(node, [{"schema": BETA, "op": "delete", "key": {"hash": key}}])
        del placed[key], state[key]
    groups = {group: sorted(key for key, held_in in placed.items() if held_in == group) for group in GROUPS}
    return state, groups, {"name": "old-attachment.bin", "media_type": "application/x-old", "size": len(content),
                           "sha256": hashlib.sha256(content).hexdigest()}


def make_old_home(daemon, revision, out, cli, name, alpha, beta, unreachable_cloud=False, file_bytes=150_000,
                  fat_records=0):
    """Run the old daemon on a new home, write, stop cleanly. Returns the node and the expected state."""
    node = OldNode(Path(daemon), revision, out)
    phrase, identity = fresh_identity(cli, node.root / "identity-source")
    identity_path = node.home / "identity.key"
    descriptor = os.open(identity_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(identity)
    if unreachable_cloud:
        (node.home / "cloud-credentials.json").write_text(json.dumps(UNREACHABLE))
        os.chmod(node.home / "cloud-credentials.json", 0o600)
    node.start()
    for schema, alternates in ((ALPHA, None), (BETA, {"by_group": RANGED})):
        system_seed(node, definition(schema, "Hash", FIELDS, alternates, description_prefix="Old home "))
    source = node.root / "source-files"
    source.mkdir(mode=0o700)
    state_alpha = write_alpha(node, alpha)
    state_alpha.update(write_fat(node, fat_records))
    state_beta, groups, file = write_beta(node, source, beta, file_bytes)
    found = {key: held(record) for key, record in read_back(node, ALPHA).items()}
    require(found == state_alpha, f"{name}: the old build does not hold what it was given")
    return node, {"phrase": phrase, "alpha": state_alpha, "beta": state_beta, "groups": groups, "file": file,
                  "marker_file": FILE_MARK}


def read_back(node, schema):
    """Every live record of one schema of a running node: key -> fields."""
    keys, cursor = [], None
    while True:
        page = v1.read_items(node, [v1.key_list(schema, limit=200, cursor=cursor)])[0]
        keys.extend(item["hash"] for item in page["keys"])
        cursor = page.get("next_cursor")
        if cursor is None:
            break
    rows = v1.flat_records(v1.read_all(node, [v1.point(schema, {"hash": key}) for key in keys]))
    return {row["key"]["hash"]: held(row["record"]) for row in rows}


def forget(node):
    """Remove a home and the root that holds it."""
    shutil.rmtree(node.root, ignore_errors=True)
