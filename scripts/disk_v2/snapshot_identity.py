"""Prepare inactive foreign snapshots through the shipped stopped-home CLI."""

import json
import os
from pathlib import Path
import subprocess
import uuid

from identity_snapshot_files import copy_account, materialize, mode_snapshot, owned, tree_digest
from logical_capture import Node, file_hash, require, save, write
from logical_peer_support import add_model, author, check_values, export, ingest, item, own_head, request
from logical_snapshot import snapshot
from snapshot_adoption import candidate, release

CONTROL = "data/logical-device-capture-v1"


def environment(node):
    values = {name: value for name, value in os.environ.items()
              if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))}
    values.update(node.environment)
    values.update(LASTDB_HOME=str(node.home), FOLDDB_HOME=str(node.home), LASTDB_ISOLATED_COPY="1",
                  LASTDB_SUPPRESS_RECOVERY_PHRASE="1")
    return values


def private_json(path, value):
    content = json.dumps(value, separators=(",", ":")).encode()
    require(len(content) <= 40 * 1024, "candidate exceeds its CLI metadata bound")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def command(cli, source, target, value, label, action, success=True):
    owned(source)
    owned(target)
    path = source.root / f"candidate-{label}.json"
    if not path.exists():
        private_json(path, value)
    scratch = source.root / f"scratch-{label}-{action}-{uuid.uuid4()}"
    scratch.mkdir(mode=0o700)
    args = [str(cli), "--data-dir", str(source.home), "db", f"snapshot-identity-{action}",
            "--into", str(target.home), "--candidate-file", str(path), "--scratch", str(scratch), "--json"]
    completed = subprocess.run(args, env=environment(source), stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=90, check=False)
    require(len(completed.stdout) <= 128 * 1024 and len(completed.stderr) <= 128 * 1024,
            "identity CLI exceeded its output bound")
    if not success:
        require(completed.returncode != 0, "unsupported identity transition was accepted")
        return {"refused": True, "exit_code": completed.returncode}
    require(completed.returncode == 0,
            f"identity {action} failed: {completed.stderr.decode(errors='replace')[:400]}")
    return json.loads(completed.stdout)


def verify(report, source, value, original, head, donor):
    require(report["version"] == 1 and report["prepared"] is True
            and report["startup_allowed"] is False and report["replacement_allowed"] is False,
            "prepared metadata granted startup or replacement")
    require(report["local_origin"] == original["origin"] and report["local_origin"] != donor["origin"]
            and report["anchor_sha256"] == original["sha256"]
            and report["authored_base"] == {key: str(head[key]) if key == "sequence" else head[key]
                                           for key in ("sequence", "sha256")}
            and report["candidate_root"] == value["native_root"]
            and report["source_root"]["home_id"] != report["prepared_root"]["home_id"]
            and report["prepared_root"]["home_id"] == value["native_root"]["home_id"],
            "source identity or original anchor changed with the physical home")
    require(report["source_root"]["current_sha256"] == file_hash(source.home / "data/disk-run/CURRENT"),
            "source report differs from the actual stopped selector")
    included = {entry["origin"]: entry for entry in value["logical_inclusion"]["map"]["positions"]}
    peers = report["peers"]
    require(len(peers) == len(included) - 1 and len(peers) <= 32,
            "prepared peer roster has an unexpected count")
    for peer in peers:
        position = {key: included[peer["origin"]][key] for key in ("sequence", "sha256")}
        require(peer["anchor_sha256"] == included[peer["origin"]]["anchor_sha256"]
                and peer["base"] == position and peer["position"] == position
                and peer["retained_receipts"] == "0", "prepared receipt base is not the certified covered position")
    require(any(peer["origin"] == donor["origin"] for peer in peers), "donor did not become a peer")
    coverage = report["logical_inclusion"]
    require(coverage["state"] == "enabled" and coverage["map"]["version"] == 2
            and coverage["map"]["producer"] == original["origin"]
            and coverage["map"]["snapshot"] == report["prepared_root"], "prepared map lost its stable producer")


def startup_refusal(target, enabled):
    target.environment["LASTDB_LOGICAL_CAPTURE"] = "1" if enabled else "0"
    target.start_seconds = 10
    try:
        target.start()
    except RuntimeError:
        process = target.process
        require(process is not None and process.poll() is not None and process.returncode != 0,
                "prepared target did not refuse startup promptly")
        return {"refused": True, "capture_enabled": enabled, "exit_code": process.returncode}
    else:
        raise RuntimeError("prepared target started without source retirement or activation")
    finally:
        if target.process is not None:
            if target.process.poll() is None:
                target.process.kill()
                target.process.wait(timeout=10)
            target.process = None
            target.log.close()
            target.log = None


class Operation:
    def __init__(self, program, output, settings):
        self.program, self.output, self.settings = program, output, settings
        self.cli = program[0].with_name("lastdb")
        require(self.cli.is_file(), "the real identity CLI is missing beside the candidate daemon")
        self.nodes, self.model = [], {}
        self.a, self.b = self.node("donor"), self.node("source")
        self.result = {"complete": False, "primary_access": False, "cloud_access": False,
                       "activation_executed": False, "source_revision": program[1],
                       "daemon_sha256": file_hash(program[0]), "cli_sha256": file_hash(self.cli),
                       "prepare_interruption_claimed": False}

    def node(self, name):
        folder = self.output / name
        folder.mkdir(mode=0o700)
        node = Node(self.program[0], self.program[1], folder, environment=dict(self.settings))
        self.nodes.append(node)
        return node

    def bootstrap(self):
        self.a.start()
        self.a.stop()
        self.result["account_injection"] = copy_account(self.a, self.b)
        self.a.start()
        self.b.start()
        self.pages = [export(self.a), export(self.b)]
        self.anchors = [page["anchor"] for page in self.pages]
        require(self.anchors[0]["origin"] != self.anchors[1]["origin"], "generated devices share a signer")
        for receiver, page in [(self.a, self.pages[1]), (self.b, self.pages[0])]:
            request(receiver, {"action": "enroll", "anchor": page["anchor"]})

    def capture(self):
        identity = str(uuid.uuid4())
        value = snapshot(self.a, "prepare", identity)
        self.a.stop()
        return value

    def detached(self, name, capture):
        target = self.node(name)
        copy_account(self.a, target)
        transport = materialize(self.a, target, capture)
        self.a.start()
        release(self.a, capture["request_id"])
        return target, transport

    def prepare(self, name, capture, head, allowed):
        result = {"name": name, "stage": "stop_source"}
        self.result.setdefault("attempts", []).append(result)
        self.b.stop()
        result["stage"] = "materialize"
        target, transport = self.detached(name, capture)
        value = candidate(capture)
        before = tree_digest(self.b)
        result.update(transport=transport, source_before=before, stage="prepare")
        result["directory_modes"] = {"source": mode_snapshot(self.b), "target": mode_snapshot(target)}
        result["prepare"] = command(self.cli, self.b, target, value, name, "prepare", allowed)
        require(tree_digest(self.b) == before, "prepare changed the stopped source")
        if not allowed:
            require(not (target.home / CONTROL).exists(), "refused preparation installed a signer")
            result["stage"] = "refused"
            return result
        report = result["prepare"]
        result["stage"] = "verify_report"
        verify(report, self.b, value, self.anchors[1], head, self.anchors[0])
        control = file_hash(target.home / CONTROL)
        require(control != file_hash(self.a.home / CONTROL), "prepared target contains donor private control")
        result["stage"] = "repeat_inspect"
        for _ in range(2):
            require(command(self.cli, self.b, target, value, name, "inspect") == report,
                    "prepared inspection changed its durable metadata")
        result["stage"] = "startup_refusal"
        result["startup_refusals"] = [startup_refusal(target, flag) for flag in (True, False)]
        result["stage"] = "inspect_after_startup_refusal"
        require(command(self.cli, self.b, target, value, name, "inspect") == report
                and file_hash(target.home / CONTROL) == control and tree_digest(self.b) == before,
                "refused startup or repeat inspection changed source identity or prepared state")
        result["source_unchanged"] = True
        result["target_never_active"] = True
        result["stage"] = "complete"
        return result

    def seed(self):
        batches = [author(self.a, "identity-a"), author(self.b, "identity-b")]
        pages = [export(self.a), export(self.b)]
        for batch, page in zip(batches, pages):
            add_model(self.model, batch, page["events"])
        ingest(self.a, pages[1]["events"])
        ingest(self.b, pages[0]["events"])
        self.result["shared_values"] = [check_values(node, self.model) for node in (self.a, self.b)]

    def advance(self, source, receiver, label):
        previous = own_head(source)["sequence"]
        batch = [item(label, 0)]
        write(source, batch)
        page = export(source, previous, 1)
        require(len(page["events"]) == 1, "identity case lacks one original next event")
        add_model(self.model, [batch], page["events"])
        if receiver is not None:
            ingest(receiver, page["events"])
        return page

    def execute(self):
        self.bootstrap()
        self.result["stage"] = "zero_head"
        self.result["zero_head"] = self.prepare("zero-target", self.capture(), own_head(self.b), True)
        self.b.start()
        self.seed()
        self.result["stage"] = "peer_coverage_refusal"
        old = self.capture()
        self.a.start()
        self.advance(self.a, self.b, "identity-a-next")
        self.a.stop()
        self.result["peer_coverage_refusal"] = self.prepare("peer-lag-target", old, own_head(self.b), False)
        self.b.start()
        self.result["stage"] = "own_tail_refusal"
        older = self.capture()
        event = self.advance(self.b, None, "identity-b-next")
        self.result["own_tail_refusal"] = self.prepare("own-lag-target", older, own_head(self.b), False)
        self.b.start()
        ingest(self.a, event["events"])
        self.result["converged_values"] = [check_values(node, self.model) for node in (self.a, self.b)]
        self.result["stage"] = "nonzero_prepare"
        self.result["nonzero"] = self.prepare("prepared-target", self.capture(), own_head(self.b), True)
        self.b.start()
        self.result["stage"] = "peer_ahead_prepare"
        before = request(self.b, {"action": "position", "origin": self.anchors[0]["origin"]})["position"]
        self.advance(self.a, None, "identity-a-ahead")
        self.result["peer_ahead_source_position"] = before
        self.result["peer_ahead"] = self.prepare("prefix-target", self.capture(), own_head(self.b), True)
        donor_peer = next(peer for peer in self.result["peer_ahead"]["prepare"]["peers"]
                          if peer["origin"] == self.anchors[0]["origin"])
        require(before["sequence"] == 4 and donor_peer["base"]["sequence"] == "5",
                "candidate-ahead preparation did not exercise the retained native prefix")
        self.result["complete"] = True
        self.result["stage"] = "complete"

    def close(self):
        self.result["owned_cleanup"] = []
        for node in reversed(self.nodes):
            try:
                node.close()
                self.result["owned_cleanup"].append(json.loads((node.output / "cleanup.json").read_text()))
            except Exception as error:
                self.result.setdefault("cleanup_errors", []).append(str(error)[:600])
                self.result["complete"] = False


def run(program, output, settings):
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("RUNNER_ENVIRONMENT") == "github-hosted",
            "identity preparation operates only on hosted CI")
    output.mkdir(mode=0o700)
    operation = Operation(program, output, settings)
    try:
        operation.execute()
    except Exception as error:
        operation.result["error"] = f"{type(error).__name__}: {error}"[:1200]
    finally:
        operation.close()
        save(output / "operation.json", operation.result)
    require(operation.result["complete"], "inactive snapshot identity preparation did not complete")
    return operation.result
