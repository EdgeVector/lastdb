#!/usr/bin/env python3
"""Make, upload and restore a new fixed CI baseline.

The baseline is cut once on home format 7, the one-time 6 -> 7 cloud break.
The cloud contract stays fixed after that: a later local format change must
keep the baseline restorable, and a failed restore is a defect of that change.
Use this mode again only for an exception that Tom approves, such as a cloud
contract change (decision-2026-11-01-lastdb-cloud-backup-has-its-own-format).

This manual mode starts a fresh home of the current
format with the fixed CI identity. It writes the 16 known records and one
complete 8 MiB file, and uploads exactly one automatic snapshot to this run's
claimed stream. The source then stops. The snapshot restores to an absent
destination, and two boots read every record and the complete file.

Only then does this step write `ci-baseline.json`. The next workflow step
uploads that archive and asks the service to move the protected baseline. The
service keeps the replaced baseline. The record notes and file bytes derive
from the recovery phrase and the public stream ID (`baseline_data.py`), so the
operator can save the protected baseline secret without any value in the log.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import sys

sys.dont_write_bytecode = True

import baseline_data
from automatic import selection
from automatic_source import Source, completion, durable_batch
from ci_control import Control
from node import OwnedNode
from ownership import run_identity
from recover import Operation
from restore_receipts import restored
from support import ACCOUNT, FILE_BYTES, OperationError, require, save_json

# No mutation log inside this run: the one snapshot stays the stream head.
LOG_INTERVAL = 604800
SNAPSHOT_INTERVAL = 86400
RECEIPT = "lastdb.cloud-ci-baseline.v1"


class BaselineSource(Source):
    def configure(self, snapshot=SNAPSHOT_INTERVAL):
        value = self.cloud("enable-automatic", "--log-interval-seconds", str(LOG_INTERVAL),
                           "--snapshot-interval-seconds", str(snapshot))
        self.policy(value, snapshot)
        return value

    def policy(self, value, snapshot=SNAPSHOT_INTERVAL):
        policy = value["automatic_backup"]
        require(value["user_hash"] == ACCOUNT and value["automatic_backup_enabled"] is True
                and policy["enabled"] is True and policy["paused"] is None
                and policy["log_interval_seconds"] == LOG_INTERVAL
                and policy["snapshot_interval_seconds"] == snapshot, "baseline_automatic_policy")
        require(value["blocked_reason"] is None, "automatic_backup_blocked")
        return policy


def private_write(path, payload):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def point(value):
    return {name: value[name] for name in ("stream_id", "commit_id", "commit_sha256")}


class BaselineOperation(Operation):
    def __init__(self, args):
        super().__init__(args)
        self.summary.update(schema="lastdb.cloud-ci-baseline-operation.v1",
                            coverage="fresh current-format home, one snapshot, and a source-free restore")

    def execute(self):
        credentials = self.credentials()
        control = Control(self.baseline, self.api_key)
        try:
            capabilities = control.context(startup=True)
            require(capabilities.get("baseline_move") is True, "ci_baseline_move_capability")
            self.summary["stage"] = "ci-quota-headroom"
            self.summary["ci_quota"] = control.usage()
        finally:
            control.close()
        phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "").strip().encode()
        require(0 < len(phrase) <= 4096, "recovery_phrase")
        try:
            try:
                identity = baseline_data.seed(phrase)
            except baseline_data.BaselineDataError as error:
                raise OperationError(str(error)) from None
            key = baseline_data.content_key(identity)
            home = self.root / "source"
            home.mkdir(mode=0o700)
            # The daemon reads this seed instead of minting a new identity.
            private_write(home / "identity.key", identity)
            del identity
            self.summary["stage"] = "baseline-source"
            expected, source = self.write_source(home, key)
            self.summary["stage"] = "baseline-snapshot"
            accepted = self.snapshot(source)
            selected = selection(expected, accepted)
            require(selected["stream_id"] == self.ci_stream_id
                    and selected["account_generation"] == self.baseline["account_generation"],
                    "baseline_owned_stream")
            self.summary["stage"] = "baseline-recovery"
            self.summary["baseline_recovery"] = self.recover(credentials, selected, phrase)
        except BaseException:
            # A started upload can leave uncertain local state; keep it private.
            self.preserve = self.node is not None
            raise
        finally:
            del phrase
            self.api_key = None
        new = point(selected)
        save_json(self.args.output / "ci-baseline.json", {
            "schema": RECEIPT, "owner": run_identity(self.args.source), "account": ACCOUNT,
            "account_generation": selected["account_generation"], "previous": point(self.baseline),
            "baseline": new, "source_stopped": True, "restored_to_absent_destination": True,
            "restored_reads_verified": True, "provider_deletion_authority": False})
        secret = baseline_data.secret(key, new["stream_id"], selected["account_generation"],
                                      new["commit_id"], new["commit_sha256"], expected["file_sha256"])
        # Public identifiers only. The commitment lets the operator check the
        # locally derived secret before it replaces the protected baseline.
        self.summary["new_baseline"] = {
            "scope": {"account": ACCOUNT, "account_generation": selected["account_generation"],
                      "stream_id": new["stream_id"]},
            "point": new, "schema": baseline_data.SCHEMA, "records": len(secret["records"]),
            "file_bytes": FILE_BYTES, "secret_sha256": baseline_data.commitment(secret)}
        del secret, key
        self.summary.update(result="passed", restored_boots=2, strict_clean_stops=2)

    def write_source(self, home, key):
        stream = self.ci_stream_id
        payload = baseline_data.file_bytes(key, stream)
        expected = {"account": ACCOUNT, "account_generation": self.baseline["account_generation"],
                    "schema": baseline_data.SCHEMA, "records": baseline_data.records(key, stream),
                    "file_bytes": len(payload), "file_sha256": hashlib.sha256(payload).hexdigest()}
        require(expected["file_bytes"] == FILE_BYTES, "baseline_file_size")
        node = OwnedNode(home, self.programs, backup_source=True, ci_stream_id=stream, terminal_r2=True)
        self.node = node
        node.start()
        # Same order as the normal source: connect, then write, then enable automatic backup.
        source = BaselineSource(node, expected, self.args.source, self.summary)
        source.connect(self.api_key)
        fields = ["id", "note", "file"]
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": baseline_data.SCHEMA,
            "descriptive_name": baseline_data.SCHEMA, "schema_type": "Hash",
            "key": {"hash_field": "id"}, "fields": fields, "field_types": {"id": "String"},
            "field_descriptions": {name: baseline_data.SCHEMA + " " + name for name in fields}}})
        path = self.root / "baseline-file.bin"
        private_write(path, payload)
        del payload
        pointer = self.programs.command(home, ["db", "put-blob-local", "--file", str(path)])
        path.unlink()
        mutations = [{"type": "mutation", "schema": baseline_data.SCHEMA, "mutation_type": "create",
                      "durability": "durable", "key_value": {"hash": record["id"], "range": None},
                      "fields_and_values": {**record, "file": pointer}}
                     for record in expected["records"]]
        durable_batch(node, mutations)
        expected["file_pointer"] = pointer
        self.summary["source_reads"] = node.verify_records(expected)
        return expected, source

    def snapshot(self, source):
        source.configure()
        published = source.published("snapshot")
        # The restore below proves the content. A later native journal entry
        # cannot enter this stream: the next log is seven days away.
        policy = published["automatic_backup"]
        self.summary["baseline_journal_complete"] = completion(published, policy)
        source.disable()
        accepted = published["last_accepted_backup"]
        # Disable stops new requests. The saved head must still be this snapshot.
        after = source.cloud("status")["last_accepted_backup"]
        require(after is not None and after["commit_sha256"] == accepted["commit_sha256"]
                and len(source.completed) == 1, "baseline_single_snapshot_head")
        self.node.stop()
        require(self.node.clean_stops == 1, "baseline_source_clean_stop")
        self.summary["source_stopped"] = True
        return accepted

    def recover(self, credentials, selected, phrase):
        require(self.node.process is None, "source_stays_stopped_for_restore")
        parent = self.root / "baseline-recovery"
        parent.mkdir(mode=0o700)
        destination = parent / "restored"
        require(not os.path.lexists(destination), "absent_restore_destination")
        report = self.programs.restore(credentials, destination, selected, phrase)
        proof = restored(destination, report, selected)
        inventory = json.dumps(proof.pop("files"), sort_keys=True, separators=(",", ":")).encode()
        proof["native_inventory_sha256"] = hashlib.sha256(inventory).hexdigest()
        node = OwnedNode(destination, self.programs)
        self.node = node
        reads = []
        for _number in range(2):
            node.start()
            reads.append(node.verify_records(selected))
            node.stop()
        require(node.clean_stops == 2, "two_baseline_restore_clean_stops")
        return {"restore": proof, "restored_boots": 2, "strict_clean_stops": 2, "keyed_reads": reads}

    def finish(self):
        if not self.output_owned:
            return
        save_json(self.args.output / "summary.json", self.summary)
        destination = os.environ.get("GITHUB_STEP_SUMMARY")
        if destination:
            path = Path(destination)
            runner = Path(os.environ["RUNNER_TEMP"]).resolve()
            require(path.is_absolute() and path.parent.resolve().is_relative_to(runner), "ci_summary_path")
            text = "\n### DEV CI baseline creation\n\n```json\n"
            text += json.dumps(self.summary, sort_keys=True, indent=2) + "\n```\n"
            require(len(text.encode()) <= 65536, "ci_summary_size")
            descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "w") as stream:
                stream.write(text)
        print(json.dumps({"result": self.summary["result"], "stage": self.summary.get("stage"),
                          "failure": self.summary.get("failure"), "source": self.args.source,
                          "new_baseline": self.summary.get("new_baseline"),
                          "primary_access": False}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--startup-proof", required=True, type=Path)
    parser.add_argument("--startup-artifact", required=True, type=Path)
    parser.add_argument("--preflight", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    operation = BaselineOperation(args)

    def interrupted(_signal, _frame):
        raise OperationError("operation_cancelled_or_deadline")
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    # The job limit is 10 minutes; owned cleanup needs the rest.
    signal.alarm(480)
    try:
        operation.prepare()
        operation.execute()
    except OperationError as error:
        operation.summary["failure"] = str(error)
    except BaseException:
        # Product replies can contain file keys or grants. Never print exceptions.
        operation.summary["failure"] = "unexpected_operation_error"
    finally:
        signal.alarm(0)
        try:
            operation.cleanup()
        except BaseException:
            operation.summary.update(result="failed", cleanup="incomplete",
                                     failure="owned_cleanup_failed", stage="cleanup")
        if operation.summary["result"] == "passed":
            operation.summary["stage"] = "complete"
        operation.finish()
    return 0 if operation.summary["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
