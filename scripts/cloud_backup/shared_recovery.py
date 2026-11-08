#!/usr/bin/env python3
"""Publish and restore a generated shared database in protected DEV CI."""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile
import time
import uuid

sys.dont_write_bytecode = True

import baseline_data
import native_records
import shared_abandoned
import shared_conflicts
import shared_automatic
import shared_daemon
import shared_multiwriter
import shared_retention
import shared_reclamation
import shared_snapshot
from automatic_source import durable_batch
from baseline import private_write
from node import OwnedNode
from shared_origin import owner as shared_owner
from programs import Programs, execute
from recover import step_summary
from shared_scope import is_digest, is_schema_alias, protected_json, scope
from support import (CREDENTIALS_FILE, OperationError, canonical_uuid,
                     child_environment, decode_json, dev_endpoint, failure,
                     private_directory, read_json, require, save_json, unexpected)


SCHEMA = "lastdb.cloud-dev-shared-recovery.v1"
FILE_BYTES = 65536
KEYS = ("checkpoint", "winner", "deleted", "peer-only")
IDENTIFIER = "shared_proof_record_identifier"
NOTE = "shared_proof_record_note"


class Operation:
    def __init__(self, args):
        self.args = args
        self.root = None
        self.root_identity = None
        self.nodes = []
        self.output_owned = False
        self.summary = {"schema": SCHEMA, "result": "failed", "stage": "admission",
                        "primary_access": False, "provider_deletions": 0}

    def prepare(self):
        source = self.args.source
        self.owner = shared_owner(source)
        self.proof_owner = self.owner
        require(re.fullmatch(r"[A-Za-z0-9.+_-]{1,128}", self.args.version)
                and self.args.version.endswith(".g" + source[:9]), "shared_source_version")
        runner = Path(os.environ.get("RUNNER_TEMP", ""))
        require(runner.is_absolute() and runner.is_dir(), "shared_runner_temp")
        require(self.args.output == runner / "cloud-shared-recovery", "shared_runner_output")
        self.args.output.mkdir(mode=0o700)
        self.output_owned = True
        self.summary.update(owner=self.owner, source=source, version=self.args.version)

        require(not self.args.retention or (self.args.multiwriter and self.args.prior_summary is None),
                "shared_retention_new_multiwriter_scope")
        require(not self.args.conflicts or (self.args.multiwriter and not self.args.retention
                and self.args.prior_summary is None), "shared_conflict_new_multiwriter_scope")
        require(not self.args.automatic_sync or (self.args.multiwriter and not self.args.retention
                and not self.args.conflicts and self.args.prior_summary is None),
                "shared_automatic_new_multiwriter_scope")
        require(not (self.args.reclamation or self.args.abandoned) or (self.args.multiwriter and not self.args.retention
                and not self.args.conflicts and not self.args.automatic_sync
                and self.args.prior_summary is None), "shared_manual_new_multiwriter_scope")
        writer_count = shared_multiwriter.WRITER_COUNT if self.args.multiwriter else 2
        self.scope = scope(writer_count)
        self.summary["scope"] = self.scope
        self.writers = sorted(self.scope["writers"],
                              key=lambda writer: writer["device_id"] != self.scope["publisher_device"])
        if self.args.prior_summary is None:
            require(self.args.prior_run is None and self.args.prior_source is None, "shared_no_prior_run")
            self.payload_id = str(uuid.uuid4())
            self.summary["payload_id"] = self.payload_id
        else:
            self.admit_prior(runner)

        phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "")
        require(0 < len(phrase.encode()) <= 4096, "shared_recovery_phrase")
        self.phrase = phrase.encode()
        try:
            self.account_seed = baseline_data.seed(self.phrase)
        except baseline_data.BaselineDataError as error:
            raise OperationError(str(error)) from None
        api_key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY", "")
        require(0 < len(api_key.encode()) <= 4096 and api_key.strip() == api_key
                and all(32 <= ord(character) < 127 for character in api_key), "shared_dev_api_key")
        self.credentials = {"api_url": dev_endpoint(), "api_key": api_key}
        seeds = protected_json("LASTDB_CLOUD_DEV_SHARED_DEVICE_SEEDS")
        require(isinstance(seeds, dict) and set(seeds) == {
            writer["device_id"] for writer in self.writers}, "shared_device_seed_scope")
        require(all(isinstance(seed, str) and re.fullmatch(r"[A-Za-z0-9+/]{43}=", seed)
                    for seed in seeds.values()), "shared_device_seed_encoding")
        seeds = {device: base64.b64decode(seed, validate=True) for device, seed in seeds.items()}
        require(len(set(seeds.values()) | {self.account_seed}) == writer_count + 1,
                "shared_independent_device_seeds")

        self.root = Path(tempfile.mkdtemp(prefix="ldb-sr-", dir="/tmp")).resolve()
        info = private_directory(self.root)
        self.root_identity = info.st_dev, info.st_ino, info.st_uid
        for name in ("credentials", "device-keys", "os-home", "tmp"):
            (self.root / name).mkdir(mode=0o700)
        for device, seed in seeds.items():
            private_write(self.device_file(device), seed)
        save_json(self.root / "credentials" / CREDENTIALS_FILE, self.credentials)
        self.bindings = self.root / "schemas.json"
        self.programs = Programs(self.args.bin_dir, self.args.version, child_environment(self.root))
        self.summary["binaries"] = self.programs.identities

    def admit_prior(self, runner):
        require(self.args.prior_summary == runner / "cloud-shared-prior/summary.json"
                and re.fullmatch(r"[1-9][0-9]{0,19}", self.args.prior_run or "")
                and re.fullmatch(r"[0-9a-f]{40}", self.args.prior_source or "")
                and self.args.prior_run != self.owner["run_id"], "shared_prior_input")
        prior = read_json(self.args.prior_summary, 65536)
        self.proof_owner = {**self.owner, "source": self.args.prior_source,
                            "run_id": self.args.prior_run, "run_attempt": "1"}
        require(isinstance(prior, dict) and prior.get("schema") == SCHEMA
                and prior.get("owner") == self.proof_owner and prior.get("source") == self.args.prior_source
                and prior.get("scope") == self.scope and prior.get("primary_access") is False
                and prior.get("provider_deletions") == 0 and prior.get("cleanup") == "owned runner data removed",
                "shared_prior_identity")
        # Each writer stream defines the schema once, before its first record.
        peer_through = 8 if prior.get("peer_restored_before_first_write") is True else 5
        positions = {"checkpoint_through": 5, "peer_applied_publisher_through": peer_through,
                     "publisher_tail_through": 8, "peer_tail_through": 4}
        require(all(type(prior.get(name)) is int and prior[name] == value
                    for name, value in positions.items()), "shared_prior_accepted_positions")
        publication = ("snapshot_published_before_both_tails" if "payload_id" in prior
                       else "snapshot_published_after_both_tails")
        checks = (publication, "peer_read_publisher_records_and_file", "source_homes_removed",
                  "publication_workspace_removed", "wrong_device_refused")
        require(all(prior.get(name) is True for name in checks), "shared_prior_completed_publication")
        binding = prior.get("schema_binding")
        require(isinstance(binding, dict) and set(binding) in (
            {"runtime_name", "portable_id"}, {"runtime_name", "portable_id", "catalog_id"})
            and is_schema_alias(binding["runtime_name"]) and is_digest(binding["portable_id"])
            and ("catalog_id" not in binding or is_digest(binding["catalog_id"])), "shared_prior_schema")
        self.schema = binding["runtime_name"]
        self.snapshot = canonical_uuid(prior.get("snapshot_id"))
        self.payload_id = canonical_uuid(prior.get("payload_id", self.snapshot))
        self.summary.update(snapshot_id=self.snapshot, prior_operation=self.proof_owner,
                            schema_binding=binding, **positions,
                            **{name: True for name in checks})

    def device_file(self, device):
        return self.root / "device-keys" / device

    def command(self, home, args, timeout=120):
        self.programs.verify()
        return execute([str(self.programs.directory / "lastdb"), "--data-dir", str(home),
                        *self.programs.cli_arguments(["cloud", *args])],
                       self.programs.environment, self.phrase, timeout)

    def writer(self, node, writer, *args):
        require(node.process is None, "shared_writer_stopped")
        path = node.home / CREDENTIALS_FILE
        save_json(path, self.credentials)
        try:
            device = writer["device_id"]
            return self.command(node.home, ["shared-writer", "--device-id", device,
                                "--device-key-file", str(self.device_file(device)),
                                "--recovery-phrase-stdin", *args])
        finally:
            path.unlink()

    def initialize_publisher(self, writer):
        node = shared_multiwriter.empty_home(self, "first")
        self.provision_schema(node, writer)
        self.writer(node, writer, "install", "--schema-bindings", str(self.bindings))
        self.bind_writer(node, writer, "--initialize-publisher")
        return node

    def bind_writer(self, node, writer, *options):
        return self.writer(node, writer, "bind", "--database-id", self.scope["database_id"],
                           "--source-stream", writer["source_stream"], "--schema-bindings",
                           str(self.bindings), *options)

    def join_writer(self, name, writer):
        destination = self.root / name
        device = writer["device_id"]
        receipt = decode_json(self.restore(destination, device, device))
        require(receipt == {"activated": True, "destination": str(destination)},
                "shared_join_restore_activation")
        node = OwnedNode(destination, self.programs)
        self.nodes.append(node)
        self.summary["stage"] = "boot-restored-writer-before-binding"
        node.start()
        if name == "peer":
            shared_multiwriter.refuse_unbound_write(self, node)
        node.stop()
        self.summary["stage"] = "bind-restored-writer"
        self.bind_writer(node, writer)
        return node

    def runtime_schema(self):
        database = uuid.UUID(self.scope["database_id"])
        suffix = base64.b32encode(database.bytes).decode().lower().rstrip("=")
        name = "CloudSharedProofAlias" if self.args.automatic_sync else "CloudSharedProof"
        return f"cloud-proof-{suffix}/{name}"

    def define_schema(self, node, writer, namespace, name, description, *, extra=(), note_type="String"):
        """Compute a typed definition locally. No schema service registers or serves it.
        A later definition of a name may add `extra` (field, description) string fields.
        A `note_type` other than String changes an existing field."""
        fields = {
            IDENTIFIER: ("String", "Stable identifier for the generated record in a DEV shared storage recovery operation."),
            NOTE: (note_type, "Synthetic text that distinguishes versions of the generated DEV shared recovery record."),
            **{field: ("String", text) for field, text in extra},
        }
        declaration = self.root / f"declaration-{name}-{uuid.uuid4().hex}.json"
        save_json(declaration, {
            "name": name, "descriptive_name": description,
            "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
            "fields": list(fields), "field_types": {field: kind for field, (kind, _) in fields.items()},
            "field_descriptions": {field: text for field, (_, text) in fields.items()},
        })
        identity = decode_json(self.writer(node, writer, "define-schema", "--namespace",
                                          namespace, "--declaration", str(declaration)))
        require(isinstance(identity, dict) and set(identity) == {"catalog_id", "portable_id", "definition"}
                and is_digest(identity["catalog_id"]) and is_digest(identity["portable_id"])
                and isinstance(identity["definition"], dict)
                and identity["definition"].get("id") == identity["portable_id"],
                "shared_exact_schema_definition")
        return identity

    def provision_schema(self, node, writer):
        self.schema = self.runtime_schema()
        namespace, name = self.schema.split("/")
        if self.args.automatic_sync:
            name = "CloudSharedProof"
        identity = self.define_schema(node, writer, namespace, name, "Shared Storage Recovery Proof Record")
        save_json(self.bindings, {self.schema: identity["definition"]})
        self.summary["schema_binding"] = {"runtime_name": self.schema, "portable_id": identity["portable_id"],
                                          "catalog_id": identity["catalog_id"]}

    def record(self, kind, version):
        owner = self.proof_owner
        return {IDENTIFIER: kind, NOTE: f"Shared DEV {owner['run_id']}.{owner['run_attempt']} {version}"}

    def payload(self, label, size=FILE_BYTES):
        return hashlib.shake_256((self.payload_id + label).encode()).digest(size)

    def attach(self, node, record, label, size=FILE_BYTES):
        payload = self.payload(label, size)
        digest = hashlib.sha256(payload).hexdigest()
        path = self.root / f"payload-{uuid.uuid4().hex}.bin"
        private_write(path, payload)
        try:
            reply = native_records.put_file(node, self.schema, record, path,
                                            target=native_records.key(record[IDENTIFIER]))
            require(reply["file"]["sha256"] == digest, "shared_native_file_marker")
        finally:
            path.unlink()
        return digest

    def restore(self, destination, device, key_device):
        return self.command(self.root / "credentials", [
            "restore-shared", "--env", "dev", "--database-id", self.scope["database_id"],
            "--snapshot-id", self.summary.get("replacement_snapshot_id", self.snapshot), "--device-id", device,
            "--device-key-file", str(self.device_file(key_device)),
            "--into", str(destination), "--recovery-phrase-stdin", "--json"])

    def verify(self, node, records, files, *, absent=(), file_bytes=FILE_BYTES):
        keys = tuple(dict.fromkeys((*KEYS, *records, *absent)))
        queries = [{"schema_name": self.schema, "filter": {"HashKey": kind}, "limit": 2, "offset": 0}
                   for kind in keys]
        replies = node.call("/api/queries/batch", {"queries": queries})["results"]
        require(len(replies) == len(keys), "shared_restored_keyed_read_count")
        for kind, reply in zip(keys, replies):
            response = reply["response"]
            rows = response["results"]
            count = int(kind in records)
            require(reply["status"] == 200 and response["ok"] is True
                    and response["returned_count"] == count and len(rows) == count
                    and response["has_more"] is False and response["next_cursor"] is None,
                    "shared_exact_key_presence")
            if count == 0:
                continue
            row = rows[0]
            require(row["key"] == native_records.key(kind) and row["fields"] == records[kind]
                    and row["alternates"] == {}, "shared_exact_record_value")
            marker = (row.get("metadata") or {}).get("file")
            if kind in files:
                require(isinstance(marker, dict) and marker.get("size_bytes") == file_bytes
                        and marker.get("sha256") == files[kind], "shared_exact_restored_file_marker")
            else:
                require(marker is None, "shared_exact_record_without_file")
        targets = [{"schema": self.schema, "records": [{"id": kind}], "file_bytes": file_bytes,
                    "file_sha256": digest} for kind, digest in files.items()]
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(node.verify_file, targets))
        node.health()

    def publish_cut(self):
        a, b = self.writers[:2]
        self.summary["stage"] = "prepare-writers"
        first = self.initialize_publisher(a)
        self.summary["stage"] = "author-publisher-prefix"
        first.start()
        local_file = shared_multiwriter.seed_local_only(self, first) if self.args.multiwriter else None
        checkpoint, original, deleted = [self.record(kind, "snapshot") for kind in KEYS[:3]]
        durable_batch(first, [native_records.create(self.schema, native_records.key(record[IDENTIFIER]), record)
                              for record in (checkpoint, original, deleted)])
        checkpoint_file = self.attach(first, checkpoint, "checkpoint")
        publication = self.root / "publication"
        self.summary["stage"] = "publish-initial-native-snapshot"
        # The stream's first entry defines the schema; three records and one file follow.
        receipt = shared_snapshot.invoke(self, first, a, publication, 5)
        self.snapshot = receipt["snapshot_id"]
        self.summary.update(snapshot_id=self.snapshot, checkpoint_through=5,
                            initial_snapshot_files=shared_snapshot.provider_counts(receipt),
                            snapshot_published_before_both_tails=True)
        if local_file is not None:
            first.verify_file(local_file)
        first.stop()
        self.summary["stage"] = "restart-publisher-after-checkpoint"
        first.start()
        previous, removed = native_records.read_records(first, self.schema, [
            native_records.key("winner"), native_records.key("deleted")])
        self.summary["stage"] = "author-publisher-tail"
        losing = self.record("winner", "first-tail")
        durable_batch(first, [native_records.replace(self.schema, previous, losing),
                              native_records.delete(self.schema, removed)])
        if self.args.multiwriter:
            # The publisher's next accepted timestamp must be strictly later than the delete.
            time.sleep(0.01)
        self.summary["stage"] = "attach-publisher-tail-file"
        self.attach(first, losing, "losing-tail")
        self.summary["stage"] = "upload-publisher-tail"
        self.summary["publisher_tail_through"] = shared_daemon.upload(self, first, 8)
        first.stop()

        shared_multiwriter.refuse_empty_join(self, a, b)
        self.summary["stage"] = "restore-peer-before-first-write"
        peer = self.join_writer("peer", b)
        peer.start()
        self.verify(peer, {"checkpoint": checkpoint, "winner": losing},
                    {"checkpoint": checkpoint_file, "winner": hashlib.sha256(
                        self.payload("losing-tail")).hexdigest()})
        prior, = native_records.read_records(peer, self.schema, [native_records.key("winner")])
        self.summary.update(peer_applied_publisher_through=8,
                            peer_read_publisher_records_and_file=True,
                            peer_restored_before_first_write=True)
        winner, peer_only = self.record("winner", "peer-tail"), self.record("peer-only", "peer-tail")
        durable_batch(peer, [native_records.replace(self.schema, prior, winner),
                             native_records.create(self.schema, native_records.key("peer-only"), peer_only)])
        self.attach(peer, winner, "winning-tail")
        self.summary["peer_tail_through"] = shared_daemon.upload(self, peer, 4)
        peer.stop()
        if self.args.multiwriter:
            shared_multiwriter.exchange_tails(self, first, b)

        if self.args.multiwriter:
            shared_multiwriter.snapshot_command(self, first, a)
            shared_multiwriter.publish_peer_tail(self, peer)
        expected_stops = (6, 3) if self.args.multiwriter else (3, 2)
        require((first.clean_stops, peer.clean_stops) == expected_stops,
                "shared_source_strict_clean_stops")
        if self.args.retention:
            shared_retention.run(self, first, peer, a, b)
        elif self.args.multiwriter:
            shared_multiwriter.rejoin_writer(self, first, a, local_file)
        self.remove_sources(self.nodes)
        if os.path.lexists(publication):
            private_directory(publication)
            shutil.rmtree(publication)
        self.summary.update(source_homes_removed=True, publication_workspace_removed=True)

        self.summary["stage"] = "reject-wrong-device-signature"
        rejected = self.root / "rejected"
        try:
            self.restore(rejected, a["device_id"], b["device_id"])
        except OperationError as error:
            require(str(error) == "product_cli_refused" and error.cli_refusal is not None
                    and error.cli_refusal["reason"] == "shared_device_signature_refused"
                    and not os.path.lexists(rejected),
                    "shared_wrong_device_refusal")
        else:
            raise OperationError("shared_wrong_device_accepted")
        self.summary["wrong_device_refused"] = True

    def execute(self):
        if self.args.abandoned:
            shared_abandoned.run(self)
            return
        if self.args.reclamation:
            shared_reclamation.run(self)
            return
        if self.args.automatic_sync:
            shared_automatic.run(self)
            return
        if self.args.conflicts:
            shared_conflicts.run(self)
            return
        if self.args.prior_summary is None:
            self.publish_cut()
        self.summary["stage"] = "restore-authorized-device"
        destination = self.root / "restored"
        publisher, peer = self.writers[:2]
        reader = publisher
        if self.args.multiwriter:
            reader = self.writers[-1]
        elif self.args.prior_summary is not None and self.args.readback_reader == "peer":
            reader = peer
        device = reader["device_id"]
        self.summary["recovery_device"] = device
        receipt = decode_json(self.restore(destination, device, device))
        require(receipt == {"activated": True, "destination": str(destination)},
                "shared_restore_activation")
        node = OwnedNode(destination, self.programs)
        self.nodes.append(node)
        records = {"checkpoint": self.record("checkpoint", "snapshot"),
                   "winner": self.record("winner", "peer-tail"),
                   "peer-only": self.record("peer-only", "peer-tail")}
        if self.args.multiwriter:
            records.update(shared_multiwriter.extra_records(self))
            records[shared_multiwriter.SNAPSHOT_WRITE] = shared_multiwriter.snapshot_record(self)
            records[shared_multiwriter.PEER_TAIL] = shared_multiwriter.peer_tail_record(self)
        files = {"checkpoint": hashlib.sha256(self.payload("checkpoint")).hexdigest(),
                 "winner": hashlib.sha256(self.payload("winning-tail")).hexdigest()}
        if self.args.retention:
            shared_retention.expect_restore(self, records, files)
        elif self.args.multiwriter:
            records.update(shared_multiwriter.rejoin_records(self))
            pending = shared_multiwriter.REJOIN_PENDING
            files[pending] = hashlib.sha256(self.payload(pending)).hexdigest()
        self.summary["stage"] = "verify-restored-native-home"
        for _ in range(2):
            node.start()
            self.verify(node, records, files)
            if self.args.multiwriter:
                require(not shared_multiwriter.has_local_only_schema(node),
                        "shared_local_only_schema_excluded")
            node.stop()
        require(node.clean_stops == 2, "shared_restored_strict_clean_stops")
        if self.args.multiwriter:
            shared_multiwriter.join_restored_writer(self, node, reader)
        self.summary.update(result="passed", restored_boots=2, strict_clean_stops=2,
                            manual_push_pull_commands=0,
                            exact_live_records=len(records), exact_absent_records=1, file_sha256=files)

    def assert_owned_root(self):
        info = private_directory(self.root)
        require((info.st_dev, info.st_ino, info.st_uid) == self.root_identity,
                "shared_owned_root_identity")

    def remove_sources(self, nodes):
        self.assert_owned_root()
        for node in nodes:
            require(node.process is None and node.home.parent == self.root, "shared_owned_stopped_source")
            private_directory(node.home)
            shutil.rmtree(node.home)

    def cleanup(self):
        if self.root is None:
            return
        self.assert_owned_root()
        for node in self.nodes:
            node.cleanup_stop()
        shutil.rmtree(self.root)
        self.summary["cleanup"] = "owned runner data removed"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--prior-summary", type=Path)
    parser.add_argument("--prior-run")
    parser.add_argument("--prior-source")
    parser.add_argument("--readback-reader", choices=("publisher", "peer"), default="peer")
    parser.add_argument("--multiwriter", action="store_true")
    parser.add_argument("--retention", action="store_true")
    parser.add_argument("--conflicts", action="store_true")
    parser.add_argument("--automatic-sync", action="store_true")
    manual = parser.add_mutually_exclusive_group()
    manual.add_argument("--reclamation", action="store_true")
    manual.add_argument("--abandoned", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    operation = Operation(args)

    def interrupted(_signal, _frame):
        raise OperationError("shared_operation_cancelled_or_deadline")

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    timeout = shared_abandoned.TIMEOUT if args.abandoned else shared_reclamation.TIMEOUT
    signal.alarm(timeout + 240 if args.abandoned or args.reclamation else 340)
    try:
        operation.prepare()
        operation.execute()
    except OperationError as error:
        operation.summary.update(failure(error))
    except BaseException as error:
        operation.summary["failure"] = "unexpected_shared_operation_error"
        operation.summary.update(unexpected(error, operation.nodes))
    finally:
        signal.alarm(0)
        try:
            operation.cleanup()
        except BaseException:
            operation.summary.update(result="failed", cleanup="incomplete",
                                     failure="shared_owned_cleanup_failed", stage="cleanup")
        if operation.summary["result"] == "passed":
            operation.summary["stage"] = "complete"
        if operation.output_owned:
            save_json(args.output / "summary.json", operation.summary)
            step_summary(operation.summary)
        print({name: operation.summary.get(name) for name in ("result", "stage", "failure")})
    return 0 if operation.summary["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
