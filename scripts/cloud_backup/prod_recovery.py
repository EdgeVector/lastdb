#!/usr/bin/env python3
"""Prove snapshot and mutation-log recovery on a dedicated PROD account in CI.

The account must be provisioned separately from any primary. Only its exact
protected scope is accepted. All homes contain generated data under a fresh
runner directory. No existing local home or customer backup is an input.
"""

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile

sys.dont_write_bytecode = True

import native_records
from automatic import selection
from automatic_source import Source, durable_batch
from baseline import private_write
from baseline_data import seed
from node import OwnedNode
from programs import Programs, execute
from restore_receipts import restored
from support import (CREDENTIALS_FILE, OperationError, child_environment, decode_json,
                     failure, private_directory, require, save_json)


def endpoint():
    path = Path(__file__).resolve().parents[2] / "folddb_profile/environments.json"
    value = decode_json(path.read_bytes())["environments"]["prod"]["exemem_api"]
    require(isinstance(value, str) and re.fullmatch(
        r"https://[a-z0-9]+\.execute-api\.us-east-1\.amazonaws\.com", value), "prod_endpoint")
    return value


class ProductionPrograms(Programs):
    def restore(self, home, destination, baseline, phrase):
        self.verify()
        # A later release check uses false, proving normal restore needs no switch.
        environment = dict(self.environment, LASTDB_BYTE_BACKUP_V2_ENABLED=os.environ["RESTORE_GATE"])
        command = [str(self.directory / "lastdb"), "--data-dir", str(home), "cloud", "backup-v2",
                   "restore", "--recovery-phrase-stdin", "--stream", baseline["stream_id"],
                   "--generation", baseline["account_generation"], "--commit", baseline["commit_id"],
                   "--commit-sha256", baseline["commit_sha256"], "--into", str(destination), "--json"]
        return decode_json(execute(command, environment, phrase, timeout=120))


class ProductionRecovery:
    def __init__(self, args):
        self.args, self.root, self.node = args, None, None
        self.nodes, self.output_owned = [], False
        self.summary = {"schema": "lastdb.prod-recovery.v1", "source": args.source,
                        "version": args.version, "result": "failed", "stage": "prepare",
                        "primary_access": False, "provider_deletions": 0}

    def prepare(self):
        require(os.environ.get("GITHUB_ACTIONS") == "true"
                and os.environ.get("GITHUB_REF") == "refs/heads/main", "accepted_main_ci_only")
        require(re.fullmatch(r"[0-9a-f]{40}", self.args.source)
                and self.args.version.endswith(".g" + self.args.source[:9]), "source_version_binding")
        require(os.environ.get("RESTORE_GATE") in ("true", "false"), "explicit_restore_gate")
        self.summary["restore_release_switch"] = os.environ["RESTORE_GATE"]
        self.account = os.environ["LASTDB_CLOUD_PROD_PROOF_ACCOUNT"]
        require(re.fullmatch(r"[0-9a-f]{32}", self.account), "dedicated_prod_account")
        runner = Path(os.environ["RUNNER_TEMP"]).resolve()
        require(self.args.output.is_absolute() and self.args.output.parent.resolve() == runner
                and not os.path.lexists(self.args.output), "owned_runner_output")
        self.args.output.mkdir(mode=0o700)
        self.output_owned = True
        self.root = Path(tempfile.mkdtemp(prefix="ldb-prod-", dir="/tmp")).resolve()
        self.root.chmod(0o700)
        info = private_directory(self.root)
        self.identity = info.st_dev, info.st_ino, info.st_uid
        for name in ("os-home", "tmp", "credentials", "source"):
            (self.root / name).mkdir(mode=0o700)
        environment = child_environment(self.root)
        environment.update(ENVIRONMENT="prod", EXEMEM_ENV="prod")
        self.programs = ProductionPrograms(self.args.bin_dir, self.args.version, environment)
        self.summary.update(account=self.account, binaries=self.programs.identities)

    def owned_node(self, home, source=False):
        node = OwnedNode(home, self.programs, backup_source=source, account=self.account)
        self.nodes.append(node)
        self.node = node
        return node

    def credentials(self):
        phrase = os.environ.pop("LASTDB_CLOUD_PROD_PROOF_RECOVERY_PHRASE", "").strip().encode()
        identity = seed(phrase)
        # Health binds this identity to the protected dedicated account before connect/upload.
        private_write(self.root / "source/identity.key", identity)
        del identity
        key = os.environ.pop("LASTDB_CLOUD_PROD_PROOF_API_KEY", "").strip().encode()
        require(0 < len(key) <= 4096 and all(32 <= char < 127 for char in key), "prod_api_key")
        save_json(self.root / "credentials" / CREDENTIALS_FILE,
                  {"api_url": endpoint(), "api_key": key.decode()})
        return phrase, key

    def write_source(self, key):
        node = self.owned_node(self.root / "source", source=True)
        node.start()
        expected = {"account": self.account, "schema": "CloudProdRecovery",
                    "records": [{"id": "prod-proof-" + str(n),
                                 "note": "Generated recovery record " + str(n)} for n in range(16)]}
        source = Source(node, expected, self.args.source, self.summary)
        source.connect(key, environment="prod")
        fields = ["id", "note"]
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": expected["schema"],
            "descriptive_name": expected["schema"], "schema_type": "Hash",
            "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
            "fields": fields, "field_types": {"id": "String"},
            "field_descriptions": {name: "PROD recovery proof " + name for name in fields}}})
        durable_batch(node, [native_records.create(expected["schema"], native_records.key(record["id"]), record)
            for record in expected["records"]])
        payload = hashlib.shake_256(self.args.source.encode()).digest(8 * 1024**2)
        path = self.root / "source-file.bin"
        private_write(path, payload)
        expected.update(file_bytes=len(payload), file_sha256=hashlib.sha256(payload).hexdigest())
        native_records.put_file(node, expected["schema"], expected["records"][0], path)
        path.unlink()
        source.expected = copy.deepcopy(expected)
        self.summary["source_reads"] = node.verify_records(expected)
        return source, expected

    def recover(self, name, expected, accepted, phrase, replay=None, observe=None):
        self.summary["stage"] = name
        require(all(node.process is None for node in self.nodes), "all_sources_stopped")
        parent = self.root / name
        parent.mkdir(mode=0o700)
        destination = parent / "restored"
        selected = selection(expected, accepted)
        report = self.programs.restore(self.root / "credentials", destination, selected, phrase)
        proof = restored(destination, report, selected, replay, observe, endpoint=endpoint())
        files = json.dumps(proof.pop("files"), sort_keys=True).encode()
        proof["native_inventory_sha256"] = hashlib.sha256(files).hexdigest()
        node = self.owned_node(destination)
        reads = []
        for _ in range(2):
            node.start()
            reads.append(node.verify_records(expected))
            node.stop()
        require(node.clean_stops == 2, "two_recovered_clean_restarts")
        return {"selection": {field: selected[field] for field in (
                    "account", "account_generation", "stream_id", "commit_id", "commit_sha256")},
                "proof": proof, "reads": reads, "clean_restarts": node.clean_stops}

    def execute(self):
        phrase, key = self.credentials()
        try:
            self.summary["stage"] = "generated-source"
            source, expected = self.write_source(key)
            del key
            self.summary["stage"] = "automatic-snapshot"
            source.configure()
            snapshot = source.published("snapshot")
            source.caught_up(snapshot)
            self.summary["stage"] = "automatic-mutation-log"
            source.write(expected["schema"])
            minimum = source.cloud("status")["mutation_log"]["head"]
            require(minimum is not None, "written_log_frontier")
            log = source.published("mutation_log", snapshot["last_accepted_backup"]["commit_sha256"],
                                   through=minimum["frontiers"])
            source.caught_up(log)
            source.disable()
            source.node.stop()
            require(source.node.home == self.root / "source"
                    and source.node.process is None, "generated_source_cleanup")
            shutil.rmtree(source.node.home)
            self.summary["generated_source_removed_before_restore"] = True
            # Only the credentials and phrase remain: the generated source is gone.
            self.summary["snapshot"] = self.recover("snapshot-recovery", expected,
                snapshot["last_accepted_backup"], phrase)
            self.summary["log"] = self.recover("log-recovery", source.expected,
                log["last_accepted_backup"], phrase,
                replay={"base": snapshot["last_accepted_backup"], "minimum": minimum},
                observe=source.observe_accepted)
            self.summary.update(result="passed", stage="complete", source_stopped=True,
                                retained_checkpoints=len(source.completed),
                                declared_bytes=source.declared_bytes)
        finally:
            del phrase

    def cleanup(self):
        stopped = True
        for node in self.nodes:
            if node.process is not None:
                try:
                    node.stop()
                except BaseException:
                    stopped = False
                    node.cleanup_stop()
        if not stopped:
            self.summary.update(result="failed", failure="owned_clean_stop_failed")
        if self.root is not None:
            info = private_directory(self.root)
            require((info.st_dev, info.st_ino, info.st_uid) == self.identity
                    and self.root.parent == Path("/tmp").resolve()
                    and self.root.name.startswith("ldb-prod-")
                    and all(node.process is None for node in self.nodes), "owned_cleanup_identity")
            # No key, phrase, native home or product reply enters uploaded artifacts.
            shutil.rmtree(self.root)
            self.summary["cleanup"] = "owned runner homes removed; cloud checkpoints retained"

    def finish(self):
        if self.output_owned:
            save_json(self.args.output / "summary.json", self.summary)
        print(json.dumps({key: self.summary.get(key) for key in (
            "result", "stage", "failure", "service_field", "source", "primary_access")}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    operation = ProductionRecovery(args)
    def interrupted(_signal, _frame):
        raise OperationError("operation_cancelled_or_deadline")
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    signal.alarm(480)
    try:
        operation.prepare()
        operation.execute()
    except OperationError as error:
        operation.summary.update(failure(error))
    except BaseException:
        operation.summary["failure"] = "unexpected_operation_error"
    finally:
        signal.alarm(0)
        try:
            operation.cleanup()
        except BaseException:
            operation.summary.update(result="failed", failure="owned_cleanup_failed")
        operation.finish()
    return 0 if operation.summary["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
