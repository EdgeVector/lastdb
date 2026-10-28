#!/usr/bin/env python3
"""Restore one pinned DEV backup with the actual CI-built Mini pair."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile
import time

sys.dont_write_bytecode = True

from node import OwnedNode
from programs import Programs
from restore_receipts import restored
from support import (ACCOUNT, OperationError, baseline_from_environment, child_environment,
                     dev_endpoint, private_directory, require, save_json)


class Operation:
    def __init__(self, args):
        self.args, self.root, self.node = args, None, None
        self.root_identity, self.preserve = None, False
        self.output_owned = False
        self.summary = {
            "schema": "lastdb.cloud-dev-restore.v1", "mode": "restore", "source": args.source,
            "version": args.version, "account": ACCOUNT, "result": "failed",
            "remote_read_only": True, "new_backup_requests": 0, "primary_access": False,
            "coverage": "pinned accepted format-4 restore; upload and acceptance are not covered",
        }

    def prepare(self):
        require(os.environ.get("GITHUB_ACTIONS") == "true", "github_ci_only")
        require(re.fullmatch(r"[0-9a-f]{40}", self.args.source), "source_revision")
        require(re.fullmatch(r"[A-Za-z0-9.+_-]{1,128}", self.args.version), "build_version")
        require(self.args.version.endswith(".g" + self.args.source[:9]), "source_version_binding")
        runner = Path(os.environ.get("RUNNER_TEMP", ""))
        require(runner.is_absolute() and runner.is_dir(), "runner_temp")
        output = self.args.output
        require(output.is_absolute() and output.parent.resolve() == runner.resolve()
                and re.fullmatch(r"[A-Za-z0-9._-]{1,80}", output.name)
                and not os.path.lexists(output), "owned_output_path")
        output.mkdir(mode=0o700)
        self.output_owned = True
        self.root = Path(tempfile.mkdtemp(prefix="ldb-cb-", dir="/tmp")).resolve()
        self.root.chmod(0o700)
        info = private_directory(self.root)
        self.root_identity = info.st_dev, info.st_ino, info.st_uid
        for name in ("credentials", "os-home", "tmp"):
            (self.root / name).mkdir(mode=0o700)
        self.baseline = baseline_from_environment()
        self.summary["selection"] = {key: self.baseline[key] for key in (
            "stream_id", "account_generation", "commit_id", "commit_sha256")}
        self.programs = Programs(self.args.bin_dir, self.args.version, child_environment(self.root))
        self.summary["binaries"] = self.programs.identities

    def credentials(self):
        key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY", "")
        require(0 < len(key.encode()) <= 4096 and key.strip() == key
                and all(ord(character) >= 32 and ord(character) != 127 for character in key),
                "dev_api_key")
        home = self.root / "credentials"
        save_json(home / "cloud_sync.json.paused", {"api_url": dev_endpoint(), "api_key": key})
        require(set(path.name for path in home.iterdir()) == {"cloud_sync.json.paused"},
                "credential_only_home")
        del key
        return home

    def execute(self):
        credentials = self.credentials()
        destination = self.root / "restored"
        phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "").strip().encode()
        require(0 < len(phrase) <= 4096, "recovery_phrase")
        started = time.monotonic()
        try:
            report = self.programs.restore(credentials, destination, self.baseline, phrase)
        finally:
            del phrase
            (credentials / "cloud_sync.json.paused").unlink()
        require(type(report.get("native_bytes")) is int and 0 < report["native_bytes"] <= 1024**3,
                "pinned_ci_native_size")
        native = restored(destination, report, self.baseline)
        # The per-file names and all product controls remain private in memory.
        inventory = json.dumps(native.pop("files"), sort_keys=True, separators=(",", ":")).encode()
        native["native_inventory_sha256"] = hashlib.sha256(inventory).hexdigest()
        self.summary["restore"] = native
        self.summary["restore_seconds"] = round(time.monotonic() - started, 3)
        self.node = OwnedNode(destination, self.programs)
        boots = []
        for _number in range(2):
            self.node.start()
            boots.append(self.node.verify_records(self.baseline))
            try:
                self.node.stop()
            except BaseException:
                self.preserve = True
                raise
        require(self.node.clean_stops == 2, "two_clean_restarts")
        self.summary.update(result="passed", restored_boots=2, strict_clean_stops=2,
                            keyed_reads=boots, file_bytes=self.baseline["file_bytes"],
                            file_sha256=self.baseline["file_sha256"])

    def cleanup(self):
        if self.node is not None and self.node.process is not None:
            try:
                self.node.stop()
            except BaseException:
                self.preserve = True
                self.node.cleanup_stop()
        if self.root is None:
            return
        info = private_directory(self.root)
        require((info.st_dev, info.st_ino, info.st_uid) == self.root_identity
                and self.root.parent == Path("/tmp").resolve()
                and self.root.name.startswith("ldb-cb-"), "cleanup_root_identity")
        require(self.node is None or self.node.process is None, "cleanup_no_daemon")
        if self.preserve:
            self.summary["cleanup"] = "private runner data retained after a strict stop failure"
            self.summary["private_recovery_path"] = str(self.root)
        else:
            shutil.rmtree(self.root)
            self.summary["cleanup"] = "owned runner data removed"

    def finish(self):
        if self.output_owned:
            save_json(self.args.output / "summary.json", self.summary)
            step_summary(self.summary)
        # Only this fixed summary can enter the Actions log or summary.
        print(json.dumps({"result": self.summary["result"], "mode": "restore",
                          "source": self.args.source, "primary_access": False}, sort_keys=True))


def step_summary(summary):
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if not destination:
        return
    path = Path(destination)
    runner = Path(os.environ["RUNNER_TEMP"]).resolve()
    require(path.is_absolute() and path.parent.resolve().is_relative_to(runner), "ci_summary_path")
    text = "\n### DEV backup recovery\n\n```json\n"
    text += json.dumps(summary, sort_keys=True, indent=2) + "\n```\n"
    require(len(text.encode()) <= 65536, "ci_summary_size")
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--mode", required=True, choices=["restore"])
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    operation = Operation(args)
    def interrupted(_signal, _frame):
        raise OperationError("operation_cancelled_or_deadline")
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    signal.alarm(1800)
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
                                     failure="owned_cleanup_failed")
        operation.finish()
    return 0 if operation.summary["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
