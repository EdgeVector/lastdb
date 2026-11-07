#!/usr/bin/env python3
"""One manually dispatched DEV backup and exact restore on a fresh account."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import tempfile

from fresh_account_broker import Broker
import fresh_account_source as source
from node import OwnedNode
from programs import Programs
from restore_receipts import restored, selection
from support import OperationError, child_environment, private_directory, read_json, require, save_json


OPERATIONS = ("roundtrip", "interrupt-after-upload")
WORKFLOW = "EdgeVector/lastdb/.github/workflows/cloud-backup-fresh-account.yml@refs/heads/main"
DEADLINE_SECONDS = 720


def admission(args):
    environment = os.environ
    require(environment.get("GITHUB_ACTIONS") == "true"
            and environment.get("RUNNER_ENVIRONMENT") == "github-hosted"
            and environment.get("GITHUB_REPOSITORY") == "EdgeVector/lastdb"
            and environment.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and environment.get("GITHUB_REF") == "refs/heads/main"
            and environment.get("GITHUB_WORKFLOW_REF") == WORKFLOW
            and re.fullmatch(r"[0-9a-f]{40}", args.source)
            and environment.get("GITHUB_SHA") == environment.get("GITHUB_WORKFLOW_SHA") == args.source,
            "accepted_main_dispatch")
    require(re.fullmatch(r"[A-Za-z0-9.-]{1,128}", args.version)
            and args.version.endswith(".g" + args.source[:9]), "fresh_client_version")
    run = environment.get("GITHUB_RUN_ID", "")
    attempt = environment.get("GITHUB_RUN_ATTEMPT", "")
    require(all(re.fullmatch(r"[1-9][0-9]{0,19}", value) for value in (run, attempt)), "fresh_workflow_run")
    runner = Path(environment["RUNNER_TEMP"])
    require(runner.is_absolute() and runner.resolve(strict=True) == runner and runner.is_dir()
            and runner.stat().st_uid == os.getuid()
            and args.output == runner / "owned-fresh-account-result"
            and args.bin_dir == runner / "fresh-backup-bin", "fresh_runner_paths")
    private_directory(args.bin_dir)
    args.output.mkdir(mode=0o700)
    return {"schema": "lastdb.fresh-dev-account-roundtrip.v1", "environment": "dev", "source": args.source,
            "version": args.version, "operation": args.operation, "run_id": run, "run_attempt": attempt,
            "result": "failed", "stage": "prepare"}


def recover(programs, credentials, parent, phrase, expected, point, report, operation):
    parent.mkdir(mode=0o700)
    destination = parent / "restored"
    require(not os.path.lexists(destination), "fresh_restore_destination_absent")
    selected = {**expected, **point}
    restored_report = programs.restore(credentials, destination, selected, phrase)
    session, _identifier = selection(destination, selected)
    require(read_json(session / "selection.json")["accepted"] == report["accepted_backup"]
            and restored_report["native_files"] == report["native_files"]
            and restored_report["native_bytes"] == report["native_bytes"],
            "fresh_restore_exact_accepted_snapshot")
    proof = restored(destination, restored_report, selected)
    files = proof.pop("files")
    b2_runs = sum(item["path"].startswith("disk-cas/runs/") for item in files)
    require(b2_runs > 0, "fresh_restore_b2_run")
    inventory = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    proof.update(native_inventory_sha256=hashlib.sha256(inventory).hexdigest(), b2_run_files=b2_runs)
    operation.summary["restore"] = proof
    operation.summary["stage"] = "verify_restored_boots"
    node = OwnedNode(destination, programs, account=point["account_id"])
    operation.node = node
    reads = []
    for _number in range(2):
        node.start()
        reads.append(node.verify_records(expected))
        node.stop()
    require(node.clean_stops == 2 and not node.forced and node.process is None, "fresh_restore_two_clean_stops")
    operation.summary.update(restored_boots=2, strict_restore_stops=2, keyed_reads=reads)


class Operation:
    def __init__(self, args, summary):
        self.args, self.summary = args, summary
        self.root, self.root_stat, self.node = None, None, None

    def run(self):
        self.root = Path(tempfile.mkdtemp(prefix="ldb-fresh-", dir="/tmp"))
        self.root_stat = private_directory(self.root)
        require(self.root.resolve(strict=True) == self.root, "fresh_private_root")
        for name in ("os-home", "tmp"):
            (self.root / name).mkdir(mode=0o700)
        programs = Programs(self.args.bin_dir, self.args.version, child_environment(self.root))
        self.summary["programs"] = programs.identities
        broker = Broker()
        home = self.root / "source"
        self.summary["stage"] = "initialize_identity"
        account = source.initialize(programs, home)
        self.summary["account_id"] = account
        self.summary["stage"] = "record_lease"
        invite = broker.lease(account, self.args.operation, source.public_key(programs, home, account))
        self.summary["lease_recorded"] = True
        self.summary["stage"] = "register_account"
        key = source.register(programs, home, account, invite)
        del invite
        self.summary["stage"] = "record_enrollment"
        broker.enrolled(account, key)
        self.summary["enrollment_recorded"] = True
        self.summary["stage"] = "write_source"
        self.node = OwnedNode(home, programs, backup_source=True, account=account)
        self.node.start()
        source.bind(self.node, key)
        expected = source.seed(self.node, self.root)
        self.summary["source_reads"] = self.node.verify_records(expected)
        self.summary["stage"] = "publish_stopped_snapshot"
        point, report = source.publish(self.node)
        self.summary.update(snapshot=point, source_stopped=True, snapshot_ordinal="1",
                            source_released=True, all_remote_file_hashes_verified=True,
                            snapshot_inventory_bytes=int(report["accepted_backup"]["inventory_root"]["stored_bytes"]))
        self.summary["stage"] = "record_snapshot"
        broker.point("snapshot", point)
        self.summary["snapshot_recorded"] = True
        if self.args.operation == "interrupt-after-upload":
            raise OperationError("intentional_interrupt_after_upload")
        self.summary["stage"] = "restore_exact_snapshot"
        credentials = self.root / "credentials"
        phrase = source.restore_inputs(home, credentials, key)
        del key
        recover(programs, credentials, self.root / "restore", phrase, expected, point, report, self)
        del phrase
        self.summary["stage"] = "record_verified_restore"
        broker.point("verified", point)
        self.summary["verification_recorded"] = True
        self.summary["stage"] = "complete"
        self.summary["result"] = "passed"

    def cleanup(self):
        if self.root is None:
            return
        if self.node is not None:
            self.node.cleanup_stop()
            require(self.node.process is None, "fresh_no_owned_process")
            self.summary["forced_cleanup_stop"] = self.node.forced
        observed = private_directory(self.root)
        require(shutil.rmtree.avoids_symlink_attacks
                and self.root.parent == Path("/tmp") and self.root.name.startswith("ldb-fresh-")
                and self.root.resolve(strict=True) == self.root
                and (observed.st_dev, observed.st_ino) == (self.root_stat.st_dev, self.root_stat.st_ino),
                "fresh_cleanup_root_identity")
        shutil.rmtree(self.root)
        self.summary["runner_private_files_removed"] = True


def finish(args, summary):
    save_json(args.output / "summary.json", summary)
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        path = Path(destination)
        runner = Path(os.environ["RUNNER_TEMP"])
        require(path.is_absolute() and path.parent.resolve(strict=True).is_relative_to(runner), "fresh_summary_path")
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "w") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and info.st_nlink == 1,
                    "fresh_summary_file")
            stream.write("\n### DEV fresh-account backup\n\n```json\n")
            json.dump(summary, stream, sort_keys=True, indent=2)
            stream.write("\n```\n")
    print(json.dumps(summary, sort_keys=True, separators=(",", ":")), flush=True)


def interrupted(signum, _frame):
    raise OperationError("fresh_operation_deadline" if signum == signal.SIGALRM else "fresh_operation_interrupted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        operation = Operation(args, admission(args))
    except Exception:
        print('{"result":"failed","failure":"fresh_runner_admission"}', flush=True)
        return 1
    for signum in (signal.SIGALRM, signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupted)
    signal.alarm(DEADLINE_SECONDS)
    try:
        operation.run()
    except OperationError as error:
        operation.summary["failure"] = str(error)
    except Exception:
        operation.summary["failure"] = "fresh_operation_error"
    finally:
        signal.alarm(0)
        try:
            operation.cleanup()
        except Exception:
            operation.summary.update(result="failed", cleanup_failure="fresh_runner_cleanup")
    try:
        finish(args, operation.summary)
    except Exception:
        print('{"result":"failed","failure":"fresh_summary_write"}', flush=True)
        return 1
    return 0 if operation.summary["result"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
