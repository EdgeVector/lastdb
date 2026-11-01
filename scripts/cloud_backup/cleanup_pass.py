#!/usr/bin/env python3
"""Advance the DEV CI cleanup in one bounded pass; a later pass resumes it."""

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile

sys.dont_write_bytecode = True

from ci_control import Control
from ownership_history import History
from recover import step_summary
from retirement import Retirement
from retirement_observation import Observation
from support import (ACCOUNT, OperationError, baseline_from_environment, child_environment,
                     private_directory, require, save_json)

SCHEMA = "lastdb.cloud-ci-cleanup.v1"
RECOVERY_RUNS = "actions/workflows/cloud-backup-recovery.yml/runs?branch=main&per_page=1&status="
# The job limit is 10 minutes. The pass stops early; the service keeps the
# saved request and its frozen epoch for the next pass.
BUDGET_SECONDS = 420


class Pass:
    def __init__(self, args):
        self.args, self.root, self.root_identity = args, None, None
        self.output_owned, self.kept_request = False, None
        self.summary = {"schema": SCHEMA, "account": ACCOUNT, "result": "failed",
                        "primary_access": False, "create_requested": args.create, "phase": None}

    def prepare(self):
        require(os.environ.get("GITHUB_ACTIONS") == "true"
                and os.environ.get("GITHUB_REF") == "refs/heads/main", "github_main_only")
        runner = Path(os.environ.get("RUNNER_TEMP", ""))
        output = self.args.output
        require(runner.is_absolute() and runner.is_dir() and output.is_absolute()
                and output.parent.resolve() == runner.resolve()
                and re.fullmatch(r"[A-Za-z0-9._-]{1,80}", output.name)
                and not os.path.lexists(output), "owned_output_path")
        output.mkdir(mode=0o700)
        self.output_owned = True
        # Only the GitHub CLI uses this home; no product program runs here.
        self.root = Path(tempfile.mkdtemp(prefix="ldb-cc-", dir="/tmp")).resolve()
        self.root.chmod(0o700)
        info = private_directory(self.root)
        self.root_identity = info.st_dev, info.st_ino, info.st_uid
        for name in ("os-home", "tmp"):
            (self.root / name).mkdir(mode=0o700)
        self.baseline = baseline_from_environment()

    def execute(self):
        key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY", "")
        token = os.environ.pop("GH_TOKEN", "")
        require(0 < len(key.encode()) <= 4096 and key.strip() == key, "dev_api_key")
        control = Control(self.baseline, key.encode(), budget_seconds=BUDGET_SECONDS)
        del key
        try:
            self.summary["stage"] = "ci-cleanup-state"
            capabilities = control.context(require_open=False, startup=True)
            require(capabilities.get("retirement") is True and capabilities.get("headroom") is True,
                    "ci_retirement_capabilities")
            retirement = Retirement(control, self.baseline, observe=Observation(self.summary))
            active = retirement.pending()
            if active is not None:
                self.summary["stage"] = "resume-ci-cleanup"
                state = retirement.finish(active=active)
            elif self.args.create:
                state = self.create(control, retirement, token)
            else:
                state = None
            del token
            if state is not None:
                self.summary["phase"] = state["phase"]
                if state["phase"] == "complete":
                    self.summary["terminal_repeat_checked"] = True
                    if state["kept_latest"] is not None:
                        self.kept_request = state["request_id"]
            self.summary["result"] = "passed"
        finally:
            self.summary["backup_service_requests"] = control.calls
            control.close()

    def create(self, control, retirement, token):
        self.summary["stage"] = "ci-recovery-idle"
        history = History(self.baseline, child_environment(self.root), token)
        # A new request freezes new uploads. Never freeze under a recovery run.
        for status in ("queued", "waiting", "in_progress"):
            value = history.api(RECOVERY_RUNS + status)
            require(type(value.get("total_count")) is int, "ci_recovery_run_directory")
            if value["total_count"] != 0:
                self.summary["create_deferred"] = "recovery_run_" + status
                return None
        control.context(startup=True)
        self.summary["stage"] = "ci-ownership-history"
        records = history.collect(limit=4)
        self.summary["ci_ownership_history"] = {
            "recent_successful_runs_limit": 4, "verified_streams": len(records),
            "unknown_records": history.unknown_records, "invalid_records": history.invalid_records,
            "complete_directory": False, "provider_deletion_authority": False}
        self.summary["stage"] = "ci-ownership-registration"
        # Earlier direct-PUT runs lack a durable recovery recipe and their
        # uncertain uploads remain protected. Never adopt them for cleanup.
        eligible = [record for record in records if record["ownership"].get("recipe") is not None]
        registered = [control.register(record) for record in eligible]
        self.summary["ci_ownership_registration"] = {
            "registered": len(registered), "recipe_less_retained": len(records) - len(eligible),
            "refresh": control.refresh(), "provider_deletions": 0}
        self.summary["stage"] = "retire-old-ci-backups"
        return retirement.create(self.args.output)

    def cleanup(self):
        if self.root is None:
            return
        info = private_directory(self.root)
        require((info.st_dev, info.st_ino, info.st_uid) == self.root_identity
                and self.root.parent == Path("/tmp").resolve()
                and self.root.name.startswith("ldb-cc-"), "cleanup_root_identity")
        shutil.rmtree(self.root)
        self.summary["cleanup"] = "owned runner data removed"

    def finish(self):
        if self.output_owned:
            save_json(self.args.output / "summary.json", self.summary)
            step_summary(self.summary)
        destination = os.environ.get("GITHUB_OUTPUT")
        if destination and self.summary["result"] == "passed":
            with open(destination, "a", encoding="utf-8") as stream:
                stream.write("kept_request=" + (self.kept_request or "") + "\n")
        # Only these validated public fields enter the Actions log.
        print(json.dumps({name: self.summary.get(name) for name in (
            "result", "stage", "failure", "phase", "create_deferred", "retirement",
            "retirement_diagnostics", "backup_service_requests", "primary_access")}, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--create", action="store_true",
                        help="start a new request when none is active and no recovery run is active")
    args = parser.parse_args()
    os.umask(0o077)
    operation = Pass(args)

    def interrupted(_signal, _frame):
        raise OperationError("operation_cancelled_or_deadline")

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    signal.alarm(BUDGET_SECONDS + 60)
    try:
        operation.prepare()
        operation.execute()
    except OperationError as error:
        operation.summary["failure"] = str(error)
    except BaseException:
        operation.summary["failure"] = "unexpected_ci_cleanup_error"
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
