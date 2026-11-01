#!/usr/bin/env python3
"""Admit one DEV recovery run in seconds; never wait for or start a cleanup."""

import argparse
import os
from pathlib import Path
import signal
import sys

sys.dont_write_bytecode = True

from ci_control import Control
from ownership import run_identity
from recover import Operation
from retirement import Retirement
from support import OperationError, require


class Preflight(Operation):
    def __init__(self, args):
        super().__init__(args)
        self.summary = {"schema": "lastdb.cloud-ci-preflight.v2", "source": args.source,
                        "version": args.version, "account": self.summary["account"],
                        "result": "failed", "primary_access": False, "cleanup_active": None}

    def prepare(self):
        super().prepare(startup=False)
        self.summary["owner"] = run_identity(self.args.source)

    def execute(self):
        self.credentials()
        control = Control(self.baseline, self.api_key, budget_seconds=60)
        try:
            self.summary["stage"] = "ci-cleanup-state"
            capabilities = control.context(require_open=False, startup=True)
            require(capabilities.get("retirement") is True and capabilities.get("headroom") is True,
                    "ci_retirement_capabilities")
            active = Retirement(control, self.baseline).pending()
            if active is not None:
                # The service refuses new uploads until this request ends. The
                # cleanup workflow resumes it in bounded passes; a run never waits.
                self.summary["cleanup_active"] = True
                self.summary["active_cleanup"] = {name: active.get(name) for name in (
                    "request_id", "phase", "resume_after_ms")}
                raise OperationError("ci_cleanup_in_progress")
            self.summary["cleanup_active"] = False
            control.context(startup=True)
            self.summary["stage"] = "ci-quota-headroom"
            self.summary["ci_quota"] = control.usage()
            self.summary["result"] = "passed"
        finally:
            self.summary["backup_service_requests"] = control.calls
            control.close()
            self.api_key = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    operation = Preflight(args)

    def interrupted(_signal, _frame):
        raise OperationError("operation_cancelled_or_deadline")

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    signal.alarm(120)
    try:
        operation.prepare()
        operation.execute()
    except OperationError as error:
        operation.summary["failure"] = str(error)
    except BaseException:
        operation.summary["failure"] = "unexpected_ci_preflight_error"
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
