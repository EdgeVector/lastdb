#!/usr/bin/env python3
"""Restore the kept CI snapshot after one completed DEV cleanup request."""

import argparse
import os
from pathlib import Path
import signal
import sys

sys.dont_write_bytecode = True

from ci_control import Control
from kept_recovery import client_profile, legacy_format, recover as recover_kept, source_profile
from recover import Operation
from retirement import Retirement
from retirement_status import Status
from support import OperationError, canonical_uuid, require


class KeptPass(Operation):
    def __init__(self, args):
        super().__init__(args)
        self.summary = {"schema": "lastdb.cloud-ci-kept.v1", "source": args.source,
                        "version": args.version, "account": self.summary["account"],
                        "result": "failed", "primary_access": False, "provider_deletions": 0}

    def prepare(self):
        super().prepare(startup=False)
        require(canonical_uuid(self.args.request_id) == self.args.request_id, "ci_cleanup_request_id")

    def execute(self):
        credentials = self.credentials()
        control = Control(self.baseline, self.api_key, budget_seconds=60)
        phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "").strip().encode()
        try:
            require(0 < len(phrase) <= 4096, "recovery_phrase")
            self.summary["stage"] = "ci-cleanup-terminal"
            validator = Status(control.scope, self.baseline)
            state = validator.validate(control.post("status", {"request_id": self.args.request_id}))
            require(state["phase"] == "complete" and state["request_id"] == self.args.request_id,
                    "ci_cleanup_not_complete")
            # The repeated execute proves the terminal counts are stable.
            kept = Retirement(control, self.baseline).confirm(validator, state)["kept_latest"]
            require(kept is not None, "ci_kept_latest_missing")
            if legacy_format(kept):
                # This client opens only its own home format. The service keeps
                # the older kept stream; a refused restore would prove nothing.
                self.summary["kept_latest"] = {
                    "stream_id": kept["stream_id"], "source": kept["owner"]["source"],
                    "source_profile": source_profile(kept["owner"]["source"]),
                    "client_profile": client_profile(), "legacy_format": True,
                    "restore_attempted": False, "retained_by_service": True}
            else:
                self.summary["stage"] = "kept-latest-recovery"
                self.summary["kept_latest"] = dict(recover_kept(self, kept, credentials, phrase),
                                                   legacy_format=False)
            self.summary["result"] = "passed"
        finally:
            self.summary["backup_service_requests"] = control.calls
            del phrase
            control.close()
            self.api_key = None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--source", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--request-id", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    operation = KeptPass(args)

    def interrupted(_signal, _frame):
        raise OperationError("operation_cancelled_or_deadline")

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    # The job limit is 10 minutes; owned cleanup needs the rest.
    signal.alarm(420)
    try:
        operation.prepare()
        operation.execute()
    except OperationError as error:
        operation.summary["failure"] = str(error)
    except BaseException:
        operation.summary["failure"] = "unexpected_ci_kept_error"
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
