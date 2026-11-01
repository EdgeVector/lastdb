#!/usr/bin/env python3
"""Reclaim only eligible CI DEV bytes before this run owns or uploads a stream."""

import argparse
import os
from pathlib import Path
import signal
import sys

sys.dont_write_bytecode = True

from ci_control import Control
from kept_recovery import FORMAT_SEVEN_SOURCE, legacy_format, recover as recover_kept
from ownership import run_identity
from ownership_history import History
from recover import Operation
from retirement import Retirement
from retirement_observation import Observation
from support import OperationError, require


class Preflight(Operation):
    def __init__(self, args):
        super().__init__(args)
        self.summary = {"schema": "lastdb.cloud-ci-preflight.v1", "source": args.source,
                        "version": args.version, "account": self.summary["account"],
                        "result": "failed", "primary_access": False}

    def prepare(self):
        super().prepare(startup=False)
        self.summary["owner"] = run_identity(self.args.source)

    def execute(self):
        credentials = self.credentials()
        # A resumed cleanup and the next plan each need bounded metadata work
        # plus a full 30-minute service drain. Budget 70 minutes for each pass;
        # never shorten the service's retirement delay to fit the client.
        control = Control(self.baseline, self.api_key, budget_seconds=8400)
        phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "").strip().encode()
        try:
            require(0 < len(phrase) <= 4096, "recovery_phrase")
            self.summary["stage"] = "pending-ci-cleanup"
            capabilities = control.context(require_open=False, startup=True)
            require(capabilities.get("retirement") is True and capabilities.get("headroom") is True,
                    "ci_retirement_capabilities")
            retirement = Retirement(control, self.baseline, observe=Observation(self.summary))
            active = retirement.pending()
            if active is not None:
                resumed = retirement.finish(active=active)
                self.summary["resumed_request"] = {name: resumed[name] for name in (
                    "request_id", "input_sha256", "plan_sha256", "released_bytes", "confirmed_allocations")}
            control.context(startup=True)
            self.summary["stage"] = "ci-ownership-history"
            token = os.environ.pop("GH_TOKEN", "")
            history = History(self.baseline, self.programs.environment, token)
            records = history.collect(limit=4)
            del token
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
            terminal = retirement.create(self.args.output)
            complete = terminal["phase"] == "complete"
            self.summary.update(retirement=terminal, terminal_repeat_checked=complete)
            control.context(startup=True)
            kept = terminal["kept_latest"] if complete else None
            if kept is not None and legacy_format(kept):
                # This client opens only format 7. The service keeps the
                # older kept stream; a refused restore would prove nothing.
                self.summary["kept_latest_legacy_format"] = {
                    "stream_id": kept["stream_id"], "source": kept["owner"]["source"],
                    "format_seven_source": FORMAT_SEVEN_SOURCE, "restore_attempted": False,
                    "retained_by_service": True}
            elif kept is not None:
                self.summary["stage"] = "kept-latest-recovery"
                self.summary["kept_latest_recovery"] = recover_kept(
                    self, kept, credentials, phrase)
            self.summary["stage"] = "ci-quota-headroom"
            self.summary["ci_quota"] = control.usage()
            self.summary["backup_service_requests"] = control.calls
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
    args = parser.parse_args()
    os.umask(0o077)
    operation = Preflight(args)

    def interrupted(_signal, _frame):
        raise OperationError("operation_cancelled_or_deadline")

    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGALRM):
        signal.signal(signum, interrupted)
    signal.alarm(9000)
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
