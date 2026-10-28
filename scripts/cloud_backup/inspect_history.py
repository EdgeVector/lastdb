#!/usr/bin/env python3
"""Read real successful CI ownership records with no cloud-backup credentials."""

import json
import os
from pathlib import Path
import sys
import tempfile

sys.dont_write_bytecode = True

from ownership_history import History
from support import (OperationError, baseline_scope_from_environment, child_environment,
                     private_directory, require)


def main():
    os.umask(0o077)
    summary = {"result": "failed", "backup_service_requests": 0,
               "provider_deletions": 0, "primary_access": False}
    try:
        require(os.environ.get("GITHUB_ACTIONS") == "true", "github_ci_only")
        runner = Path(os.environ.get("RUNNER_TEMP", ""))
        require(runner.is_absolute() and runner.is_dir(), "runner_temp")
        scope = baseline_scope_from_environment()
        token = os.environ.pop("GH_TOKEN", "")
        with tempfile.TemporaryDirectory(prefix="ci-ownership-read-", dir=runner) as temporary:
            root = Path(temporary)
            private_directory(root)
            for name in ("os-home", "tmp"):
                (root / name).mkdir(mode=0o700)
            records, unknown, invalid = [], 0, 0
            for event in ("push", "workflow_dispatch"):
                history = History(scope, child_environment(root), token)
                records.extend(history.collect(limit=2, event=event))
                unknown += history.unknown_records
                invalid += history.invalid_records
            del token
            require(records or invalid == 0, "unverifiable_actual_ci_ownership_records")
            summary.update(result="passed" if records else "no_current_records", verified_records=len(records),
                           unknown_records=unknown, invalid_records=invalid,
                           baseline_exclusion_verified=bool(records), complete_directory=False)
    except OperationError as error:
        summary["failure"] = str(error)
    except BaseException:
        summary["failure"] = "unexpected_ci_history_error"
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["result"] in ("passed", "no_current_records") else 1


if __name__ == "__main__":
    sys.exit(main())
