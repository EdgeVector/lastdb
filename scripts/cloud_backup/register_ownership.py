#!/usr/bin/env python3
"""Save an actual uploaded CI receipt before its workflow ends or archive expires."""

import argparse
import json
import os
from pathlib import Path
import re
import sys
import tempfile

sys.dont_write_bytecode = True

from ci_control import Control
from ownership_history import History
from startup_proof import runner_path, save as save_startup
from support import (OperationError, baseline_from_environment, child_environment,
                     private_directory, require)


def summary(value):
    print(json.dumps(value, sort_keys=True))
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        path = Path(destination)
        runner = Path(os.environ["RUNNER_TEMP"]).resolve()
        require(path.is_absolute() and path.parent.resolve().is_relative_to(runner), "ci_summary_path")
        raw = "\n### CI ownership registration\n\n```json\n"
        raw += json.dumps(value, sort_keys=True, indent=2) + "\n```\n"
        require(len(raw.encode()) <= 4096, "ci_registration_summary_size")
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "w") as stream:
            stream.write(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--artifact-id", required=True)
    parser.add_argument("--artifact-sha256", required=True)
    parser.add_argument("--phase", choices=("startup", "complete"), default="complete")
    parser.add_argument("--startup-proof", type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    control = None
    result = {"result": "failed", "primary_access": False, "provider_deletions": 0}
    try:
        require(re.fullmatch(r"[0-9a-f]{40}", args.source), "ci_registration_source")
        require(re.fullmatch(r"[1-9][0-9]{0,19}", args.artifact_id)
                and int(args.artifact_id) < 2**64, "ci_registration_artifact_id")
        digest = args.artifact_sha256.removeprefix("sha256:")
        require(re.fullmatch(r"[0-9a-f]{64}", digest), "ci_registration_artifact_digest")
        runner = Path(os.environ.get("RUNNER_TEMP", ""))
        require(runner.is_absolute() and runner.is_dir(), "runner_temp")
        baseline = baseline_from_environment()
        require((args.phase == "startup") == (args.startup_proof is not None),
                "ci_startup_proof_argument")
        if args.startup_proof is not None:
            runner_path(args.startup_proof)
            require(not os.path.lexists(args.startup_proof), "ci_startup_proof_absent")
        token = os.environ.pop("GH_TOKEN", "")
        key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY", "").encode()
        with tempfile.TemporaryDirectory(prefix="ci-ownership-save-", dir=runner) as temporary:
            root = Path(temporary)
            private_directory(root)
            for name in ("os-home", "tmp"):
                (root / name).mkdir(mode=0o700)
            history = History(baseline, child_environment(root), token)
            del token
            record = history.current(args.artifact_id, digest, args.source, startup=args.phase == "startup")
            control = Control(baseline, key)
            del key
            control.context(startup=args.phase == "startup")
            registered = control.start(record) if args.phase == "startup" else control.register(record)
            if args.phase == "startup":
                save_startup(args.startup_proof, record, registered, baseline, args.source)
            result.update(result="passed", source=args.source, artifact_id=args.artifact_id,
                          phase=args.phase, registration=registered, backup_service_requests=control.calls)
    except OperationError as error:
        result["failure"] = str(error)
    except BaseException:
        result["failure"] = "unexpected_ci_registration_error"
    finally:
        if control is not None:
            control.close()
    summary(result)
    return 0 if result["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
