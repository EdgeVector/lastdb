#!/usr/bin/env python3
"""Prepare the exact main CI stream claim before any native backup allocation."""

import argparse
import json
import os
from pathlib import Path
import re
import sys
import uuid

sys.dont_write_bytecode = True

from ownership import run_identity
from support import (OperationError, baseline_scope_from_environment, private_directory,
                     require, save_json)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        require(re.fullmatch(r"[0-9a-f]{40}", args.source), "ci_start_source")
        owner = run_identity(args.source)
        baseline = baseline_scope_from_environment()
        runner = Path(os.environ.get("RUNNER_TEMP", ""))
        require(runner.is_absolute() and runner.is_dir(), "runner_temp")
        output = args.output
        require(output.is_absolute() and output.parent.resolve() == runner.resolve()
                and re.fullmatch(r"[A-Za-z0-9._-]{1,80}", output.name)
                and not os.path.lexists(output), "ci_start_owned_output")
        output.mkdir(mode=0o700)
        private_directory(output)
        stream = str(uuid.uuid4())
        require(stream != baseline["stream_id"], "ci_start_excludes_baseline")
        receipt = {"schema": "lastdb.cloud-ci-start.v1", "owner": owner,
                   "account": baseline["account"], "account_generation": baseline["account_generation"],
                   "stream_id": stream}
        save_json(output / "ci-start.json", receipt)
        command_output = Path(os.environ.get("GITHUB_OUTPUT", ""))
        require(command_output.is_absolute()
                and command_output.parent.resolve().is_relative_to(runner.resolve()),
                "ci_start_github_output")
        descriptor = os.open(command_output, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "w") as target:
            target.write("stream_id=" + stream + "\n")
        print(json.dumps({"result": "prepared", "stream_id": stream,
                          "provider_deletions": 0, "primary_access": False}, sort_keys=True))
        return 0
    except OperationError as error:
        print(json.dumps({"result": "failed", "failure": str(error)}, sort_keys=True))
    except BaseException:
        print('{"result":"failed","failure":"unexpected_ci_start_preparation_error"}')
    return 1


if __name__ == "__main__":
    sys.exit(main())
