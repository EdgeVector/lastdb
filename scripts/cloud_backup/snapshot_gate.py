#!/usr/bin/env python3
"""DEV round trip and native cutover entrypoint for owner signed-database snapshots.

Trusted GitHub Actions on main runs this. Pull requests do not. Records are
generated from a public label, and the script never prints an API key, recovery
phrase, or invite code.
"""

import json
import os
from pathlib import Path
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

from programs import Programs  # noqa: E402
from snapshot_cleanup import current_attempt, load_owner  # noqa: E402
from snapshot_gate_aws import cleanup, load_guard, require_native_dispatch, summary  # noqa: E402
from snapshot_gate_cutoff import run_native_cutoff  # noqa: E402
from snapshot_gate_roundtrip import run_gate  # noqa: E402
from snapshot_resume import verified_attempt  # noqa: E402
from support import OperationError, child_environment, dev_endpoint, failure, require  # noqa: E402


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    native = command in ("native-cutoff", "cleanup-native-cutoff")
    resume = command == "resume-cleanup"
    if native or resume:
        require_native_dispatch()
    require(command in ("run", "cleanup") or native or resume, "command")
    require(
        os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main",
        "accepted_main_only",
    )
    os.umask(0o077)
    dev_endpoint()
    guard = load_guard()
    owner = load_owner()
    if resume:
        require(len(sys.argv) == 4, "arguments")
        attempt, revision, proof = verified_attempt(sys.argv[2], sys.argv[3])
        summary(
            f"original_run={attempt.run_id} original_attempt={attempt.attempt} "
            f"original_source={revision} database_id={attempt.database}"
        )
        cleanup(guard, attempt, owner, expect_charge=proof == "success", resume=True, proof_skipped=proof == "skipped")
        return
    if command in ("cleanup", "cleanup-native-cutoff"):
        require(len(sys.argv) == 2, "arguments")
        cleanup(
            guard,
            current_attempt(),
            owner,
            expect_charge=os.environ.get("REQUIRE_RELEASED_SNAPSHOT_BYTES") == "true",
        )
        return
    parser_args = sys.argv[2:]
    require_automatic = native and parser_args[-1:] == ["--require-automatic"]
    if require_automatic:
        parser_args = parser_args[:-1]
    require(
        len(parser_args) == 4 and parser_args[0] == "--bin-dir" and parser_args[2] == "--version",
        "arguments",
    )
    with tempfile.TemporaryDirectory(prefix="ldb-snapshot-", dir="/tmp") as temporary:
        root = Path(temporary)
        for name in ("os-home", "tmp"):
            (root / name).mkdir(mode=0o700)
        programs = Programs(Path(parser_args[1]).resolve(strict=True), parser_args[3], child_environment(root))
        try:
            if native:
                run_native_cutoff(programs, root, guard, owner, require_automatic=require_automatic)
            else:
                run_gate(programs, root, guard, owner)
        except OperationError as error:
            summary("snapshot gate failed: " + json.dumps(failure(error), sort_keys=True))
            raise


if __name__ == "__main__":
    main()
