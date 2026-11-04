#!/usr/bin/env python3
"""Point the two protected baseline inputs at a moved CI baseline.

A baseline move changes the service's CI baseline, but the protected inputs
`LASTDB_CLOUD_DEV_BASELINE` (environment secret) and
`LASTDB_CLOUD_DEV_BASELINE_SCOPE` (variable) still name the replaced one. Every
normal recovery then fails at its start claim with
`ci_control_service_integrity_mismatch`. `GITHUB_TOKEN` cannot write Actions
secrets or variables, so the manual baseline mode runs this command with
`GH_PAT` right after the service move passes. An operator runs the same command
when that step fails.

The command derives the exact secret from the recovery phrase and the public
point of `ci-baseline.json`, and requires the run's public commitment
(`secret_sha256`) before any write. The secret goes to `gh` on stdin. The
command requires explicit `GH_TOKEN` and repeats the metadata-only credential
preflight before it reads the phrase. The output holds public identifiers and
value-free diagnostics only. This module stays compatible with Python 3.9.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True

import baseline_data
import baseline_writer
from support import ACCOUNT, OperationError, canonical_uuid, decode_json, failure, require

RECEIPT = "lastdb.cloud-ci-baseline.v1"
ENVIRONMENT = "cloud-backup-dev"
SECRET = "LASTDB_CLOUD_DEV_BASELINE"
VARIABLE = "LASTDB_CLOUD_DEV_BASELINE_SCOPE"
PHRASE = "LASTDB_CLOUD_DEV_RECOVERY_PHRASE"
DIGEST = re.compile(r"[0-9a-f]{64}")


def load(path, limit):
    require(path.is_file() and path.stat().st_size <= limit, "baseline_inputs_file")
    return decode_json(path.read_bytes())


def receipt_point(path):
    value = load(path, 65536)
    require(isinstance(value, dict) and value.get("schema") == RECEIPT
            and value.get("account") == ACCOUNT, "baseline_inputs_receipt")
    require(all(value.get(name) is True for name in (
        "source_stopped", "restored_to_absent_destination", "restored_reads_verified")),
        "baseline_inputs_receipt_unverified")
    generation = value.get("account_generation")
    require(isinstance(generation, str) and re.fullmatch(r"0|[1-9][0-9]{0,19}", generation),
            "baseline_inputs_generation")
    point = value.get("baseline")
    require(isinstance(point, dict) and set(point) == {"stream_id", "commit_id", "commit_sha256"},
            "baseline_inputs_point")
    canonical_uuid(point["stream_id"])
    canonical_uuid(point["commit_id"])
    require(isinstance(point["commit_sha256"], str) and DIGEST.fullmatch(point["commit_sha256"]),
            "baseline_inputs_point")
    return generation, point


def summary_commitment(path, generation, point):
    """The commitment of `baseline.py`'s own summary for exactly this point."""
    value = load(path, 65536)
    require(isinstance(value, dict) and value.get("result") == "passed", "baseline_inputs_summary")
    new = value.get("new_baseline")
    require(isinstance(new, dict) and new.get("point") == point and new.get("scope") == {
        "account": ACCOUNT, "account_generation": generation, "stream_id": point["stream_id"]},
        "baseline_inputs_summary_point")
    return new.get("secret_sha256")


def gh(arguments):
    """Run a public metadata call with a fixed refusal code, never raw stderr."""
    completed = subprocess.run(["gh"] + arguments, stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL, env=baseline_writer.gh_environment(),
                               timeout=60, check=False)
    require(completed.returncode == 0, "baseline_inputs_gh_" + "_".join(arguments[:2]))
    return completed.stdout


def set_secret(repo, value):
    """Write the secret on stdin. No gh output from this call reaches the log."""
    completed = subprocess.run(["gh", "secret", "set", SECRET, "--env", ENVIRONMENT, "--repo", repo],
                               input=value, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                               env=baseline_writer.gh_environment(), timeout=60, check=False)
    # Metadata reads do not attest writes; this call must succeed separately.
    require(completed.returncode == 0, "baseline_inputs_gh_secret_set")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", required=True, type=Path, help="ci-baseline.json")
    commitment = parser.add_mutually_exclusive_group(required=True)
    commitment.add_argument("--summary", type=Path, help="the baseline step's summary.json")
    commitment.add_argument("--secret-sha256", help="secret_sha256 of the run summary")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "EdgeVector/lastdb"))
    args = parser.parse_args()
    result = {"result": "failed", "primary_access": False, "provider_deletions": 0}
    try:
        require(args.repo == baseline_writer.REPO, "baseline_inputs_repo")
        generation, point = receipt_point(args.receipt)
        expected = (summary_commitment(args.summary, generation, point) if args.summary
                    else args.secret_sha256)
        require(isinstance(expected, str) and DIGEST.fullmatch(expected), "baseline_inputs_commitment")
        # This repeats the workflow's pre-move check before any phrase/key work.
        # A metadata GET cannot attest the later secret or variable write.
        checked = baseline_writer.preflight()
        result["writer_preflight"] = checked
        require(checked["result"] == "passed", checked.get("failure", "baseline_writer_preflight_failed"))
        # CI passes the phrase in the environment; an operator pipes it on stdin.
        phrase = os.environ.pop(PHRASE, "").encode() or sys.stdin.buffer.read()
        try:
            key = baseline_data.content_key(baseline_data.seed(phrase.strip()))
        except baseline_data.BaselineDataError as error:
            raise OperationError(str(error)) from None
        finally:
            del phrase
        stream_id = point["stream_id"]
        file_sha256 = hashlib.sha256(baseline_data.file_bytes(key, stream_id)).hexdigest()
        secret = baseline_data.secret(key, stream_id, generation, point["commit_id"],
                                      point["commit_sha256"], file_sha256)
        del key
        require(baseline_data.commitment(secret) == expected, "baseline_inputs_secret_commitment")
        scope = {"account": ACCOUNT, "account_generation": generation, "stream_id": stream_id}
        scope_text = json.dumps(scope, sort_keys=True, separators=(",", ":"))
        # GitHub stamps whole seconds; the margin covers a small local clock skew.
        started = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=120)
        # Secret first: until the variable follows, the pair refuses as
        # `protected_baseline_matches_public_scope` before any cloud access.
        set_secret(args.repo, baseline_data.encode(secret))
        del secret
        gh(["variable", "set", VARIABLE, "--repo", args.repo, "--body", scope_text])
        saved = gh(["variable", "get", VARIABLE, "--repo", args.repo]).decode().strip()
        require(saved == scope_text, "baseline_inputs_variable_readback")
        listed = decode_json(gh(["secret", "list", "--env", ENVIRONMENT, "--repo", args.repo,
                                 "--json", "name,updatedAt"]))
        updated = [item.get("updatedAt") for item in listed if item.get("name") == SECRET]
        require(len(updated) == 1 and isinstance(updated[0], str), "baseline_inputs_secret_listed")
        when = datetime.strptime(updated[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        require(when >= started, "baseline_inputs_secret_not_updated")
        result.update(result="passed", repo=args.repo, environment=ENVIRONMENT, scope=scope,
                      point=point, secret_sha256=expected, secret_updated_at=updated[0],
                      variable_verified=True)
    except OperationError as error:
        result.update(failure(error))
    except BaseException:
        # Never print an exception: a derivation error can carry key material.
        result["failure"] = "unexpected_baseline_inputs_error"
    print(json.dumps(result, sort_keys=True))
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        with open(destination, "a") as stream:
            stream.write("\n### CI baseline inputs\n\n```json\n"
                         + json.dumps(result, sort_keys=True, indent=2) + "\n```\n")
    return 0 if result["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
