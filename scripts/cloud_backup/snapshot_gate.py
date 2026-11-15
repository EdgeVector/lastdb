#!/usr/bin/env python3
"""DEV round trip and native cutover entrypoint for owner signed-database snapshots.

Trusted GitHub Actions on main runs this. Pull requests do not. Records are
generated from a public label, and the script never prints an API key, recovery
phrase, or invite code.
"""

from __future__ import annotations

import http.client
import json
import os
from pathlib import Path
import re
import sys
import tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))

from programs import Programs  # noqa: E402
from snapshot_attempt_account import provision, retire  # noqa: E402
from snapshot_cleanup import RunAttempt, current_attempt, load_owner  # noqa: E402
from snapshot_gate_aws import cleanup, load_guard, require_native_dispatch, summary  # noqa: E402
from snapshot_gate_cutoff import run_native_cutoff  # noqa: E402
from snapshot_gate_roundtrip import run_gate  # noqa: E402
from support import OperationError, child_environment, decode_json, dev_endpoint, failure, require  # noqa: E402

REPOSITORY = "EdgeVector/lastdb"
WORKFLOW = ".github/workflows/snapshot-roundtrip-dev.yml"
PROOF_STEPS = {
    "snapshot-roundtrip-dev": "Seed, snapshot, and restore the gate database",
    "snapshot-roundtrip-published-nightly": "Seed, snapshot, and restore the gate database",
    "snapshot-native-cutoff-dev": "Recover a lagging device across the native snapshot cutoff",
}
DISPOSABLE_PROOF_STEPS = {
    "snapshot-import-dev": "Seed, snapshot, and restore the gate database",
    "snapshot-import-published-nightly": "Seed, snapshot, and restore the gate database",
    "snapshot-native-cutoff-operation-dev": "Recover a lagging device across the native snapshot cutoff",
}
MAX_REPLY_BYTES = 2_000_000


def _github_json(path, token):
    connection = http.client.HTTPSConnection("api.github.com", timeout=20)
    try:
        connection.request("GET", path, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "LastDB-snapshot-cleanup",
            "X-GitHub-Api-Version": "2022-11-28",
        })
        response = connection.getresponse()
        require(response.status == 200, "snapshot_resume_github_status")
        body = response.read(MAX_REPLY_BYTES + 1)
    except (OSError, http.client.HTTPException):
        raise OperationError("snapshot_resume_github_transport") from None
    finally:
        connection.close()
    require(len(body) <= MAX_REPLY_BYTES, "snapshot_resume_github_size")
    parsed = decode_json(body)
    require(isinstance(parsed, dict), "snapshot_resume_github_reply")
    return parsed


def _proof_result(reply, target):
    jobs = reply.get("jobs")
    require(isinstance(jobs, list) and len(jobs) == reply.get("total_count") and len(jobs) <= 50,
            "snapshot_resume_original_jobs")
    proofs = [job for job in jobs if isinstance(job, dict) and job.get("name") in DISPOSABLE_PROOF_STEPS]
    disposable = bool(proofs)
    names = DISPOSABLE_PROOF_STEPS if disposable else PROOF_STEPS
    if not disposable:
        proofs = [job for job in jobs if isinstance(job, dict) and job.get("name") in names]
    require(len(proofs) == 1, "snapshot_resume_original_proof")
    job = proofs[0]
    require(job.get("run_id") == int(target.run_id) and job.get("run_attempt") == int(target.attempt)
            and job.get("status") == "completed" and isinstance(job.get("steps"), list),
            "snapshot_resume_original_job_scope")
    steps = [step for step in job["steps"]
             if isinstance(step, dict) and step.get("name") == names[job["name"]]]
    require(len(steps) <= 1, "snapshot_resume_original_proof_step")
    return (steps[0].get("conclusion") if steps else None), disposable


def verified_attempt(run_id, attempt_number):
    require(os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{WORKFLOW}@refs/heads/main",
            "snapshot_resume_protected_workflow")
    target = RunAttempt(run_id, attempt_number)
    token = os.environ.pop("SNAPSHOT_GITHUB_TOKEN", "")
    require(isinstance(token, str) and re.fullmatch(r"[!-~]{1,1024}", token),
            "snapshot_resume_github_token")
    path = f"/repos/{REPOSITORY}/actions/runs/{target.run_id}/attempts/{target.attempt}"
    run = _github_json(path, token)
    require(run.get("id") == int(target.run_id)
            and run.get("run_attempt") == int(target.attempt)
            and run.get("path") == WORKFLOW
            and run.get("head_branch") == "main"
            and run.get("event") in ("push", "schedule", "workflow_dispatch")
            and isinstance(run.get("repository"), dict) and run["repository"].get("full_name") == REPOSITORY
            and isinstance(run.get("head_repository"), dict)
            and run["head_repository"].get("full_name") == REPOSITORY,
            "snapshot_resume_original_scope")
    require(run.get("status") == "completed" and bool(run.get("conclusion")),
            "snapshot_resume_original_not_terminal")
    revision = run.get("head_sha")
    require(isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision),
            "snapshot_resume_original_revision")
    proof, disposable = _proof_result(_github_json(f"{path}/jobs?per_page=50", token), target)
    return target, revision, proof, disposable


def resume_cleanup(command):
    require(len(sys.argv) == 4, "arguments")
    attempt, revision, proof, disposable = verified_attempt(sys.argv[2], sys.argv[3])
    summary(
        f"original_run={attempt.run_id} original_attempt={attempt.attempt} "
        f"original_source={revision} database_id={attempt.database}"
    )
    if command == "resume-legacy-cleanup":
        require(not disposable, "snapshot_resume_disposable_attempt")
        cleanup(load_guard(), attempt, load_owner(), expect_charge=proof == "success", resume=True,
                proof_skipped=proof == "skipped")
    elif disposable:
        retire(attempt, summary, expect_charge=proof == "success")
    else:
        output = os.environ.get("GITHUB_OUTPUT")
        require(output, "snapshot_resume_legacy_output")
        with open(output, "a", encoding="ascii") as stream:
            stream.write("legacy=true\n")


def main():
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    native = command == "native-cutoff"
    resume = command in ("resume-cleanup", "resume-legacy-cleanup")
    if native or resume:
        require_native_dispatch()
    require(command in ("run", "retire") or native or resume, "command")
    require(
        os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REF") == "refs/heads/main",
        "accepted_main_only",
    )
    os.umask(0o077)
    dev_endpoint()
    if resume:
        resume_cleanup(command)
        return
    if command == "retire":
        require(len(sys.argv) == 2, "arguments")
        attempt = current_attempt()
        proof_attempt = os.environ.get("SNAPSHOT_PROOF_ATTEMPT", "")
        require(not proof_attempt or proof_attempt == attempt.attempt, "snapshot_retire_rerun_all_jobs")
        expect_charge = os.environ.get("REQUIRE_RELEASED_SNAPSHOT_BYTES") == "true"
        require(not expect_charge or proof_attempt == attempt.attempt, "snapshot_retire_proof_attempt")
        retire(attempt, summary, expect_charge=expect_charge)
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
        guard = load_guard()
        attempt = current_attempt()
        owner = provision(programs, root, attempt)
        summary(f"snapshot_account=provisioned account_id={owner.account} database_id={attempt.database}")
        if native:
            run_native_cutoff(programs, root, guard, owner, require_automatic=require_automatic)
        else:
            run_gate(programs, root, guard, owner)


if __name__ == "__main__":
    try:
        main()
    except OperationError as error:
        summary("snapshot gate failed: " + json.dumps(failure(error), sort_keys=True))
        sys.exit(1)
