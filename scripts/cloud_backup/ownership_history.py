"""Read bounded, verified ownership inputs from successful main CI runs.

This directory supplies only explicit candidate ownership. Missing, expired or
unseen records never imply ownership or permission to delete cloud data.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import io
from pathlib import Path
import re
import shutil
import time
import zipfile
import zlib

from ownership import REPOSITORY, WORKFLOW, run_identity
from programs import execute
from recovery_recipe import validate as validate_recipe
from support import ACCOUNT, OperationError, canonical_uuid, decode_json, require

MAX_RUNS = 100
MAX_ZIP = 32768
MAX_RECEIPT = 16384


def number(value):
    require(type(value) is int and 0 < value < 2**64, "ci_history_integer")
    return value


def point(value, stream):
    require(isinstance(value, dict) and set(value) - {"recipe"} == {
        "stream_id", "commit_id", "commit_sha256"}, "ci_history_point_fields")
    require(value["stream_id"] == stream, "ci_history_point_stream")
    canonical_uuid(value["commit_id"])
    require(value["commit_id"].startswith("a17dbac2-")
            and isinstance(value["commit_sha256"], str)
            and re.fullmatch(r"[0-9a-f]{64}", value["commit_sha256"]), "ci_history_point_identity")


def ownership(raw, run, baseline):
    value = decode_json(raw)
    require(isinstance(value, dict) and set(value) == {
        "schema", "owner", "account", "account_generation", "stream_id",
        "observed_accepted_points", "verified_recovery_points", "source_stopped",
        "log_and_snapshot_recovery_passed", "complete_frozen_directory",
        "provider_deletion_authority"}, "ci_history_receipt_fields")
    require(value["schema"] == "lastdb.cloud-ci-stream.v1" and value["owner"] == {
        "repository": REPOSITORY, "workflow": WORKFLOW, "source": run["head_sha"],
        "run_id": str(run["id"]), "run_attempt": str(run["run_attempt"])}, "ci_history_exact_owner")
    require(value["account"] == ACCOUNT and baseline["account"] == ACCOUNT
            and value["account_generation"] == baseline["account_generation"], "ci_history_dev_scope")
    stream = canonical_uuid(value["stream_id"])
    require(stream != baseline["stream_id"], "ci_history_preserves_baseline")
    require(value["source_stopped"] is True and value["log_and_snapshot_recovery_passed"] is True
            and value["complete_frozen_directory"] is False
            and value["provider_deletion_authority"] is False, "ci_history_proof_boundary")
    observed, recovered = value["observed_accepted_points"], value["verified_recovery_points"]
    require(isinstance(observed, list) and 2 <= len(observed) <= 8
            and isinstance(recovered, list) and 2 <= len(recovered) <= 8, "ci_history_point_bound")
    for selected in observed + recovered:
        point(selected, stream)
    require(len({item["commit_id"] for item in observed}) == len(observed)
            and len({item["commit_id"] for item in recovered}) == len(recovered)
            and all(item in observed for item in recovered), "ci_history_point_closure")
    if "recipe" in value:
        validate_recipe(value["recipe"], recovered, value["owner"]["source"])
    return value


def startup_ownership(raw, run, baseline):
    value = decode_json(raw)
    require(isinstance(value, dict) and set(value) == {
        "schema", "owner", "account", "account_generation", "stream_id"}, "ci_start_receipt_fields")
    require(value["schema"] == "lastdb.cloud-ci-start.v1" and value["owner"] == {
        "repository": REPOSITORY, "workflow": WORKFLOW, "source": run["head_sha"],
        "run_id": str(run["id"]), "run_attempt": str(run["run_attempt"])}, "ci_start_exact_owner")
    require(value["account"] == baseline["account"] == ACCOUNT
            and value["account_generation"] == baseline["account_generation"], "ci_start_dev_scope")
    require(canonical_uuid(value["stream_id"]) != baseline["stream_id"], "ci_start_preserves_baseline")
    return value


def unzip(raw, digest, filename="ci-stream.json"):
    require(0 < len(raw) <= MAX_ZIP and hashlib.sha256(raw).hexdigest() == digest,
            "ci_history_artifact_digest")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        require(len(entries) == 1, "ci_history_artifact_single_file")
        entry = entries[0]
        require(entry.filename == filename and not entry.is_dir()
                and 0 < entry.file_size <= MAX_RECEIPT and entry.compress_size <= MAX_ZIP
                and not entry.flag_bits & 1 and ((entry.external_attr >> 16) & 0o170000) != 0o120000,
                "ci_history_artifact_entry")
        raw = archive.read(entry)
        require(len(raw) == entry.file_size, "ci_history_artifact_length")
        return raw


class History:
    def __init__(self, baseline, environment, token):
        require(isinstance(token, str) and 0 < len(token) <= 4096, "ci_history_read_token")
        binary = shutil.which("gh", path=environment["PATH"])
        require(binary is not None and Path(binary).is_absolute(), "ci_history_github_cli")
        self.binary, self.baseline = binary, baseline
        self.environment = {key: environment[key] for key in ("PATH", "HOME", "TMPDIR", "LANG")}
        self.environment.update(GH_TOKEN=token, GH_HOST="github.com", GH_PROMPT_DISABLED="1",
                                GH_NO_UPDATE_NOTIFIER="1", NO_COLOR="1")
        self.deadline = time.monotonic() + 300

    def api(self, suffix, binary=False):
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "ci_history_deadline")
        try:
            raw = execute([self.binary, "api", "--hostname", "github.com",
                           f"repos/{REPOSITORY}/" + suffix], self.environment,
                          timeout=min(30, remaining))
        except OperationError as error:
            raise OperationError("ci_history_github_read_failed") from error
        if binary:
            return raw
        try:
            return decode_json(raw)
        except OperationError as error:
            raise OperationError("ci_history_github_response_invalid") from error

    def candidate(self, run, finished=True, startup=False):
        number(run["id"])
        number(run["run_attempt"])
        require(run["repository"]["full_name"] == run["head_repository"]["full_name"] == REPOSITORY
                and run["path"] == WORKFLOW and run["head_branch"] == "main"
                and run["event"] in ("push", "workflow_dispatch")
                and re.fullmatch(r"[0-9a-f]{40}", run["head_sha"]), "ci_history_trusted_run")
        require((run["status"] == "completed" and run["conclusion"] == "success") if finished
                else run["status"] in ("queued", "in_progress", "completed"), "ci_history_run_state")
        prefix = "cloud-ci-start" if startup else "cloud-ci-stream"
        expected = f"{prefix}-{run['id']}-{run['run_attempt']}"
        directory = self.api(f"actions/runs/{run['id']}/artifacts?per_page=100")
        require(type(directory["total_count"]) is int and 0 <= directory["total_count"] <= 100
                and len(directory["artifacts"]) == directory["total_count"], "ci_history_artifact_directory")
        selected = [item for item in directory["artifacts"] if item["name"] == expected]
        require(len(selected) <= 1, "ci_history_unique_artifact")
        if not selected or selected[0]["expired"] is True:
            return None
        artifact = selected[0]
        require(artifact["expired"] is False and 0 < number(artifact["size_in_bytes"]) <= MAX_ZIP
                and artifact["workflow_run"]["id"] == run["id"]
                and artifact["workflow_run"]["head_sha"] == run["head_sha"]
                and artifact["workflow_run"]["head_branch"] == "main", "ci_history_artifact_origin")
        digest = artifact["digest"]
        require(isinstance(digest, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", digest),
                "ci_history_artifact_hash")
        identifier = number(artifact["id"])
        raw = self.api(f"actions/artifacts/{identifier}/zip", binary=True)
        receipt_raw = unzip(raw, digest[7:], "ci-start.json" if startup else "ci-stream.json")
        receipt = (startup_ownership if startup else ownership)(receipt_raw, run, self.baseline)
        completed = self.completion(run) if finished else None
        return {"ownership": receipt, "artifact_id": str(identifier), "artifact_sha256": digest[7:],
                "archive": raw, "receipt_sha256": hashlib.sha256(receipt_raw).hexdigest(),
                "artifact_url": f"https://github.com/{REPOSITORY}/actions/runs/{run['id']}/artifacts/{identifier}",
                "artifact_expires_at": artifact["expires_at"], "completed_at": completed}

    def current(self, identifier, digest, source, startup=False):
        try:
            owner = run_identity(source)
            run = self.api(f"actions/runs/{owner['run_id']}/attempts/{owner['run_attempt']}")
            require(str(run["id"]) == owner["run_id"]
                    and str(run["run_attempt"]) == owner["run_attempt"]
                    and run["head_sha"] == source, "ci_current_run_identity")
            record = self.candidate(run, finished=False, startup=startup)
            require(record is not None and record["artifact_id"] == identifier
                    and record["artifact_sha256"] == digest, "ci_current_artifact_identity")
            return record
        finally:
            self.environment.pop("GH_TOKEN", None)

    def completion(self, run):
        directory = self.api(f"actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs?per_page=100")
        require(type(directory["total_count"]) is int and 0 < directory["total_count"] <= 100
                and len(directory["jobs"]) == directory["total_count"], "ci_history_job_directory")
        selected = [job for job in directory["jobs"] if job["name"] == "Automatic DEV backup and recovery"]
        require(len(selected) == 1, "ci_history_unique_recovery_job")
        job = selected[0]
        require(job["run_id"] == run["id"] and job["run_attempt"] == run["run_attempt"]
                and job["head_sha"] == run["head_sha"] and job["status"] == "completed"
                and job["conclusion"] == "success" and isinstance(job["completed_at"], str),
                "ci_history_successful_recovery_job")
        # Exact-attempt completion orders reruns correctly; run creation IDs do not.
        datetime.strptime(job["completed_at"], "%Y-%m-%dT%H:%M:%SZ")
        return job["completed_at"]

    def runs(self, known_attempts, limit, event):
        require(len(known_attempts) <= 10000 and all(isinstance(value, str)
                and re.fullmatch(r"[1-9][0-9]{0,19}/[1-9][0-9]{0,19}", value)
                for value in known_attempts), "ci_history_known_attempt_bound")
        require(type(limit) is int and 1 <= limit <= MAX_RUNS, "ci_history_requested_run_bound")
        require(event in (None, "push", "workflow_dispatch"), "ci_history_event")
        runs, seen = [], set()
        page_size = min(50, limit)
        for page in range(1, (limit + page_size - 1) // page_size + 1):
            suffix = f"?branch=main&status=success&per_page={page_size}&page={page}"
            if event is not None:
                suffix += "&event=" + event
            value = self.api("actions/workflows/cloud-backup-recovery.yml/runs" + suffix)
            entries = value["workflow_runs"]
            require(isinstance(entries, list) and len(entries) <= page_size, "ci_history_run_page")
            for run in entries[:limit - len(seen)]:
                identifier = number(run["id"])
                attempt = number(run["run_attempt"])
                require(identifier not in seen, "ci_history_duplicate_run")
                seen.add(identifier)
                if (run["event"] in ("push", "workflow_dispatch")
                        and f"{identifier}/{attempt}" not in known_attempts):
                    runs.append(run)
            require(len(seen) <= limit, "ci_history_run_bound")
            if len(seen) == limit or len(entries) < page_size:
                break
        return runs

    def candidate_or_unknown(self, run):
        try:
            return self.candidate(run), False
        except OperationError as error:
            if str(error) in ("ci_history_github_read_failed", "ci_history_deadline",
                              "ci_history_github_response_invalid"):
                raise
            return None, True
        except (KeyError, TypeError, ValueError, OverflowError, RuntimeError,
                NotImplementedError, zipfile.BadZipFile, zlib.error):
            # Unverifiable history never grants ownership or deletion authority.
            return None, True

    def collect(self, known_attempts=(), limit=100, event=None):
        try:
            runs = self.runs(known_attempts, limit, event)
            with ThreadPoolExecutor(max_workers=4) as pool:
                values = list(pool.map(self.candidate_or_unknown, runs))
            records = [value for value, _invalid in values if value is not None]
            self.unknown_records = len(values) - len(records)
            self.invalid_records = sum(invalid for _value, invalid in values)
            return records
        finally:
            self.environment.pop("GH_TOKEN", None)
