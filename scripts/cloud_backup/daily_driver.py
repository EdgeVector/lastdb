#!/usr/bin/env python3
"""Dispatch the existing main DEV operation only after a bounded freshness check."""

import json
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

REPOSITORY = "EdgeVector/lastdb"
TARGET = "cloud-backup-recovery.yml"
WORKFLOW = ".github/workflows/" + TARGET
DRIVER = ".github/workflows/cloud-backup-daily.yml"
RECOVERY_JOB = "Automatic DEV backup and recovery"
ACTIVE = ("requested", "queued", "pending", "waiting", "in_progress")
PER_PAGE, MAX_PAGES, MAX_CALLS = 100, 10, 80
MAX_BODY = 2 * 1024 * 1024
STEPS = ("Build the exact accepted client", "Package the exact client",
         "Run automatic snapshots, mutation logs, restart and both recoveries",
         "Retain successful CI stream ownership", "Save durable CI ownership before this run ends")


class Refused(RuntimeError):
    pass


def require(condition, code):
    if not condition:
        raise Refused(code)


def integer(value):
    require(type(value) is int and 0 <= value <= 2**64 - 1, "invalid_api_integer")
    return value


def timestamp(value):
    require(isinstance(value, str), "invalid_api_time")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        raise Refused("invalid_api_time") from None


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "duplicate_api_json_key")
        result[key] = value
    return result


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        raise Refused("github_redirect_refused")


class GitHub:
    def __init__(self, token):
        require(0 < len(token) <= 4096 and token.strip() == token
                and all(32 < ord(character) < 127 for character in token), "github_token_missing_or_invalid")
        self.token, self.calls = token, 0
        self.deadline = time.monotonic() + 180
        self.opener = build_opener(NoRedirect())

    def call(self, path, body=None):
        require(path.startswith("actions/") and ".." not in path and not path.startswith("//"), "fixed_github_api_path")
        self.calls += 1
        remaining = self.deadline - time.monotonic()
        require(self.calls <= MAX_CALLS and remaining > 0, "github_request_budget_exhausted")
        if body is not None:
            require(path == f"actions/workflows/{TARGET}/dispatches" and body == {"ref": "main"},
                    "only_existing_main_dispatch_is_permitted")
        request = Request(f"https://api.github.com/repos/{REPOSITORY}/{path}",
            data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + self.token, "Accept": "application/vnd.github+json",
                     "X-GitHub-Api-Version": "2026-03-10", "Content-Type": "application/json"},
            method="GET" if body is None else "POST")
        try:
            with self.opener.open(request, timeout=min(15, remaining)) as response:
                require(response.status == 200, "unexpected_github_status")
                raw = response.read(MAX_BODY + 1)
                require(len(raw) <= MAX_BODY and time.monotonic() < self.deadline, "github_response_budget_exhausted")
        except HTTPError as error:
            raise Refused("github_http_" + str(error.code)) from None
        except (URLError, TimeoutError, OSError):
            raise Refused("github_transport_outcome_unknown") from None
        try:
            value = json.loads(raw, object_pairs_hook=unique,
                               parse_constant=lambda _value: require(False, "nonfinite_api_json"))
        except (UnicodeError, ValueError):
            raise Refused("invalid_github_json") from None
        require(isinstance(value, dict), "invalid_github_object")
        return value

    def pages(self, path, key, parameters, observation=None):
        expected, seen = None, set()
        if observation is not None:
            observation.update(directory_complete=False, history_limit=PER_PAGE * MAX_PAGES)
        for page in range(1, MAX_PAGES + 1):
            value = self.call(path + "?" + urlencode({**parameters, "per_page": PER_PAGE, "page": page}))
            total, rows = integer(value.get("total_count")), value.get(key)
            require(isinstance(rows, list) and len(rows) <= PER_PAGE, "invalid_github_page")
            require(expected is None or total == expected, "github_directory_changed")
            expected = total
            if observation is not None:
                observation["reported_total"] = total
            require(len(rows) == min(PER_PAGE, max(0, total - len(seen))), "incomplete_github_page")
            for row in rows:
                require(isinstance(row, dict) and integer(row.get("id")) > 0 and row["id"] not in seen,
                        "duplicate_or_invalid_github_item")
                seen.add(row["id"])
                yield row
            if len(seen) == total:
                if observation is not None:
                    observation["directory_complete"] = True
                return
        if observation is None:
            raise Refused("active_or_job_directory_bound_requires_owner_review")


def trusted(run):
    if run.get("event") == "pull_request":
        return False  # This job has no DEV credentials and cannot be a real recovery.
    require(run.get("event") in ("push", "workflow_dispatch") and run.get("head_branch") == "main"
            and run.get("path") == WORKFLOW
            and run.get("repository", {}).get("full_name") == REPOSITORY
            and run.get("head_repository", {}).get("full_name") == REPOSITORY
            and isinstance(run.get("head_sha"), str) and re.fullmatch("[0-9a-f]{40}", run["head_sha"])
            and integer(run.get("run_attempt")) > 0, "unknown_or_untrusted_recovery_run")
    return True


def active(api):
    for state in ACTIVE:
        for run in api.pages(f"actions/workflows/{TARGET}/runs", "workflow_runs",
                             {"branch": "main", "status": state}):
            if trusted(run):
                require(run.get("status") == state, "active_run_status_changed")
                return {"run_id": run["id"], "attempt": run["run_attempt"], "status": state}
    return None


def successful_job(api, run, now):
    jobs = list(api.pages(f"actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs", "jobs", {}))
    matches = [job for job in jobs if job.get("name") == RECOVERY_JOB]
    require(len(matches) == 1, "missing_or_duplicate_real_recovery_job")
    job = matches[0]
    require(job.get("run_id") == run["id"] and job.get("run_attempt") == run["run_attempt"]
            and job.get("head_sha") == run["head_sha"] and job.get("status") == "completed",
            "real_recovery_job_identity_or_state_changed")
    if job.get("conclusion") == "skipped":
        return None
    require(job.get("conclusion") == "success", "successful_run_has_failed_recovery_job")
    steps = job.get("steps")
    require(isinstance(steps, list) and len(steps) <= 100, "invalid_real_recovery_steps")
    for name in STEPS:
        match = [step for step in steps if step.get("name") == name]
        require(len(match) == 1 and match[0].get("status") == "completed"
                and match[0].get("conclusion") == "success", "real_recovery_step_not_successful")
    finished = timestamp(job.get("completed_at"))
    require(timestamp(job.get("started_at")) <= finished <= timestamp(run.get("updated_at"))
            and finished <= now + timedelta(minutes=5), "inconsistent_recovery_completion_time")
    return {"run_id": run["id"], "attempt": run["run_attempt"], "job_id": job["id"],
            "source": run["head_sha"], "completed_at": job["completed_at"],
            "url": f"https://github.com/{REPOSITORY}/actions/runs/{run['id']}"}


def fresh(api, now):
    cutoff = now - timedelta(hours=24)
    examined = {"scope": "bounded_successful_main_history", "runs_examined": 0,
                "jobs_examined": 0, "cutoff": cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")}
    for run in api.pages(f"actions/workflows/{TARGET}/runs", "workflow_runs",
                         {"branch": "main", "status": "success"}, examined):
        examined["runs_examined"] += 1
        if not trusted(run):
            continue
        require(run.get("status") == "completed" and run.get("conclusion") == "success", "successful_run_state_changed")
        # Creation time cannot exclude an old run with a recent successful rerun.
        # updated_at is only an upper bound; exact-attempt job completion decides freshness.
        updated = timestamp(run.get("updated_at"))
        require(updated <= now + timedelta(minutes=5), "future_recovery_run_time")
        if updated < cutoff:
            continue
        examined["jobs_examined"] += 1
        result = successful_job(api, run, now)
        if result is not None and timestamp(result["completed_at"]) >= cutoff:
            return {"disposition": "fresh_real_recovery", "last_success": result, "freshness": examined}
    return {"disposition": "no_fresh_proof_in_bounded_search", "freshness": examined}


def inspect(api, now):
    current = active(api)
    if current is not None:
        return {"disposition": "recovery_already_active", "active": current}
    return fresh(api, now)


def context():
    require(os.environ.get("GITHUB_ACTIONS") == "true" and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
            and os.environ.get("GITHUB_REF") == "refs/heads/main"
            and os.environ.get("GITHUB_EVENT_NAME") in ("schedule", "workflow_dispatch")
            and os.environ.get("GITHUB_WORKFLOW_REF") == f"{REPOSITORY}/{DRIVER}@refs/heads/main"
            and re.fullmatch("[0-9a-f]{40}", os.environ.get("GITHUB_SHA", "")), "trusted_default_branch_driver_only")
    require(not any(os.environ.get(name) for name in ("LASTDB_CLOUD_DEV_API_KEY", "LASTDB_CLOUD_DEV_RECOVERY_PHRASE",
            "LASTDB_CLOUD_DEV_BASELINE", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")),
            "driver_must_not_receive_cloud_credentials")


def summary(value):
    raw = json.dumps(value, sort_keys=True)
    require(len(raw.encode()) <= 8192, "bounded_daily_summary")
    print(raw)
    destination = os.environ.get("GITHUB_STEP_SUMMARY")
    if destination:
        path, runner = Path(destination), Path(os.environ["RUNNER_TEMP"]).resolve()
        require(path.is_absolute() and path.parent.resolve().is_relative_to(runner), "daily_summary_path")
        descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "w") as stream:
            stream.write("\n### Daily cloud recovery check\n\n```json\n" + raw + "\n```\n")


def main():
    api = None
    result = {"status": "failed", "dispatch_attempted": False, "dispatch_outcome": "not_attempted",
              "target": WORKFLOW, "target_ref": "main", "cloud_credentials": False,
              "cloud_recovery_proven_by_driver": False}
    try:
        context()
        api = GitHub(os.environ.pop("GH_TOKEN", ""))
        now = datetime.now(timezone.utc)
        workflow = api.call(f"actions/workflows/{TARGET}")
        require(workflow.get("path") == WORKFLOW and workflow.get("state") == "active", "target_workflow_unavailable")
        decision = inspect(api, now)
        if decision["disposition"] == "no_fresh_proof_in_bounded_search":
            # Recheck immediately before the sole dispatch. GitHub offers no atomic
            # check-and-dispatch operation; the target retains its serialized lane.
            decision = inspect(api, datetime.now(timezone.utc))
        if decision["disposition"] == "no_fresh_proof_in_bounded_search":
            result.update(dispatch_attempted=True, dispatch_outcome="unknown", freshness=decision["freshness"],
                          dispatch_rationale="no_positive_fresh_proof_in_bounded_search")
            dispatched = api.call(f"actions/workflows/{TARGET}/dispatches", {"ref": "main"})
            run_id = integer(dispatched.get("workflow_run_id"))
            require(run_id > 0 and dispatched.get("run_url") == f"https://api.github.com/repos/{REPOSITORY}/actions/runs/{run_id}"
                    and dispatched.get("html_url") == f"https://github.com/{REPOSITORY}/actions/runs/{run_id}",
                    "unknown_dispatch_receipt")
            result.update(disposition="dispatch_accepted", dispatch_outcome="accepted",
                          dispatched_run_id=run_id, dispatched_run_url=dispatched["html_url"])
        else:
            result.update(decision)
        result["status"] = "passed"
    except Refused as error:
        result["failure"] = str(error)
    except BaseException:
        result["failure"] = "unexpected_daily_driver_error"
    finally:
        os.environ.pop("GH_TOKEN", None)
        if api is not None:
            api.token = ""
            result["github_calls"] = api.calls
        summary(result)
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
