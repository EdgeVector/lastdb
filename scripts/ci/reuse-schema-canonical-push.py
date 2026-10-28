#!/usr/bin/env python3
"""Reuse the exact branch-push schema suite, or select a fresh merge-tree run."""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass


WORKFLOW = "schema-canonical-properties.yml"
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
REQUIRED_PRODUCER_STEPS = (
    "Run bounded schema properties",
    "Record evidence checksums",
    "Upload schema evidence",
)


class GateError(Exception):
    """A required producer result cannot be accepted."""


@dataclass(frozen=True)
class PushTarget:
    repository: str
    branch: str
    sha: str


def write_output(value):
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
        output.write(f"reused={value}\n")


def git_tree(revision):
    result = subprocess.run(
        ["git", "rev-parse", "--verify", f"{revision}^{{tree}}"],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )
    if result.returncode or not re.fullmatch(r"[0-9a-f]{40,64}", result.stdout.strip()):
        raise GateError("Cannot resolve the candidate source tree; checkout needs full history")
    return result.stdout.strip()


def source_target(event):
    repository = os.environ["GITHUB_REPOSITORY"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise GateError("Invalid repository identity")
    event_name = os.environ["GITHUB_EVENT_NAME"]
    if event_name == "pull_request":
        head = event["pull_request"]["head"]
        if (head.get("repo") or {}).get("full_name") != repository:
            return None, "Fork pull request: execute the isolated merge-candidate suite"
        target = PushTarget(repository, head["ref"], head["sha"])
        validate_target(target)
        if git_tree("HEAD") != git_tree(target.sha):
            return None, "The merge tree differs from the branch tree: execute a fresh suite"
        return target, None
    if event_name == "push":
        ref = os.environ["GITHUB_REF"]
        if event.get("deleted") or not ref.startswith("refs/heads/"):
            raise GateError("Push reuse requires an existing branch revision")
        target = PushTarget(repository, ref.removeprefix("refs/heads/"), os.environ["GITHUB_SHA"])
        validate_target(target)
        return target, None
    return None, "Manual or other event: execute a fresh suite"


def validate_target(target):
    if not target.branch or not re.fullmatch(r"[0-9a-f]{40,64}", target.sha):
        raise GateError("Invalid branch-push source identity")


def api_origin():
    base = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise GateError("The GitHub API must use an HTTPS origin without embedded credentials")
    return base


def api_url(target):
    query = urllib.parse.urlencode(
        {"event": "push", "branch": target.branch, "head_sha": target.sha, "per_page": 100}
    )
    return f"{api_origin()}/repos/{target.repository}/actions/workflows/{WORKFLOW}/runs?{query}"


def read_payload(url, token, deadline, http_timeout):
    for attempt in range(3):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise GateError("Timed out before a branch-push result was available")
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "lastdb-schema-canonical-gate",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=min(http_timeout, remaining)) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise GateError("The workflow-run response exceeded its size bound")
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise GateError("The GitHub API returned no JSON object")
            return payload
        except urllib.error.HTTPError as error:
            if error.code != 429 and not 500 <= error.code < 600:
                raise GateError(f"GitHub workflow lookup failed with HTTP {error.code}") from None
            failure = f"HTTP {error.code}"
        except (urllib.error.URLError, TimeoutError):
            failure = "a network timeout or connection error"
        except (ValueError, UnicodeError):
            raise GateError("The GitHub API returned invalid workflow-run JSON") from None
        if attempt == 2:
            raise GateError(f"GitHub workflow lookup failed after three attempts: {failure}")
        time.sleep(max(0, min(2 ** (attempt + 1), deadline - time.monotonic())))
    raise GateError("No workflow-run response")


def validate_producer_jobs(target, producer, token, deadline, http_timeout):
    query = urllib.parse.urlencode({"filter": "latest", "per_page": 100})
    url = (
        f"{api_origin()}/repos/{target.repository}/actions/runs/"
        f"{producer['id']}/jobs?{query}"
    )
    payload = read_payload(url, token, deadline, http_timeout)
    jobs = payload.get("jobs")
    if payload.get("total_count") != 1 or not isinstance(jobs, list) or len(jobs) != 1:
        raise GateError("The producer must contain exactly one properties job in its latest attempt")
    job = jobs[0]
    if not isinstance(job, dict) or (
        job.get("name") != "properties"
        or job.get("run_id") != producer["id"]
        or job.get("run_attempt") != producer.get("run_attempt")
        or job.get("head_sha") != target.sha
        or job.get("status") != "completed"
        or job.get("conclusion") != "success"
    ):
        raise GateError("The latest producer properties job did not complete successfully for this source")
    steps = job.get("steps")
    if not isinstance(steps, list):
        raise GateError("The producer properties job has no step results")
    for name in REQUIRED_PRODUCER_STEPS:
        matches = [step for step in steps if isinstance(step, dict) and step.get("name") == name]
        if len(matches) != 1 or (
            matches[0].get("status") != "completed"
            or matches[0].get("conclusion") != "success"
        ):
            raise GateError(f"The producer did not complete the required step: {name}")


def newest_producer(runs, target, current_run_id):
    matches = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        run_id = run.get("id")
        if not isinstance(run_id, int) or run_id <= 0 or run_id == current_run_id:
            continue
        if run.get("event") != "push" or run.get("head_sha") != target.sha:
            continue
        if run.get("head_branch") != target.branch:
            continue
        if (run.get("repository") or {}).get("full_name") != target.repository:
            continue
        if (run.get("head_repository") or {}).get("full_name") != target.repository:
            continue
        if run.get("path", "").split("@", 1)[0] != f".github/workflows/{WORKFLOW}":
            continue
        matches.append(run)
    return max(matches, key=lambda run: run["id"], default=None)


def wait_for_producer(target, options):
    token = os.environ.get("GH_TOKEN", "")
    if not token:
        raise GateError("GH_TOKEN with read-only Actions permission is required")
    current_run_id = int(os.environ["GITHUB_RUN_ID"])
    deadline = time.monotonic() + options.wait_seconds
    url = api_url(target)
    observed = None
    started = time.monotonic()
    while time.monotonic() < deadline:
        payload = read_payload(url, token, deadline, options.http_timeout)
        runs = payload.get("workflow_runs")
        if not isinstance(runs, list):
            raise GateError("The GitHub API returned no workflow-run list")
        run = newest_producer(runs, target, current_run_id)
        if run:
            progress = (run["id"], run.get("run_attempt"), run.get("status"), run.get("conclusion"))
            if progress != observed:
                print(f"Branch-push producer {progress[0]}: {progress[2]} / {progress[3]}", flush=True)
                observed = progress
            if run.get("status") == "completed":
                if run.get("conclusion") != "success":
                    raise GateError(
                        f"Newest branch-push run {run['id']} did not succeed: {run.get('conclusion')}"
                    )
                validate_producer_jobs(target, run, token, deadline, options.http_timeout)
                return run
        interval = options.poll_seconds if time.monotonic() - started < 60 else options.max_poll_seconds
        time.sleep(max(0, min(interval, deadline - time.monotonic())))
    raise GateError("Timed out: the exact branch-push suite has no successful completed result")


def summarize_reuse(target, producer):
    server = os.environ.get("GITHUB_SERVER_URL", "https://github.com").rstrip("/")
    url = f"{server}/{target.repository}/actions/runs/{producer['id']}"
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
        summary.write(
            "### Schema canonical properties: branch-push result reused\n\n"
            f"Source revision: `{target.sha}`\n\n"
            f"[Producer run]({url}) · [Producer artifacts]({url}#artifacts)\n\n"
            "The producer properties job completed the suite, checksums, and evidence upload "
            "for this source tree. This gate ran no second suite "
            "and publishes no duplicate evidence artifact.\n"
        )
    print(f"Accepted branch-push suite: {url}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait-seconds", type=float, default=1800)
    parser.add_argument("--poll-seconds", type=float, default=15)
    parser.add_argument("--max-poll-seconds", type=float, default=30)
    parser.add_argument("--http-timeout", type=float, default=20)
    options = parser.parse_args()
    if min(options.wait_seconds, options.poll_seconds, options.max_poll_seconds, options.http_timeout) <= 0:
        raise GateError("Wait, poll, and HTTP timeout values must be positive")
    with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as event_file:
        event = json.load(event_file)
    target, reason = source_target(event)
    if target is None:
        print(reason, flush=True)
        write_output("false")
        return
    producer = wait_for_producer(target, options)
    summarize_reuse(target, producer)
    write_output("true")


if __name__ == "__main__":
    try:
        main()
    except (GateError, OSError, KeyError, ValueError, subprocess.TimeoutExpired) as error:
        print(f"Schema canonical reuse gate failed: {error}", file=sys.stderr)
        sys.exit(1)
