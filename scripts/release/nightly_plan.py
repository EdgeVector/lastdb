#!/usr/bin/env python3
"""Freeze a nightly source and require its exact GitHub Actions checks."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import tempfile
import time
import tomllib


REPOSITORY = "EdgeVector/lastdb"
ACTIONS_APP_ID = 15368
CHECKS = ("ci-required", "benchmark-complete", "Analyze (actions)", "Analyze (python)",
          "Analyze (rust)", "Analyze (ruby)", "Analyze (javascript-typescript)")
SHA = re.compile(r"[0-9a-f]{40}\Z")
CORE_VERSION = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
MAX_API_BYTES, MAX_ERROR_BYTES, MAX_PLAN_BYTES = 4 * 1024 ** 2, 64 * 1024, 16 * 1024
MAX_CHECK_PAGES, CHECKS_PER_PAGE = 10, 100


class MetadataError(RuntimeError):
    """A failed metadata command with numeric status and no stderr content."""

    def __init__(self, exit_code, http_status=None):
        if type(exit_code) is not int:
            raise ValueError("The metadata exit code is invalid")
        if http_status is not None and (type(http_status) is not int or not 100 <= http_status <= 599):
            raise ValueError("The metadata HTTP status is invalid")
        self.exit_code, self.http_status = exit_code, http_status
        suffix = f" (HTTP {http_status})" if http_status is not None else ""
        super().__init__(f"A nightly metadata command failed with exit code {exit_code}{suffix}")


def _capture(command, body=b"", timeout=45):
    """Read bounded child output without exposing its stderr or environment."""
    streams, buffers = selectors.DefaultSelector(), {"out": bytearray(), "err": bytearray()}
    process = None
    try:
        with tempfile.TemporaryFile() as source:
            source.write(body)
            source.seek(0)
            process = subprocess.Popen(command, stdin=source, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            streams.register(process.stdout, selectors.EVENT_READ, "out")
            streams.register(process.stderr, selectors.EVENT_READ, "err")
            deadline = time.monotonic() + timeout
            while streams.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError("A nightly metadata command exceeded its timeout")
                for key, _ in streams.select(min(1, remaining)):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        streams.unregister(key.fileobj)
                        continue
                    target = buffers[key.data]
                    target.extend(chunk)
                    limit = MAX_API_BYTES if key.data == "out" else MAX_ERROR_BYTES
                    if len(target) > limit:
                        raise RuntimeError("A nightly metadata command exceeded its output bound")
            code = process.wait(timeout=max(.1, deadline - time.monotonic()))
            if code:
                statuses = re.findall(rb"\bHTTP ([1-5][0-9]{2})\b", buffers["err"])
                raise MetadataError(code, int(statuses[-1]) if statuses else None)
            return bytes(buffers["out"]).decode("utf-8")
    finally:
        streams.close()
        if process is not None:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            for stream in (process.stdout, process.stderr):
                if stream is not None:
                    stream.close()


def gh_json(endpoint, method="GET", payload=None):
    """Run one bounded GitHub API request; callers choose any mutation explicitly."""
    prefix = f"repos/{REPOSITORY}"
    if (not isinstance(endpoint, str) or len(endpoint) > 1000
            or not (endpoint == prefix or endpoint.startswith(prefix + "/"))
            or any(char in endpoint for char in ("\n", "\r", "\x00"))):
        raise ValueError("The nightly API endpoint is outside this repository")
    if method not in {"GET", "POST", "PATCH", "PUT", "DELETE"}:
        raise ValueError("The nightly API method is invalid")
    body = b"" if payload is None else json.dumps(payload, separators=(",", ":")).encode()
    if len(body) > MAX_ERROR_BYTES:
        raise ValueError("The nightly API payload exceeds its bound")
    command = ["gh", "api", "--hostname", "github.com", "--method", method,
               "--header", "Accept: application/vnd.github+json",
               "--header", "X-GitHub-Api-Version: 2022-11-28", endpoint]
    if payload is not None:
        command += ["--input", "-"]
    output = _capture(command, body)
    return json.loads(output) if output.strip() else None


def _git(*arguments):
    return _capture(["git", *arguments], timeout=20).strip()


def _sha(value, name):
    if not isinstance(value, str) or not SHA.fullmatch(value):
        raise ValueError(f"The nightly {name} must be a complete source SHA")
    return value


def _read_json(path, maximum=MAX_PLAN_BYTES):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise ValueError("The nightly JSON file identity or size is invalid")
    with path.open("rb") as source:
        raw = source.read(maximum + 1)
    if len(raw) > maximum:
        raise ValueError("The nightly JSON file exceeds its bound")
    return json.loads(raw)


def load_plan(path):
    plan = _read_json(path)
    if (not isinstance(plan, dict) or plan.get("repository") != REPOSITORY
            or plan.get("event_name") not in {"pull_request", "workflow_dispatch", "schedule", "push"}
            or type(plan.get("publish")) is not bool):
        raise ValueError("The nightly plan identity is invalid")
    _sha(plan.get("source"), "source")
    _sha(plan.get("base"), "base")
    version = re.fullmatch(rf"{CORE_VERSION}-nightly\.([0-9]{{8}})\.g([0-9a-f]{{9}})",
                           plan.get("version", "")) if isinstance(plan.get("version"), str) else None
    tag = re.fullmatch(r"nightly-([0-9]{8})-([0-9a-f]{9})", plan.get("tag", "")) \
        if isinstance(plan.get("tag"), str) else None
    if not version or not tag or type(plan.get("epoch")) is not int or plan["epoch"] < 0:
        raise ValueError("The nightly plan version or source epoch is invalid")
    if version[1] != tag[1] or version[2] != plan["source"][:9] or tag[2] != plan["source"][:9]:
        raise ValueError("The nightly plan version does not identify its source")
    datetime.strptime(version[1], "%Y%m%d")
    if (not isinstance(plan.get("run_url"), str)
            or not re.fullmatch(r"https://github\.com/EdgeVector/lastdb/actions/runs/[0-9]+", plan["run_url"])):
        raise ValueError("The nightly plan run URL is invalid")
    if plan["event_name"] == "pull_request" and plan["publish"]:
        raise ValueError("A pull request cannot publish a nightly release")
    return plan


def make_plan(output):
    if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise ValueError("Nightly builds require the canonical repository")
    repository = gh_json(f"repos/{REPOSITORY}")
    if (not isinstance(repository, dict) or repository.get("full_name") != REPOSITORY
            or repository.get("private") is not False or repository.get("visibility") != "public"):
        raise ValueError("Nightly builds require a public repository")
    source = _sha(os.environ.get("SOURCE_SHA"), "source")
    if _git("rev-parse", "HEAD") != source or _git("status", "--porcelain"):
        raise ValueError("The nightly source checkout differs or contains local changes")
    event = os.environ.get("GITHUB_EVENT_NAME")
    if event not in {"pull_request", "workflow_dispatch", "schedule", "push"}:
        raise ValueError("The nightly event is unsupported")
    publish = event != "pull_request" and os.environ.get("GITHUB_REF") == "refs/heads/main"
    if publish:
        _git("merge-base", "--is-ancestor", source, "refs/remotes/origin/main")
    if event == "pull_request":
        data = _read_json(os.environ["GITHUB_EVENT_PATH"], maximum=1024 ** 2)
        request = data["pull_request"]
        if _sha(request["head"]["sha"], "pull-request head") != source:
            raise ValueError("The nightly source differs from the pull-request head")
        base = _sha(os.environ.get("BASE_SHA"), "pull-request base")
        if base != request["base"]["sha"]:
            raise ValueError("The nightly base differs from the pull-request base")
    else:
        base = _sha(_git("rev-parse", source + "^"), "base")
    root = Path(_git("rev-parse", "--show-toplevel")).resolve()
    output = Path(output).resolve()
    if output.is_relative_to(root) or output.exists():
        raise ValueError("The nightly plan needs a fresh path outside the source checkout")
    core = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    if not isinstance(core, str) or not re.fullmatch(CORE_VERSION, core):
        raise ValueError("The workspace version is not a numeric release core")
    date = datetime.now(timezone.utc).strftime("%Y%m%d")
    epoch = int(_git("show", "-s", "--format=%ct", source))
    run_id = os.environ.get("GITHUB_RUN_ID", "")
    if not re.fullmatch(r"[0-9]+", run_id) or os.environ.get("GITHUB_SERVER_URL") != "https://github.com":
        raise ValueError("The nightly run identity is invalid")
    plan = {"repository": REPOSITORY, "event_name": event, "source": source,
            "version": f"{core}-nightly.{date}.g{source[:9]}", "tag": f"nightly-{date}-{source[:9]}",
            "epoch": epoch, "base": base, "publish": publish,
            "run_url": f"https://github.com/{REPOSITORY}/actions/runs/{run_id}"}
    encoded = json.dumps(plan, indent=2, sort_keys=True) + "\n"
    if len(encoded.encode()) > MAX_PLAN_BYTES:
        raise ValueError("The nightly plan exceeds its bound")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x") as target:
        target.write(encoded)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as target:
        for name in ("source", "version", "tag", "epoch", "base", "publish"):
            value = str(plan[name]).lower() if type(plan[name]) is bool else str(plan[name])
            target.write(f"{name}={value}\n")
    return plan


def _latest_checks(source):
    latest, seen = {}, 0
    for page in range(1, MAX_CHECK_PAGES + 1):
        result = gh_json(f"repos/{REPOSITORY}/commits/{source}/check-runs"
                         f"?filter=all&per_page={CHECKS_PER_PAGE}&page={page}")
        rows, total = result["check_runs"], result["total_count"]
        if (not isinstance(rows, list) or len(rows) > CHECKS_PER_PAGE
                or type(total) is not int or not 0 <= total <= MAX_CHECK_PAGES * CHECKS_PER_PAGE):
            raise ValueError("The exact-source check list exceeds its bound")
        for check in rows:
            if check.get("head_sha") != source:
                raise ValueError("A check result belongs to another source")
            if check.get("name") not in CHECKS or check.get("app", {}).get("id") != ACTIONS_APP_ID:
                continue
            identity = check["id"]
            if type(identity) is not int or identity <= 0:
                raise ValueError("An exact-source check identity is invalid")
            previous = latest.get(check["name"])
            if previous is None or identity > previous["id"]:
                latest[check["name"]] = check
        seen += len(rows)
        if seen >= total:
            return latest
        if not rows:
            raise ValueError("The exact-source check list ended before its reported count")
    raise ValueError("The exact-source check list did not finish within its bound")


def wait_gates(plan, wait_seconds=1800):
    """Require current Actions successes for every named context at one source."""
    if (not isinstance(plan, dict) or plan.get("repository") != REPOSITORY
            or plan.get("event_name") not in {"pull_request", "workflow_dispatch", "schedule", "push"}):
        raise ValueError("The nightly gate plan identity is invalid")
    source = _sha(plan.get("source"), "source")
    if type(wait_seconds) is not int or not 0 <= wait_seconds <= 7200:
        raise ValueError("The nightly gate wait must be between 0 and 7200 seconds")
    if plan["event_name"] == "pull_request":
        return {"skipped": True, "reason": "PR builds do not publish releases", "source": source}
    deadline = time.monotonic() + wait_seconds
    while True:
        latest = _latest_checks(source)
        pending = []
        for name in CHECKS:
            check = latest.get(name)
            if check is None or check.get("status") != "completed":
                pending.append(name)
            elif check.get("conclusion") != "success":
                raise RuntimeError(f"The exact-source gate {name} did not succeed")
        if not pending:
            return {"skipped": False, "source": source,
                    "checks": {name: {"id": latest[name]["id"], "url": latest[name]["html_url"]}
                               for name in CHECKS}}
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("The exact-source gates remain unavailable: " + ", ".join(pending))
        print("Exact-source gates pending: " + ", ".join(pending), flush=True)
        time.sleep(min(30, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    plan.add_argument("--output", type=Path, required=True)
    gates = commands.add_parser("gates")
    gates.add_argument("--plan", type=Path, required=True)
    gates.add_argument("--wait-seconds", type=int, default=1800)
    args = parser.parse_args()
    result = (make_plan(args.output) if args.command == "plan"
              else wait_gates(load_plan(args.plan), args.wait_seconds))
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
