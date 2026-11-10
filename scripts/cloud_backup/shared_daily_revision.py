"""Bounded, sanitized ExeMem identity evidence from actual product requests."""

import os
from pathlib import Path
import re

from support import OperationError, decode_json, private_file, require, stable


VARIABLE = "LASTDB_CLOUD_CI_SERVICE_RECEIPT"
FILENAME = "exemem-service-responses.jsonl"
MAX_BYTES = 8 * 1024 * 1024


def prepare(root):
    path = root / FILENAME
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    os.close(descriptor)
    return path


def child_capture(environment):
    """Pass only the owned receipt and a CI boolean through the closed child env."""
    value = os.environ.get(VARIABLE)
    if value is None:
        return
    path = Path(value)
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and path.is_absolute()
            and path == Path(os.environ.get("RUNNER_TEMP", "")) / FILENAME,
            "shared_service_receipt_scope")
    stream, _metadata = private_file(path)
    stream.close()
    environment.update({VARIABLE: str(path), "GITHUB_ACTIONS": "true"})


def identity(value):
    require(isinstance(value, dict) and set(value) == {
        "revision", "function_version", "lastdb_revision"}, "shared_service_identity_fields")
    require(isinstance(value["revision"], str)
            and re.fullmatch(r"[0-9a-f]{40}", value["revision"])
            and isinstance(value["function_version"], str)
            and re.fullmatch(r"[1-9][0-9]{0,19}", value["function_version"])
            and (value["lastdb_revision"] is None or isinstance(value["lastdb_revision"], str)
                 and re.fullmatch(r"[0-9a-f]{40}", value["lastdb_revision"])),
            "shared_service_identity_value")
    return value["revision"], value["function_version"], value["lastdb_revision"]


def public_identity(value):
    return dict(zip(("revision", "function_version", "lastdb_revision"), value))


def parse(raw):
    pending, seen, identities = set(), set(), set()
    offset, responses, missing, invalid = 0, 0, 0, 0
    for line in raw.splitlines(keepends=True):
        require(line.endswith(b"\n") and len(line) <= 512, "shared_service_receipt_line")
        entry = decode_json(line)
        require(isinstance(entry, dict) and type(entry.get("request")) is int,
                "shared_service_receipt_entry")
        request = entry["request"]
        if entry.get("event") == "started":
            require(set(entry) == {"event", "request"} and request == offset
                    and request not in seen, "shared_service_receipt_start")
            pending.add(request)
            seen.add(request)
        else:
            require(set(entry) == {"event", "request", "status", "identity"}
                    and entry["event"] == "response" and request in pending
                    and type(entry["status"]) is int and 100 <= entry["status"] <= 599,
                    "shared_service_receipt_response")
            pending.remove(request)
            responses += 1
            observed = entry["identity"]
            require(isinstance(observed, dict), "shared_service_receipt_identity")
            state = observed.get("state")
            if state == "identified":
                identities.add(identity({key: value for key, value in observed.items() if key != "state"}))
                require(len(identities) <= 16, "shared_service_revision_bound")
            else:
                require(set(observed) == {"state"} and state in ("missing", "invalid"),
                        "shared_service_receipt_identity_state")
                missing += state == "missing"
                invalid += state == "invalid"
        offset += len(line)
    result = "stable"
    if len(identities) > 1:
        result = "deployment_changed"
    elif not responses or pending or missing or invalid:
        result = "unattested"
    return {"result": result, "requests": len(seen), "responses": responses,
            "pending": len(pending), "missing": missing, "invalid": invalid,
            "identities": [public_identity(value) for value in sorted(identities, key=str)]}


def collect(path):
    """Read only after product children stop. Evidence failure never skips cleanup."""
    try:
        stream, before = private_file(path)
        with stream:
            require(before.st_size <= MAX_BYTES, "shared_service_receipt_size")
            raw = stream.read(MAX_BYTES + 1)
            require(len(raw) <= MAX_BYTES and stable(before, os.fstat(stream.fileno())),
                    "shared_service_receipt_changed")
        return parse(raw)
    except (OperationError, OSError):
        return {"result": "invalid_receipt", "identities": []}


def combined(services):
    require(services, "shared_service_results_missing")
    values = set()
    for service in services:
        require(isinstance(service, dict) and service.get("result") == "stable"
                and isinstance(service.get("identities"), list) and len(service["identities"]) == 1
                and type(service.get("responses")) is int and service["responses"] > 0
                and service.get("requests") == service["responses"]
                and all(service.get(name) == 0 for name in ("pending", "missing", "invalid")),
                "shared_service_unattested_case")
        values.add(identity(service["identities"][0]))
    return {"result": "stable" if len(values) == 1 else "deployment_changed",
            "identities": [public_identity(value) for value in sorted(values, key=str)]}


def write_summary(summary, result):
    summary.write(f"\nExeMem serving identity: **{result['result']}**.\n\n")
    for value in result["identities"]:
        identity(value)
        summary.write(f"- ExeMem `{value['revision']}`, Lambda version `{value['function_version']}`")
        if value["lastdb_revision"] is not None:
            summary.write(f", embedded LastDB dependency `{value['lastdb_revision']}`")
        summary.write(".\n")
    summary.write("\nObserved on actual product control responses; provider-direct transfers are excluded. "
                  "The embedded dependency is distinct from the tested LastDB client revision.\n")
