"""Bounded, account-scoped ownership calls; no provider administration authority."""

import base64
import http.client
import json
import re
import socket
import ssl
import time
from urllib.parse import urlsplit

from support import ACCOUNT, OperationError, canonical_uuid, decode_json, dev_endpoint, require

ERROR_CODES = frozenset((
    "invalid_input", "unsupported_version", "unsupported_capability", "unsupported_format",
    "unsupported_key_mode", "unauthorized", "not_found", "generation_conflict",
    "stream_authority_conflict", "job_closed", "identity_conflict", "predecessor_conflict",
    "resource_limit", "quota_exceeded", "missing_dependency", "integrity_mismatch",
    "recovery_key_mismatch", "source_unavailable", "provider_pending", "outcome_unknown", "retry_later"))


def decimal(value):
    require(isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]{0,19}", value)
            and int(value) < 2**64, "ci_control_decimal")
    return int(value)


def digest(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "ci_control_digest")
    return value


def no_deletion(value):
    require(value.get("provider_deletion_authorized") is False
            and type(value.get("provider_deletions")) is int
            and value["provider_deletions"] == 0, "ci_registration_no_deletion")


class Control:
    def __init__(self, baseline, key):
        require(baseline["account"] == ACCOUNT and isinstance(key, bytes)
                and 0 < len(key) <= 4096 and all(32 <= byte <= 126 for byte in key),
                "ci_control_credentials")
        self.key = key.decode("ascii")
        self.scope = {"account_id": ACCOUNT, "account_generation": baseline["account_generation"]}
        decimal(self.scope["account_generation"])
        self.baseline = {name: baseline[name] for name in ("stream_id", "commit_id", "commit_sha256")}
        canonical_uuid(self.baseline["stream_id"])
        canonical_uuid(self.baseline["commit_id"])
        digest(self.baseline["commit_sha256"])
        endpoint = urlsplit(dev_endpoint())
        require(endpoint.scheme == "https" and endpoint.path == "" and endpoint.port is None,
                "ci_control_dev_endpoint")
        self.host = endpoint.hostname
        self.deadline = time.monotonic() + 300
        self.calls = 0

    def close(self):
        self.key = None

    def post(self, route, extra):
        require(self.key is not None and route in ("context", "start", "register", "refresh", "usage"),
                "ci_control_route")
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "ci_control_deadline")
        request = {"version": 2, "scope": self.scope, **extra}
        if route == "context":
            request = {"version": 2, "account_id": ACCOUNT, "required_destinations": []}
        elif route == "usage":
            request = {"version": 2, "account_id": ACCOUNT}
        body = json.dumps(request, allow_nan=False, separators=(",", ":")).encode()
        require(len(body) <= 65536, "ci_control_request_size")
        connection = http.client.HTTPSConnection(self.host, timeout=min(30, remaining),
                                                 context=ssl.create_default_context())
        try:
            self.calls += 1
            suffix = "usage/read" if route == "usage" else "ci/" + route
            connection.request("POST", "/api/cloud/backups/v2/" + suffix, body,
                               {"X-API-Key": self.key, "Content-Type": "application/json"})
            response = connection.getresponse()
            require(not 300 <= response.status < 400 and response.getheader("Location") is None,
                    "ci_control_redirect_refused")
            length = response.getheader("Content-Length")
            require(length is None or (length.isdecimal() and int(length) <= 65536),
                    "ci_control_response_size")
            raw = response.read(65537)
            require(len(raw) <= 65536 and time.monotonic() <= self.deadline,
                    "ci_control_response_bound")
            try:
                value = decode_json(raw)
            except OperationError:
                raise OperationError("ci_control_http_" + str(response.status)) from None
            code = value.get("code") if isinstance(value, dict) else None
            if isinstance(code, str) and code in ERROR_CODES:
                # Protocol codes are fixed public names; never print the raw field or reply.
                raise OperationError("ci_control_service_" + code)
            require(response.status == 200, "ci_control_http_" + str(response.status))
            require(isinstance(value, dict) and type(value.get("version")) is int
                    and value["version"] == 2 and "code" not in value, "ci_control_response")
            return value
        except (OSError, socket.timeout, http.client.HTTPException) as error:
            # The caller can repeat only the same registration; never infer success.
            raise OperationError("ci_control_transport_unknown") from error
        finally:
            connection.close()

    def context(self, require_open=True, startup=False):
        value = self.post("context", {})
        require(value.get("scope") == self.scope and type(value.get("new_work_allowed")) is bool
                and (not require_open or value["new_work_allowed"]),
                "ci_control_account_context")
        capabilities = value.get("capabilities")
        require(isinstance(capabilities, dict) and capabilities.get("registration") is True,
                "ci_registration_capability")
        if startup:
            require(all(capabilities.get(name) is True for name in (
                "startup_registration", "startup_required", "r2_multipart_part")),
                "ci_startup_capabilities")
        return capabilities

    def usage(self):
        value = self.post("usage", {})
        require(value.get("account_id") == ACCOUNT, "ci_usage_dev_account")
        counters = {name: decimal(value.get(name)) for name in (
            "other_used_bytes", "reserved_bytes", "retained_bytes", "quota_bytes")}
        used = sum(counters[name] for name in ("other_used_bytes", "reserved_bytes", "retained_bytes"))
        require(used < 2**64, "ci_usage_total_bound")
        available = max(0, counters["quota_bytes"] - used)
        # This is an observation, not a reservation. Service admission stays authoritative.
        minimum = 96 * 1024**2
        require(available >= minimum, "ci_usage_insufficient_headroom_before_upload")
        return {**counters, "available_bytes": available, "minimum_bytes": minimum,
                "observation_only": True, "quota_changed": False}

    def claim(self, record, route, schema):
        receipt = record["ownership"]
        stream = canonical_uuid(receipt["stream_id"])
        require(receipt["schema"] == schema and receipt["account"] == ACCOUNT
                and receipt["account_generation"] == self.scope["account_generation"]
                and stream != self.baseline["stream_id"], "ci_register_owned_scope")
        archive = record["archive"]
        require(isinstance(archive, bytes) and 0 < len(archive) <= 32768,
                "ci_register_archive_bound")
        value = self.post(route, {"artifact_id": record["artifact_id"],
                          "archive_b64": base64.b64encode(archive).decode("ascii"),
                          "baseline": self.baseline})
        no_deletion(value)
        require(value.get("registered") is True and value.get("stream_id") == stream
                and value.get("artifact_id") == record["artifact_id"]
                and value.get("receipt_sha256") == record["receipt_sha256"], "ci_register_identity")
        digest(value.get("registration_sha256"))
        return value

    def start(self, record):
        value = self.claim(record, "start", "lastdb.cloud-ci-start.v1")
        require(value.get("scope") == self.scope and value.get("state") == "startup_registered"
                and value.get("admission_open") is True, "ci_start_admission")
        return {"stream_id": value["stream_id"], "state": value["state"],
                "registration_sha256": value["registration_sha256"]}

    def register(self, record):
        value = self.claim(record, "register", "lastdb.cloud-ci-stream.v1")
        completed = value.get("recovery_completed_ms")
        require((value.get("state") == "waiting_run" and completed is None)
                or (value.get("state") == "completed" and decimal(completed) > 0),
                "ci_register_completion")
        latest = value.get("latest_successful_stream")
        if latest is not None:
            canonical_uuid(latest)
            require(latest != self.baseline["stream_id"], "ci_register_latest_scope")
        return {"stream_id": value["stream_id"], "state": value["state"],
                "registration_sha256": value["registration_sha256"]}

    def refresh(self):
        after, records, promoted = None, 0, 0
        for _page in range(5001):
            value = self.post("refresh", {"after": after})
            no_deletion(value)
            require(value.get("scope") == self.scope and "after" in value
                    and type(value.get("refreshed")) is int
                    and 0 <= value["refreshed"] <= 2 and type(value.get("promoted")) is int
                    and 0 <= value["promoted"] <= value["refreshed"], "ci_refresh_counts")
            records += value["refreshed"]
            promoted += value["promoted"]
            cursor = value.get("after")
            if cursor is None:
                return {"registrations": records, "promoted": promoted, "complete": True}
            canonical_uuid(cursor)
            require(value["refreshed"] > 0 and (after is None or cursor > after),
                    "ci_refresh_cursor_progress")
            after = cursor
        raise OperationError("ci_refresh_page_bound")
