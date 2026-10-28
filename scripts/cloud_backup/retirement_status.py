"""Validate immutable cleanup identities across bounded service progress."""

import hashlib
import json

from ci_control import decimal, digest, no_deletion
from kept_recovery import selected
from support import canonical_uuid, require

PHASES = ("directory", "mark", "planned", "retire", "drain", "delete", "reconcile", "complete")
MINIMUM = 96 * 1024**2


def input_digest(value):
    domain = b"lastdb:cloud-byte-backup:ci-cleanup-input:v2"
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return hashlib.sha256(len(domain).to_bytes(4, "big") + domain
                          + len(raw).to_bytes(8, "big") + raw).hexdigest()


def count(value):
    require(type(value) is int and 0 <= value < 2**64, "ci_cleanup_counter")
    return value


class Status:
    def __init__(self, scope, baseline):
        self.scope = scope
        self.baseline = baseline
        self.previous = None

    def validate(self, value, expected=None):
        require(isinstance(value, dict) and type(value.get("version")) is int
                and value["version"] == 2 and value.get("scope") == self.scope,
                "ci_cleanup_status_scope")
        request = value.get("input")
        require(isinstance(request, dict) and set(request) == {
            "version", "scope", "request_id", "required_headroom_bytes"}
            and type(request["version"]) is int and request["version"] == 2
            and request["scope"] == self.scope, "ci_cleanup_saved_input")
        request_id = canonical_uuid(request["request_id"])
        require(value.get("request_id") == request_id
                and decimal(request["required_headroom_bytes"]) > 0
                and value.get("input_sha256") == input_digest(request)
                and (expected is None or request == expected), "ci_cleanup_input_identity")
        if value.get("phase") == "bootstrap_headroom":
            require(expected is not None and self.previous is None, "ci_bootstrap_is_new_unsaved_input")
            self.headroom(value.get("headroom"), request["required_headroom_bytes"])
            no_deletion(value)
            require(value.get("saved") is False and value.get("plan_sha256") is None
                    and value.get("no_prior_startup_claims") is True
                    and value.get("new_work_allowed") is True
                    and value.get("eligible_cleanup_complete") is False
                    and value.get("global_erasure_complete") is False
                    and value.get("legacy_and_unknown_streams_retained") is True
                    and value["headroom"]["sufficient"] is True,
                    "ci_bootstrap_only_observation")
            return value
        baseline = {name: self.baseline[name] for name in ("stream_id", "commit_id", "commit_sha256")}
        require(value.get("baseline") == baseline, "ci_cleanup_preserves_baseline")
        require("kept_latest" in value, "ci_cleanup_kept_field")
        if value["kept_latest"] is not None:
            selected(value["kept_latest"], self.baseline)
        phase = value.get("phase")
        require(phase in PHASES, "ci_cleanup_blocked_or_unknown_phase")
        digest(value.get("directory_sha256"))
        for name in ("mark_sha256", "plan_sha256"):
            require(name in value, "ci_cleanup_commitment_field")
            if value[name] is not None:
                digest(value[name])
        if PHASES.index(phase) >= PHASES.index("planned"):
            require(value["mark_sha256"] is not None and value["plan_sha256"] is not None,
                    "ci_cleanup_planned_commitments")
        for name in ("confirmed_allocations", "released_bytes", "protected_uncertain_bytes"):
            count(value.get(name))
        require(value.get("completion_scope") == "eligible_ci_owned_streams"
                and value.get("global_erasure_complete") is False
                and value.get("eligible_cleanup_complete") is (
                    phase == "complete" and value["protected_uncertain_bytes"] == 0),
                "ci_cleanup_completion_scope")
        self.headroom(value.get("headroom"), request["required_headroom_bytes"])
        self.continuity(value)
        self.previous = value
        return value

    def continuity(self, value):
        before = self.previous
        if before is None:
            return
        require(all(value[name] == before[name] for name in (
            "input", "input_sha256", "request_id", "baseline", "kept_latest")),
            "ci_cleanup_saved_identity_changed")
        require(PHASES.index(value["phase"]) >= PHASES.index(before["phase"])
                and all(value[name] >= before[name] for name in (
                    "confirmed_allocations", "released_bytes")),
                "ci_cleanup_progress_regressed")
        for name in ("plan_sha256", "mark_sha256"):
            require(before[name] is None or value[name] == before[name], "ci_cleanup_plan_changed")
        if before["plan_sha256"] is not None:
            require(value["directory_sha256"] == before["directory_sha256"],
                    "ci_cleanup_directory_changed")

    @staticmethod
    def headroom(value, required):
        require(isinstance(value, dict), "ci_cleanup_headroom")
        counters = {name: decimal(value.get(name)) for name in (
            "observed_ms", "quota_bytes", "other_used_bytes", "reserved_bytes", "retained_bytes",
            "available_bytes", "required_headroom_bytes")}
        used = sum(counters[name] for name in ("other_used_bytes", "reserved_bytes", "retained_bytes"))
        require(used < 2**64 and counters["observed_ms"] > 0
                and value["required_headroom_bytes"] == required
                and counters["available_bytes"] == max(0, counters["quota_bytes"] - used)
                and value.get("sufficient") is (counters["available_bytes"] >= int(required))
                and value.get("reserves_quota") is False
                and value.get("admission_authoritative") is True, "ci_cleanup_headroom_identity")
