"""Real owner commands and writes for the serialized DEV automatic operation."""

import copy
import hashlib
import os
import re
import time

from support import ACCOUNT, OperationError, canonical_uuid, require

MAX_CHECKPOINTS = 8
MAX_DECLARED_BYTES = 64 * 1024 * 1024


def completion(value, policy):
    journal = value["mutation_log"]
    head = journal["head"]
    return {"enabled": bool(journal["enabled"]), "no_gap": journal["gap"] is None,
            "no_snapshot_required": not journal["snapshot_required"],
            "released": journal["retained_bytes"] == 0,
            "acknowledged": head is not None and journal["acknowledged_through"] == head["sequence"],
            "frontiers_equal": head is not None and policy["last_backed_up_frontiers"] == head["frontiers"]}


def number(value):
    return value if type(value) is int and 0 <= value <= 2**64 - 1 else None


def frontiers(value):
    return None if not isinstance(value, dict) else {name: number(value.get(name)) for name in ("main", "cas")}


def public(value, pattern):
    return value if isinstance(value, str) and len(value) <= 64 and re.fullmatch(pattern, value) else None


def paused(value):
    if value is None:
        return None
    if not isinstance(value, dict):
        return {"disposition": None, "reason": None}
    dispositions = ("retry", "attention")
    reasons = ("temporary", "disabled", "busy", "missing", "identity", "invalid", "disk", "source",
               "unsupported", "resource_limit", "quota", "credentials", "authority", "refused",
               "closing", "shutdown", "worker_panic")
    return {"disposition": value.get("disposition") if value.get("disposition") in dispositions else None,
            "reason": value.get("reason") if value.get("reason") in reasons else None}


def checkpoint_identity(accepted):
    return {"job": accepted["begin"]["job"], "commit_sha256": accepted["commit_sha256"],
            "ordinal": accepted["ordinal"], "root_receipt_id": accepted["root_receipt_id"],
            "inventory_root": {name: accepted["inventory_root"][name]
                for name in ("sha256", "page_count", "object_count", "stored_bytes")}}


def packed_snapshot(value):
    counts = {name: number(value.get(name))
              for name in ("metadata_pack_objects", "multi_file_pack_objects")}
    require(all(count is not None and count > 0 for count in counts.values())
            and counts["multi_file_pack_objects"] <= counts["metadata_pack_objects"],
            "selected_snapshot_uses_multi_file_packs")
    accepted = value["last_accepted_backup"]
    return {"request_id": value["request_id"], "commit_sha256": accepted["commit_sha256"], **counts}


def released_publication(value, policy):
    accepted = value.get("last_accepted_backup")
    return (value["phase"] == "published" and not value["running"]
            and value["retained_source_bytes"] == 0 and accepted is not None
            and policy["last_completed_request_id"] == value["request_id"]
            and accepted["begin"]["job"]["commit_id"] == value["request_id"])


def durable_batch(node, mutations):
    """Send one owned batch once; an uncertain result never permits a replay."""
    require(isinstance(mutations, list) and 0 < len(mutations) <= 64
            and all(item["durability"] == "durable"
                    and item["mutation_type"] in ("create", "update", "delete") for item in mutations),
            "bounded_durable_ci_mutations")
    reply = node.call("/api/mutations/batch", {"mutations": mutations})
    identifiers = reply.get("mutation_ids")
    expected = {result: sum(item["mutation_type"] == verb for item in mutations)
                for verb, result in (("create", "created"), ("update", "updated"), ("delete", "deleted"))}
    expected.update(no_op=0, total=len(mutations))
    require(reply.get("ok") is True and reply.get("user_hash") == ACCOUNT
            and type(reply.get("count")) is int and reply["count"] == len(mutations)
            and isinstance(identifiers, list) and len(identifiers) == len(mutations)
            and all(isinstance(value, str) and value for value in identifiers)
            and len(set(identifiers)) == len(identifiers)
            and isinstance(reply.get("operations"), dict) and reply["operations"] == expected
            and all(type(value) is int and value >= 0 for value in reply["operations"].values())
            and reply.get("durability") == "durable", "exact_durable_ci_mutation_ack")
    for value in identifiers:
        canonical_uuid(value)
    return reply


def observation(value, policy, conditions, kind, through, count, declared_bytes):
    uuid = r"[0-9a-f]{8}-(?:[0-9a-f]{4}-){3}[0-9a-f]{12}"
    digest = r"[0-9a-f]{64}"
    journal, accepted = value["mutation_log"], value.get("last_accepted_backup") or {}
    head = journal["head"] or {}
    job = accepted.get("begin", {}).get("job", {})
    stream = job.get("stream", {})
    generation = public(stream.get("scope", {}).get("account_generation"), r"0|[1-9][0-9]{0,19}")
    generation = generation if generation is not None and int(generation) <= 2**64 - 1 else None
    return {"requested_kind": kind if kind in ("snapshot", "mutation_log") else None,
            "published": value["phase"] == "published",
            "running": value["running"] if type(value["running"]) is bool else None,
            "automatic_enabled": (value.get("automatic_backup_enabled")
                                  if type(value.get("automatic_backup_enabled")) is bool else None),
            "policy_enabled": policy.get("enabled") if type(policy.get("enabled")) is bool else None,
            "policy_paused": paused(policy.get("paused")),
            "log_interval_seconds": number(policy.get("log_interval_seconds")),
            "snapshot_interval_seconds": number(policy.get("snapshot_interval_seconds")),
            "retained_source_bytes": number(value["retained_source_bytes"]),
            "request_id": public(value["request_id"], uuid),
            "accepted_request_id": public(job.get("commit_id"), uuid),
            "accepted_stream_id": public(stream.get("stream_id"), uuid),
            "accepted_account_generation": generation,
            "accepted_commit_sha256": public(accepted.get("commit_sha256"), digest),
            "last_completed_request_id": public(policy["last_completed_request_id"], uuid),
            "pending_request_id": public(policy["pending_request_id"], uuid),
            "next_log_at": number(policy["next_log_at"]),
            "requested_through": frontiers(through),
            "last_backed_up_frontiers": frontiers(policy["last_backed_up_frontiers"]),
            "journal": {"head_epoch": public(head.get("epoch"), uuid),
                "head_sha256": public(head.get("sha256"), digest),
                "head_sequence": number(head.get("sequence")),
                "head_frontiers": frontiers(head.get("frontiers")),
                "acknowledged_through": number(journal["acknowledged_through"]),
                "retained_bytes": number(journal["retained_bytes"])},
            "completion": conditions, "observed_checkpoints": count,
            "declared_checkpoint_bytes": declared_bytes}


class Source:
    def __init__(self, node, baseline, source, summary):
        self.node, self.expected, self.source = node, copy.deepcopy(baseline), source
        self.summary = summary
        self.completed, self.declared_bytes = {}, 0
        # The one final stopped snapshot. The v1 ownership receipt names only
        # daemon-initiated points, so this point stays outside `completed`.
        self.stopped = None
        self.publication_deadlines = {}

    def cloud(self, *arguments, input_bytes=None, checkpoint=None):
        value = self.node.programs.command(self.node.home, ["cloud", "backup-v2", *arguments, "--json"], input_bytes)
        if (isinstance(value.get("automatic_backup"), dict)
                and all(name in value for name in ("mutation_log", "last_accepted_backup"))):
            policy = value["automatic_backup"]
            conditions = completion(value, policy)
            kind, through = checkpoint or (None, None)
            try:
                self.observe_checkpoint(value)
            finally:
                self.summary["automatic_checkpoint_observation"] = observation(
                    value, policy, conditions, kind, through, len(self.completed), self.declared_bytes)
        return value

    def connect(self, key):
        value = self.cloud("connect", "--existing", "--env", "dev", "--api-key-stdin",
                           "--expected-user-hash", ACCOUNT, input_bytes=key)
        require(value["manual_backup_enabled"] and not value["automatic_backup_enabled"]
                and not value["registration_attempted"], "existing_dev_connection_only")

    def configure(self, snapshot=86400):
        value = self.cloud("enable-automatic", "--log-interval-seconds", "60",
                           "--snapshot-interval-seconds", str(snapshot))
        self.policy(value, snapshot)
        return value

    def policy(self, value, snapshot=86400):
        policy = value["automatic_backup"]
        require(value["user_hash"] == ACCOUNT and value["automatic_backup_enabled"] is True
                and policy["enabled"] is True and policy["paused"] is None
                and policy["log_interval_seconds"] == 60
                and policy["snapshot_interval_seconds"] == snapshot, "automatic_dev_policy")
        require(value["blocked_reason"] is None, "automatic_backup_blocked")
        return policy

    def published(self, kind, previous=None, snapshot=86400, through=None):
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            value = self.cloud("status", checkpoint=(kind, through))
            policy = self.policy(value, snapshot)
            accepted = value.get("last_accepted_backup")
            if (released_publication(value, policy) and value["backup_kind"] == kind
                    and accepted["commit_sha256"] != previous
                    and (through is None or all(policy["last_backed_up_frontiers"][name] >= frontier
                         for name, frontier in through.items()))):
                request = accepted["begin"]["job"]["commit_id"]
                canonical_uuid(request)
                require(request.startswith("a17dbac2-") and request == value["request_id"],
                        "daemon_initiated_request")
                require(accepted["begin"]["job"]["stream"]["scope"]["account_id"] == ACCOUNT,
                        "accepted_dev_scope")
                if kind == "snapshot" and previous is None:
                    self.summary["packed_snapshot"] = packed_snapshot(value)
                self.node.health()
                require(time.monotonic() < deadline, "automatic_publication_deadline")
                self.publication_deadlines[accepted["commit_sha256"]] = deadline
                self.summary["frozen_backup_selection"] = {"kind": kind, "request_id": request,
                    "commit_sha256": public(accepted["commit_sha256"], r"[0-9a-f]{64}")}
                return copy.deepcopy(value)
            time.sleep(0.5)
        raise OperationError("automatic_publication_deadline")

    def observe_checkpoint(self, value):
        accepted = value.get("last_accepted_backup")
        if accepted is None:
            return
        self.observe_accepted(accepted)

    def observe_accepted(self, accepted):
        job = accepted["begin"]["job"]
        request = canonical_uuid(job["commit_id"])
        if self.stopped is not None and request == self.stopped["begin"]["job"]["commit_id"]:
            require(checkpoint_identity(self.stopped) == checkpoint_identity(accepted),
                    "immutable_stopped_checkpoint")
            return
        require(request.startswith("a17dbac2-"), "daemon_initiated_request")
        self.owned_stream(accepted)
        if request in self.completed:
            require(checkpoint_identity(self.completed[request]) == checkpoint_identity(accepted),
                    "immutable_published_checkpoint")
        else:
            self.declare(accepted)
            self.completed[request] = copy.deepcopy(accepted)
        self.bounded()

    def bind_stopped(self, accepted):
        """Count the final stopped snapshot once in the same stream and cloud budget."""
        request = canonical_uuid(accepted["begin"]["job"]["commit_id"])
        require(self.stopped is None and not request.startswith("a17dbac2-")
                and request not in self.completed, "one_stopped_backup")
        self.owned_stream(accepted)
        self.declare(accepted)
        self.stopped = copy.deepcopy(accepted)
        self.bounded()

    def owned_stream(self, accepted):
        job = accepted["begin"]["job"]
        require(job["stream"]["scope"]["account_id"] == ACCOUNT, "accepted_dev_scope")
        if self.node.ci_stream_id is not None:
            require(job["stream"]["stream_id"] == self.node.ci_stream_id, "accepted_owned_ci_stream")
        if self.node.terminal_r2:
            require(accepted["begin"].get("r2_multipart") is True, "accepted_terminal_r2_mode")
        if self.completed:
            first = next(iter(self.completed.values()))
            require(job["stream"] == first["begin"]["job"]["stream"], "one_owned_automatic_stream")

    def declare(self, accepted):
        declared = int(accepted["inventory_root"]["stored_bytes"])
        require(declared > 0, "positive_checkpoint_bytes")
        self.declared_bytes += declared

    def bounded(self):
        points = len(self.completed) + (self.stopped is not None)
        require(points <= MAX_CHECKPOINTS and self.declared_bytes <= MAX_DECLARED_BYTES,
                "bounded_ci_cloud_work")

    def retired(self, request):
        canonical_uuid(request)
        require(request.startswith("a17dbac2-"), "owned_automatic_metadata_only")
        path = self.node.home / "data/cloud-byte-backup-v2/jobs" / request
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if not os.path.lexists(path):
                return
            self.policy(self.cloud("status"))
            time.sleep(0.5)
        raise OperationError("previous_automatic_metadata_not_retired")

    def caught_up(self, selected, snapshot=86400):
        """Observe current progress without replacing the frozen recovery point."""
        commit = selected["last_accepted_backup"]["commit_sha256"]
        require(commit in self.publication_deadlines, "frozen_publication_deadline")
        deadline = self.publication_deadlines[commit]
        value = selected
        while time.monotonic() < deadline:
            policy = self.policy(value, snapshot)
            if (released_publication(value, policy) and all(completion(value, policy).values())
                    and policy["pending_request_id"] is None
                    and policy["attempts"] == policy["retry_at"] == 0):
                self.node.health()
                require(time.monotonic() < deadline, "accepted_complete_journal_head")
                return
            time.sleep(min(0.5, max(0, deadline - time.monotonic())))
            if time.monotonic() < deadline:
                value = self.cloud("status")
        raise OperationError("accepted_complete_journal_head")

    def write(self, schema):
        # Automatic publication may split these real writes across several logs.
        payload = hashlib.shake_256(("Cloud automatic CI " + self.source).encode()).digest(65536)
        path = self.node.home.parent / "automatic-file.bin"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        pointer = self.node.programs.command(self.node.home, ["db", "put-blob-local", "--file", str(path)])
        mutations = []
        records = self.expected["records"]
        for number, record in enumerate(records):
            deleted = number == len(records) - 1
            if deleted:
                records[number] = {"id": record["id"], "deleted": True}
                fields = {"id": record["id"]}
            else:
                record["note"] = "Automatic log update " + str(number)
                fields = {**record, "file": pointer}
            mutations.append({"type": "mutation", "schema": schema,
                "key_value": {"hash": record["id"], "range": None}, "fields_and_values": fields,
                "mutation_type": "delete" if deleted else "update", "durability": "durable",
                "must_exist": True})
        durable_batch(self.node, mutations)
        self.expected.update(file_pointer=pointer, file_bytes=len(payload), file_sha256=hashlib.sha256(payload).hexdigest())
        self.node.verify_records(self.expected)

    def disable(self):
        value = self.cloud("disable-automatic")
        require(value["automatic_backup_enabled"] is False and not value["running"]
                and value["retained_source_bytes"] == 0, "automatic_disabled_after_source_release")
