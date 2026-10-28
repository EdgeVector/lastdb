"""Real owner commands and writes for the serialized DEV automatic operation."""

import copy
import hashlib
import os
import time

from support import ACCOUNT, OperationError, canonical_uuid, read_json, require

MAX_CHECKPOINTS = 8
MAX_DECLARED_BYTES = 64 * 1024 * 1024


class Source:
    def __init__(self, node, baseline, source):
        self.node, self.expected, self.source = node, copy.deepcopy(baseline), source
        self.completed, self.declared_bytes = {}, 0

    def cloud(self, *arguments, input_bytes=None):
        return self.node.programs.command(self.node.home, ["cloud", "backup-v2", *arguments, "--json"], input_bytes)

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
            value = self.cloud("status")
            policy = self.policy(value, snapshot)
            accepted = value.get("last_accepted_backup")
            if (value["phase"] == "published" and not value["running"]
                    and value["retained_source_bytes"] == 0 and value["backup_kind"] == kind
                    and policy["last_completed_request_id"] == value["request_id"]
                    and accepted and accepted["commit_sha256"] != previous
                    and (through is None or all(policy["last_backed_up_frontiers"][name] >= frontier
                         for name, frontier in through.items()))):
                request = accepted["begin"]["job"]["commit_id"]
                canonical_uuid(request)
                require(request.startswith("a17dbac2-") and request == value["request_id"],
                        "daemon_initiated_request")
                require(accepted["begin"]["job"]["stream"]["scope"]["account_id"] == ACCOUNT,
                        "accepted_dev_scope")
                if request not in self.completed:
                    self.completed[request] = accepted
                    self.declared_bytes += int(accepted["inventory_root"]["stored_bytes"])
                require(len(self.completed) <= MAX_CHECKPOINTS and self.declared_bytes <= MAX_DECLARED_BYTES,
                        "bounded_ci_cloud_work")
                self.node.health()
                return value
            time.sleep(0.5)
        raise OperationError("automatic_publication_deadline")

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

    def caught_up(self, value):
        journal, policy = value["mutation_log"], self.policy(value)
        require(journal["enabled"] and journal["gap"] is None and not journal["snapshot_required"]
                and journal["retained_bytes"] == 0
                and journal["acknowledged_through"] == journal["head"]["sequence"]
                and policy["last_backed_up_frontiers"] == journal["head"]["frontiers"],
                "accepted_complete_journal_head")

    def routes(self, value):
        request = canonical_uuid(value["request_id"])
        root = self.node.home / "data/cloud-byte-backup-v2/jobs" / request
        plan = read_json(root / "log-plan.json")
        require(plan["ready"] and 0 < plan["objects"] <= 4096
                and 0 < plan["files"] <= 4096, "bounded_complete_log_plan")
        destinations = {}
        for number in range(plan["objects"]):
            record = read_json(root / f"log-object-{number:08}.json")
            receipt = record["receipt"]
            require(receipt and receipt["object"] == record["input"]["object"], "accepted_log_receipt")
            destination = receipt["object"]["destination"]
            if record["payload"] is not None:
                require(destination == "r2", "log_metadata_uses_r2")
            destinations[receipt["receipt_id"]] = destination
        roles = set()
        for number in range(plan["files"]):
            file = read_json(root / f"log-file-{number:08}.json")
            path = file["path"].split("/")
            require(len(path) == 5 and path[0] == "mutation-log"
                    and path[3] in ("main", "cas-add", "cas-delete"), "native_log_file_role")
            roles.add(path[3])
            destination = "r2" if path[3] == "main" else "b2"
            require(file["extents"] and all(destinations[e["receipt_id"]] == destination
                    for e in file["extents"]), "main_r2_and_cas_b2_routes")
        require({"main", "cas-add"} <= roles, "real_main_and_cas_log_bytes")
        return {"main_r2": True, "cas_b2": True, "metadata_r2": True, "native_files": plan["files"]}

    def write(self, schema):
        # Real native CAS bytes accompany the record updates in the next log.
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
                "mutation_type": "delete" if deleted else "update", "durability": "durable"})
        reply = self.node.call("/api/mutations/batch", {"mutations": mutations})
        require(len(reply["mutation_ids"]) == len(mutations), "real_dev_mutations_acknowledged")
        self.expected.update(file_pointer=pointer, file_bytes=len(payload), file_sha256=hashlib.sha256(payload).hexdigest())
        self.node.verify_records(self.expected)

    def disable(self):
        value = self.cloud("disable-automatic")
        require(value["automatic_backup_enabled"] is False and not value["running"]
                and value["retained_source_bytes"] == 0, "automatic_disabled_after_source_release")
