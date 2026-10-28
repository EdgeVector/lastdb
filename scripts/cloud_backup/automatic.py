"""Automatic snapshot, real mutation log, restart and source-free DEV recovery."""

import hashlib
import json
import time

from automatic_source import Source
from node import OwnedNode
from restore_receipts import restored
from support import require


def selection(baseline, accepted):
    result = dict(baseline)
    job = accepted["begin"]["job"]
    result.update(stream_id=job["stream"]["stream_id"],
                  account_generation=job["stream"]["scope"]["account_generation"],
                  commit_id=job["commit_id"], commit_sha256=accepted["commit_sha256"])
    return result


class Automatic:
    def __init__(self, operation, credentials, phrase):
        self.operation, self.credentials, self.phrase = operation, credentials, phrase
        self.programs, self.baseline = operation.programs, operation.baseline

    def recover(self, name, expected, accepted, replay=None):
        operation = self.operation
        require(operation.node.process is None, "source_stays_stopped_for_restore")
        parent = operation.root / name
        parent.mkdir(mode=0o700)
        destination = parent / "restored"
        selected = selection(expected, accepted)
        report = self.programs.restore(self.credentials, destination, selected, self.phrase)
        proof = restored(destination, report, selected, replay)
        inventory = json.dumps(proof.pop("files"), sort_keys=True, separators=(",", ":")).encode()
        proof["native_inventory_sha256"] = hashlib.sha256(inventory).hexdigest()
        node = OwnedNode(destination, self.programs)
        operation.node = node
        reads = []
        for _number in range(2):
            node.start()
            reads.append(node.verify_records(expected))
            node.stop()
        require(node.clean_stops == 2, "two_automatic_restore_clean_stops")
        return {"restore": proof, "restored_boots": 2, "strict_clean_stops": 2, "keyed_reads": reads}

    def run(self):
        operation = self.operation
        source = Source(OwnedNode(operation.root / "restored", self.programs, backup_source=True),
                        self.baseline, operation.args.source)
        operation.node = source.node
        source.node.start()
        source.connect(operation.api_key)
        source.configure()
        snapshot = source.published("snapshot")
        source.caught_up(snapshot)
        source.write(self.baseline["schema"])
        log = source.published("mutation_log", snapshot["last_accepted_backup"]["commit_sha256"])
        source.caught_up(log)
        routes = source.routes(log)
        source.retired(snapshot["request_id"])
        require(log["automatic_backup"]["last_snapshot_at"] == snapshot["automatic_backup"]["last_snapshot_at"],
                "log_did_not_replace_daily_snapshot")
        revision = log["automatic_backup"]["revision"]
        source.node.stop()
        source.node.start()
        policy = source.policy(source.cloud("status"))
        require(policy["revision"] == revision, "automatic_policy_survives_daemon_restart")
        # A clean stop can add its normal native housekeeping to the journal.
        # Give the unchanged minute schedule time to publish that complete cut.
        time.sleep(65)
        restarted = source.published("mutation_log")
        source.caught_up(restarted)
        source.node.verify_records(source.expected)
        source.configure(snapshot=60)
        later = source.published("snapshot", snapshot["last_accepted_backup"]["commit_sha256"], snapshot=60)
        require(later["automatic_backup"]["last_snapshot_at"] > snapshot["automatic_backup"]["last_snapshot_at"],
                "changed_interval_produced_a_later_snapshot")
        source.configure()
        source.retired(restarted["request_id"])
        source.disable()
        source.node.stop()
        replay = {"base": snapshot["last_accepted_backup"], "through": log["mutation_log"]["head"]}
        log_restore = self.recover("log-recovery", source.expected, log["last_accepted_backup"], replay)
        snapshot_restore = self.recover("snapshot-recovery", source.expected, later["last_accepted_backup"])
        return {"initial_snapshot": True, "minute_mutation_log": True, "daily_snapshot_unchanged_by_log": True,
                "automatic_policy_survives_restart": True, "changed_snapshot_interval_observed": True,
                "log_interval_seconds": 60, "final_snapshot_interval_seconds": 86400,
                "new_file_bytes": source.expected["file_bytes"], "new_file_sha256": source.expected["file_sha256"],
                "mutation_log_routes": routes,
                "updated_records": 15, "deleted_records": 1, "observed_checkpoints": len(source.completed),
                "declared_checkpoint_bytes": source.declared_bytes,
                "log_recovery": log_restore, "later_snapshot_recovery": snapshot_restore,
                "accepted_backups_retained": True, "provider_deletions": 0,
                "previous_automatic_local_metadata_retired": True,
                "enrollment_requests": 0, "primary_access": False}
