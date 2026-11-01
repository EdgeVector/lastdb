"""Automatic snapshot, real mutation log, restart and source-free DEV recovery."""

import hashlib
import json
import time

from automatic_source import Source
from automatic_prefixes import PrefixRecords
from automatic_stopped import StoppedFinal
from lifecycle import Lifecycle
from node import OwnedNode
from ownership import complete as complete_ownership
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

    def dependencies(self, expected, accepted):
        chosen = selection(expected, accepted)
        report = self.programs.command(self.credentials, ["cloud", "backup-v2", "inspect-dependencies",
            "--recovery-phrase-stdin", "--stream", chosen["stream_id"], "--generation", chosen["account_generation"],
            "--commit", chosen["commit_id"], "--commit-sha256", chosen["commit_sha256"], "--json"], self.phrase)
        require(report["accepted_hashes_verified"] is True and 0 < report["retained_points"] <= 8
                and report["closure_only"] is True and report["account_directory_frozen"] is False
                and report["provider_deletion_authorized"] is False and report["provider_deletions"] == 0,
                "accepted_dependency_inspection")
        return report

    def recover(self, name, expected, accepted, replay=None, lifecycle=None, initial=False, observe=None, temporary=None):
        operation = self.operation
        operation.summary["stage"] = name
        require(operation.node.process is None, "source_stays_stopped_for_restore")
        parent = operation.root / name
        parent.mkdir(mode=0o700)
        destination = parent / "restored"
        selected = selection(expected, accepted)
        report = self.programs.restore(self.credentials, destination, selected, self.phrase)
        proof = restored(destination, report, selected, replay, observe)
        inventory = json.dumps(proof.pop("files"), sort_keys=True, separators=(",", ":")).encode()
        proof["native_inventory_sha256"] = hashlib.sha256(inventory).hexdigest()
        node = OwnedNode(destination, self.programs)
        operation.node = node
        reads = []
        lifecycle_reads = []
        temporary_reads = []
        for _number in range(2):
            node.start()
            reads.append(node.verify_records(expected))
            if lifecycle is not None:
                lifecycle_reads.append(lifecycle.verify(node, initial, name == "snapshot-recovery"))
            if temporary is not None:
                temporary_reads.append(temporary.verify_absent(node))
            node.stop()
        require(node.clean_stops == 2, "two_automatic_restore_clean_stops")
        return {"restore": proof, "restored_boots": 2, "strict_clean_stops": 2,
                "keyed_reads": reads, "lifecycle_reads": lifecycle_reads, "temporary_key_reads": temporary_reads}

    def run(self):
        operation = self.operation
        operation.summary["stage"] = "automatic-connect"
        source = Source(OwnedNode(operation.root / "restored", self.programs, backup_source=True,
                                 ci_stream_id=operation.ci_stream_id, terminal_r2=operation.terminal_r2),
                        self.baseline, operation.args.source, operation.summary)
        operation.node = source.node
        source.node.start()
        source.node.verify_records(self.baseline)
        source.connect(operation.api_key)
        lifecycle = Lifecycle(source, self.baseline)
        lifecycle.seed()
        operation.summary["stage"] = "automatic-snapshot"
        source.configure()
        snapshot = source.published("snapshot")
        source.caught_up(snapshot)
        operation.summary["stage"] = "automatic-snapshot-stop"
        source.node.stop()
        source.node.ttl_sweep = True
        operation.summary["stage"] = "automatic-ttl-restart"
        source.node.start()
        operation.summary["stage"] = "ttl-log-expiry"
        first_ttl = lifecycle.await_expiry(2)
        operation.summary["stage"] = "mutation-log"
        source.write(self.baseline["schema"])
        temporary = PrefixRecords(source)
        minimums = temporary.write()
        written = minimums["both_deleted"]
        log = source.published("mutation_log", snapshot["last_accepted_backup"]["commit_sha256"],
                               through=written["frontiers"])
        source.caught_up(log)
        source.retired(snapshot["request_id"])
        timer = snapshot["automatic_backup"]
        require(all(log["automatic_backup"][name] == timer[name]
                    for name in ("last_snapshot_at", "next_snapshot_at")),
                "log_did_not_move_daily_snapshot_timer")
        revision = log["automatic_backup"]["revision"]
        operation.summary["stage"] = "automatic-restart"
        source.node.stop()
        source.node.start()
        policy = source.policy(source.cloud("status"))
        require(policy["revision"] == revision, "automatic_policy_survives_daemon_restart")
        require(policy["next_snapshot_at"] == timer["next_snapshot_at"],
                "saved_snapshot_deadline_survives_restart")
        # A clean stop can add its normal native housekeeping to the journal.
        # Give the unchanged minute schedule time to publish that complete cut.
        time.sleep(65)
        restarted = source.published("mutation_log")
        source.caught_up(restarted)
        source.node.verify_records(source.expected)
        temporary.verify_absent(source.node)
        operation.summary["stage"] = "ttl-snapshot-expiry"
        lifecycle.write(lifecycle.expired[2:])
        second_ttl = lifecycle.await_expiry(4)
        operation.summary["stage"] = "snapshot-interval"
        source.configure(snapshot=60)
        later = source.published("snapshot", snapshot["last_accepted_backup"]["commit_sha256"],
                                 snapshot=60, through=second_ttl)
        source.caught_up(later, snapshot=60)
        temporary.verify_absent(source.node)
        require(later["automatic_backup"]["last_snapshot_at"] > snapshot["automatic_backup"]["last_snapshot_at"],
                "changed_interval_produced_a_later_snapshot")
        operation.summary["stage"] = "automatic-disable"
        source.configure()
        source.retired(restarted["request_id"])
        operation.summary["stage"] = "stopped-backup-automatic-on"
        stopped = StoppedFinal(source)
        final_stopped = stopped.run()
        operation.summary["stage"] = "automatic-disable"
        source.disable()
        source.node.stop()
        dependency_inspections = {"log": self.dependencies(source.expected, log["last_accepted_backup"]),
                                  "snapshot": self.dependencies(source.expected, later["last_accepted_backup"])}
        replay = {"base": snapshot["last_accepted_backup"], "minimum": written, "prefixes": minimums}
        base_restore = self.recover("base-recovery", self.baseline, snapshot["last_accepted_backup"],
                                    lifecycle=lifecycle, initial=True, temporary=temporary)
        log_restore = self.recover("log-recovery", source.expected, log["last_accepted_backup"], replay,
                                   lifecycle=lifecycle, observe=source.observe_accepted, temporary=temporary)
        snapshot_restore = self.recover("snapshot-recovery", source.expected, later["last_accepted_backup"],
                                        lifecycle=lifecycle, temporary=temporary)
        stopped_restore = self.recover("stopped-recovery", stopped.expected, stopped.accepted,
                                       temporary=temporary)
        # The stopped point is not a daemon-initiated v1 recipe point; the log after it is the head.
        ownership = complete_ownership(operation, source, [value["last_accepted_backup"]
                                       for value in (snapshot, log, later)])
        return {"initial_snapshot": True, "minute_mutation_log": True, "daily_snapshot_unchanged_by_log": True,
                "automatic_policy_survives_restart": True, "changed_snapshot_interval_observed": True,
                "log_interval_seconds": 60, "final_snapshot_interval_seconds": 86400,
                "new_file_bytes": source.expected["file_bytes"], "new_file_sha256": source.expected["file_sha256"],
                "mutation_log_routes": log_restore["restore"]["mutation_log_routes"],
                "updated_records": 15, "deleted_records": 1, "observed_checkpoints": len(source.completed),
                "declared_checkpoint_bytes": source.declared_bytes,
                "base_recovery": base_restore, "log_recovery": log_restore,
                "later_snapshot_recovery": snapshot_restore,
                "stopped_backup_with_automatic_on": final_stopped, "stopped_recovery": stopped_restore,
                "ttl_log_frontiers": first_ttl, "ttl_snapshot_frontiers": second_ttl,
                "dependency_inspections": dependency_inspections,
                "ci_stream_ownership": ownership,
                "accepted_backups_retained": True, "provider_deletions": 0,
                "previous_automatic_local_metadata_retired": True,
                "enrollment_requests": 0, "primary_access": False}
