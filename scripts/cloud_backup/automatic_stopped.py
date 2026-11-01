"""A final stopped snapshot while automatic backup stays on, then the next automatic log.

A program upgrade must keep automatic backup on, so its final recovery point
comes from `backup-v2 start-stopped` on that enabled policy. The automatic log
of the restarted source must then continue in the stopped snapshot's log epoch.
That log is also the stream head, which the ownership receipt must name.
"""

import copy
import time
import uuid

from automatic_source import completion, durable_batch
from support import ACCOUNT, OperationError, canonical_uuid, require

# A clean stop needs only seconds; the next log tick must not start before it.
MARGIN_SECONDS = 30


def frontiers(value):
    require(isinstance(value, dict) and set(value) == {"main", "cas"}
            and all(type(number) is int and 0 <= number < 2**64 for number in value.values()),
            "stopped_frontiers")
    return {"main": value["main"], "cas": value["cas"]}


def cut(profile):
    # The run profile serializes each frontier as a decimal string.
    values = {name: profile[name]["frontier"] for name in ("main", "cas")}
    require(all(isinstance(value, str) and value.isdigit() and str(int(value)) == value
                for value in values.values()), "stopped_run_profile_frontier")
    return frontiers({name: int(value) for name, value in values.items()})


def covers(after, before):
    return all(after[name] >= before[name] for name in ("main", "cas"))


def ordinal(accepted):
    value = accepted["ordinal"]
    require(isinstance(value, str) and value.isdigit() and str(int(value)) == value, "accepted_ordinal")
    return int(value)


class StoppedFinal:
    def __init__(self, source):
        self.source = source
        stream = canonical_uuid(source.node.ci_stream_id)
        self.key = "CloudStoppedFinal:" + stream
        require(self.key not in {record["id"] for record in source.expected["records"]},
                "stopped_key_excludes_baseline")
        self.notes = ("stopped-final:" + stream, "after-stopped-final:" + stream)
        self.expected, self.accepted = None, None

    def write(self, note, previous):
        """One durable create or update of the run-owned key; the baseline records stay unchanged."""
        source = self.source
        item = {"type": "mutation", "schema": source.expected["schema"],
                "key_value": {"hash": self.key, "range": None},
                "fields_and_values": {"id": self.key, "note": note, "file": source.expected["file_pointer"]},
                "mutation_type": "create" if previous is None else "update", "durability": "durable",
                "expected": ({"type": "absent", "field": "id"} if previous is None
                             else {"type": "value", "field": "note", "value": previous})}
        if previous is not None:
            item["must_exist"] = True
        durable_batch(source.node, [item])
        expected = copy.deepcopy(source.expected)
        expected["records"].append({"id": self.key, "note": note})
        source.node.verify_records(expected)
        value = source.cloud("status")
        source.policy(value)
        head = value["mutation_log"]["head"]
        require(head is not None, "stopped_write_journal_head")
        return expected, value, frontiers(head["frontiers"])

    def idle(self):
        """A released, complete journal with most of one log interval before the next tick."""
        source = self.source
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            value = source.cloud("status")
            policy = source.policy(value)
            if (all(completion(value, policy).values()) and not value["running"]
                    and value["retained_source_bytes"] == 0 and value["last_accepted_backup"] is not None
                    and policy["pending_request_id"] is None
                    and policy["attempts"] == policy["retry_at"] == 0
                    and policy["next_log_at"] - time.time() >= MARGIN_SECONDS):
                return value
            time.sleep(0.5)
        raise OperationError("stopped_backup_idle_automatic_owner")

    def backup(self, before, written):
        """Stop cleanly, then back up the exact stopped session with automatic backup on."""
        source = self.source
        node = source.node
        pid, start = node.process.pid, node.start_ts
        node.stop()
        request = str(uuid.uuid4())
        require(not request.startswith("a17dbac2-"), "stopped_request_not_automatic")
        report = node.programs.command(node.home, ["cloud", "backup-v2", "start-stopped",
            "--request-id", request, "--expected-user-hash", ACCOUNT, "--stopped-pid", str(pid),
            "--stopped-start-ts", str(start), "--json"], extra=node.ci_uploads(), timeout=900)
        accepted = report["accepted_backup"]
        begin = accepted["begin"]
        predecessor = begin["predecessor"]
        stopped = cut(report["run_profile"])
        require(report["version"] == 1 and report["request"] == {
                    "request_id": request, "account_id": ACCOUNT,
                    "stopped_pid": pid, "stopped_start_ts": start}
                and report["automatic_backup_enabled"] is True
                and report["all_remote_file_hashes_verified"] is True
                and report["source_released"] is True and report["native_cut_unchanged"] is True
                and type(report["verified_remote_objects"]) is int and report["verified_remote_objects"] > 0
                and type(report["native_files"]) is int and report["native_files"] > 0
                and begin["job"]["commit_id"] == request
                and isinstance(predecessor, dict)
                and predecessor["commit_sha256"] == before["last_accepted_backup"]["commit_sha256"]
                and covers(stopped, written), "stopped_backup_with_automatic_on")
        source.bind_stopped(accepted)
        return report, stopped

    def run(self):
        source = self.source
        before = self.idle()
        policy = before["automatic_backup"]
        backed = frontiers(policy["last_backed_up_frontiers"])
        first, after_write, written = self.write(self.notes[0], None)
        late = after_write["automatic_backup"]
        require(late["pending_request_id"] is None and late["revision"] == policy["revision"]
                and frontiers(late["last_backed_up_frontiers"]) == backed
                and covers(written, backed) and written != backed, "final_write_ahead_of_automatic_backup")
        report, stopped = self.backup(before, written)
        self.expected, self.accepted = first, report["accepted_backup"]
        source.node.start()
        restarted = source.cloud("status")
        restarted_policy = source.policy(restarted)
        journal = restarted["mutation_log"]
        anchor = journal["anchor"]
        require(restarted_policy["revision"] == policy["revision"]
                and isinstance(anchor, dict) and frontiers(anchor["frontiers"]) == stopped
                and journal["gap"] is None and journal["snapshot_required"] is False
                and covers(frontiers(restarted_policy["last_backed_up_frontiers"]), stopped),
                "automatic_policy_records_stopped_snapshot")
        source.node.verify_records(first)
        _final, _moved, through = self.write(self.notes[1], self.notes[0])
        log = source.published("mutation_log", self.accepted["commit_sha256"], through=through)
        source.caught_up(log)
        latest = source.cloud("status")["mutation_log"]
        accepted = log["last_accepted_backup"]
        require(latest["anchor"] is not None and latest["anchor"]["epoch"] == anchor["epoch"]
                and latest["head"]["epoch"] == anchor["epoch"]
                and ordinal(accepted) > ordinal(self.accepted), "automatic_log_on_stopped_snapshot")
        predecessor = accepted["begin"]["predecessor"]
        return {"request_id": self.accepted["begin"]["job"]["commit_id"],
                "commit_sha256": self.accepted["commit_sha256"],
                "automatic_backup_enabled": True, "automatic_policy_revision_unchanged": True,
                "final_write_after_last_automatic_backup": True,
                "previous_automatic_head_is_predecessor": True,
                "verified_remote_objects": report["verified_remote_objects"],
                "native_files": report["native_files"], "native_bytes": report["native_bytes"],
                "automatic_backed_before_stop": backed, "stopped_cut": stopped,
                "restart_log_anchor_is_stopped_cut": True,
                "next_automatic_log": {"request_id": accepted["begin"]["job"]["commit_id"],
                                       "commit_sha256": accepted["commit_sha256"],
                                       "same_log_epoch": True,
                                       "direct_successor": isinstance(predecessor, dict)
                                       and predecessor["commit_sha256"] == self.accepted["commit_sha256"]}}
