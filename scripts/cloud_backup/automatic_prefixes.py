"""Write two run-owned keys once, then bind all three durable journal positions."""

import copy

import native_records
from automatic_source import durable_batch
from restore_log import position
from support import canonical_uuid, require


STAGES = ("created", "updated_and_deleted", "both_deleted")


class PrefixRecords:
    def __init__(self, source):
        self.source, self.schema = source, source.expected["schema"]
        stream = canonical_uuid(source.node.ci_stream_id)
        self.keys = {name: "CloudAutomaticPrefix:" + stream + ":" + name for name in ("keep", "remove")}
        require(not set(self.keys.values()) & {record["id"] for record in source.expected["records"]},
                "temporary_keys_exclude_baseline")
        self.created = {key: "created:" + stream + ":" + name for name, key in self.keys.items()}
        self.updated = "updated:" + stream
        self.minimums = {}

    def verify(self, node, expected):
        queries = [{"schema_name": self.schema, "fields": ["id", "note"],
                    "filter": {"HashKey": key}, "limit": 2, "offset": 0} for key in self.keys.values()]
        replies = node.call("/api/queries/batch", {"queries": queries})["results"]
        require(len(replies) == 2, "two_temporary_keyed_reads")
        for key, reply in zip(self.keys.values(), replies):
            value, note = reply["response"], expected[key]
            require(reply["status"] == 200 and value["ok"] is True
                    and not value.get("has_more", False)
                    and value.get("next_cursor") is None and isinstance(value["results"], list)
                    and value["returned_count"] == len(value["results"]) == int(note is not None),
                    "complete_temporary_keyed_read")
            if note is not None:
                record = value["results"][0]
                require(record["key"] == {"hash": key, "range": None}
                        and record["fields"] == {"id": key, "note": note}, "exact_temporary_record")
        return {"keys": 2, "present": sum(value is not None for value in expected.values()),
                "exact_values_verified": True}

    def verify_absent(self, node):
        return self.verify(node, {key: None for key in self.keys.values()})

    def journal(self):
        value = self.source.cloud("status")
        self.source.policy(value)
        journal = value["mutation_log"]
        require(journal["enabled"] is True and journal["gap"] is None
                and journal["snapshot_required"] is False, "temporary_mutation_complete_journal")
        return copy.deepcopy(position(journal["head"]))

    def mutate(self, stage, mutations, expected):
        before = self.journal()
        durable_batch(self.source.node, mutations)
        after = self.journal()
        require(before["epoch"] == after["epoch"] and before["sequence"] < after["sequence"]
                and all(before["frontiers"][name] <= after["frontiers"][name] for name in ("main", "cas")),
                "new_durable_temporary_journal_position")
        self.minimums[stage] = after
        self.verify(self.source.node, expected)

    def write(self):
        self.verify_absent(self.source.node)
        creates = [native_records.create(self.schema, native_records.key(key), {"id": key, "note": note})
                   for key, note in self.created.items()]
        self.mutate("created", creates, self.created)
        keep, remove = self.keys["keep"], self.keys["remove"]
        rows = native_records.read_records(self.source.node, self.schema,
                                           [native_records.key(key) for key in (keep, remove)])
        for key, row in zip((keep, remove), rows):
            require(row["fields"] == {"id": key, "note": self.created[key]},
                    "temporary_created_preimage")
        changes = [native_records.replace(self.schema, rows[0], {"id": keep, "note": self.updated}),
                   native_records.delete(self.schema, rows[1])]
        self.mutate("updated_and_deleted", changes, {keep: self.updated, remove: None})
        row = native_records.read_records(self.source.node, self.schema, [native_records.key(keep)])[0]
        require(row["fields"] == {"id": keep, "note": self.updated}, "temporary_updated_preimage")
        deletes = [native_records.delete(self.schema, row)]
        self.mutate("both_deleted", deletes, {keep: None, remove: None})
        require(set(self.minimums) == set(STAGES)
                and self.minimums["created"]["epoch"] == self.minimums["updated_and_deleted"]["epoch"]
                    == self.minimums["both_deleted"]["epoch"]
                and self.minimums["created"]["sequence"] < self.minimums["updated_and_deleted"]["sequence"]
                    < self.minimums["both_deleted"]["sequence"], "three_distinct_durable_mutation_prefixes")
        return copy.deepcopy(self.minimums)
