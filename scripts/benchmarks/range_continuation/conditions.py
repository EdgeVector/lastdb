"""Actual generated condition reads through the existing CI-owned daemon."""

import hashlib
import json

from condition_queries import FIELDS, cases, request, validate
from condition_values import COUNT, HASH
from ranges import latency


def value_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class Conditions:
    def __init__(self, node, schema, expected):
        self.node, self.schema = node, schema
        self.expected = {key: value for key, value in expected.items() if key[0] == HASH}
        if not 0 < len(self.expected) <= COUNT:
            raise RuntimeError("The generated keyed condition data exceeds its fixed128-key bound")

    def sample(self, spec, reference, phase):
        result, receipt = self.node.call("/api/query", spec)
        rows = validate(result, self.expected, spec, reference)
        costs = receipt["storage_read_receipt"]
        if costs is None or (phase == "fresh-process" and (
                costs["read_operations"] == 0 or costs["payload_bytes"] == 0)):
            raise RuntimeError("A generated condition read lacks its receipt or fresh-process disk reads")
        if phase == "warm" and costs["logical_warm_hits"] == 0:
            raise RuntimeError("A generated warm condition read has no logical warm hit")
        return {"request_id": receipt["request_id"], "elapsed_ms": receipt["elapsed_ms"],
                "request_sha256": value_hash(spec), "records": len(rows),
                "values_sha256": value_hash([[row["key"], row["fields"]] for row in rows]),
                "storage_read_receipt": costs, "reference_metadata_verified": True}

    def run(self):
        # Only a primary pass A field exists in this control. The secondary
        # reader supplies per-home references without reusing condition fields.
        spec = request(self.schema, FIELDS, predicate_limit=COUNT)
        result, _ = self.node.call("/api/query", spec)
        reference = {(row["key"]["hash"], row["key"]["range"]): row
                     for row in validate(result, self.expected, spec)}
        cells = []
        for phase in ("warm", "fresh-process"):
            for label, spec in cases(self.schema):
                if phase == "fresh-process":
                    self.node.stop()
                    self.node.settings.pop("LASTDB_SCHEMA_PRELOAD_SCHEMAS", None)
                    self.node.start()
                before = self.node.status("condition-" + phase + "-" + label + "-before")
                count = 32 if phase == "warm" else 1
                samples = [self.sample(spec, reference, phase) for _ in range(count)]
                after = self.node.status("condition-" + phase + "-" + label + "-after")
                cells.append({"phase": phase, "label": label, "samples": samples,
                    "latency": latency(samples), "memory_before": before, "memory_after": after})
        short = request(self.schema, ["title"], **{"where": [{"present": {"field": "title"}}],
                        "limit": 7, "offset": 5})
        self.sample(short, reference, "operational")
        snapshot = [[list(key), value] for key, value in sorted(self.expected.items())]
        return {"cells": cells, "records": len(self.expected), "input_sha256": value_hash(snapshot),
                "metadata_reference": "Same-home full projection with only predicate_limit; no reusable secondary pass A fields",
                "all_values_and_metadata_verified": True, "offset_limit_verified": True,
                "scope": "Warm repeats32/cell; fresh-process1/cell, each after restart; OS cache uncontrolled; no legacy body tombstone or concurrent mutation claim"}


def paired_conditions_match(first, last):
    a, b = first["conditions"], last["conditions"]
    if (a["input_sha256"] != b["input_sha256"] or len(a["cells"]) != 16
            or len(b["cells"]) != 16 or not a["all_values_and_metadata_verified"]
            or not b["all_values_and_metadata_verified"]):
        return False
    for left, right in zip(a["cells"], b["cells"]):
        size = 32 if left["phase"] == "warm" else 1
        if ((left["phase"], left["label"]) != (right["phase"], right["label"])
                or len(left["samples"]) != size or len(right["samples"]) != size
                or [(s["request_sha256"], s["values_sha256"]) for s in left["samples"]]
                != [(s["request_sha256"], s["values_sha256"]) for s in right["samples"]]):
            return False
    return True
