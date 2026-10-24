"""Actual generated condition reads through the existing CI-owned daemon."""

import hashlib
import json

from condition_queries import FIELDS, cases, request, validate
from condition_values import COUNT, HASH
from ranges import latency


def value_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def capture_phases(node, samples):
    # Capture outside each timed cell. The current 256-entry request ring is
    # larger than one 32-request cell; join by both caller and exact request ID.
    response, _ = node.call("/api/status?recent=1")
    operations = response.get("status", {}).get("request_ops", {})
    requested = {sample["request_id"] for sample in samples}
    if len(requested) != len(samples):
        raise RuntimeError("A condition cell repeats a measured request ID")
    matched = {}
    for operation in operations.get("recent", []):
        request_id = operation.get("request_id")
        if request_id not in requested or operation.get("client") != "range-continuation-ci":
            continue
        if request_id in matched:
            raise RuntimeError("The request ring repeats a measured condition request ID")
        matched[request_id] = operation
    if len(matched) != len(samples):
        raise RuntimeError("The request ring lacks complete measured condition request coverage")
    for sample in samples:
        operation = matched[sample["request_id"]]
        sample["server_request_ops"] = {"duration_ms": operation["duration_ms"],
            "phases": {name: value for name, value in operation.get("phases", {}).items()
                       if name in ("hydrate_atoms_us", "hydrate_format_us")}}
    return {"requests": len(samples), "matched": len(matched), "complete": True,
            "ring_capacity": operations.get("ring_capacity")}


class Conditions:
    def __init__(self, node, schema, expected):
        self.node, self.schema = node, schema
        self.expected = {key: value for key, value in expected.items() if key[0] == HASH}
        if not 0 < len(self.expected) <= COUNT:
            raise RuntimeError("The generated keyed condition data exceeds its fixed128-key bound")

    def sample(self, spec, reference, phase, *, conditioned=True):
        result, receipt = self.node.call("/api/query", spec)
        rows = validate(result, self.expected, spec, reference, conditioned=conditioned)
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
                coverage = capture_phases(self.node, samples)
                cells.append({"phase": phase, "label": label, "samples": samples,
                    "latency": latency(samples), "memory_before": before, "memory_after": after,
                    "phase_coverage": coverage})
        short = request(self.schema, ["title"], **{"where": [{"present": {"field": "title"}}],
                        "limit": 7, "offset": 5})
        self.sample(short, reference, "operational")
        # Ordinary reads can stamp primary metadata differently from the
        # condition pass. Keep a separate reference for that established API.
        spec = request(self.schema, FIELDS)
        result, _ = self.node.call("/api/query", spec)
        ordinary_reference = {(row["key"]["hash"], row["key"]["range"]): row
                             for row in validate(result, self.expected, spec, conditioned=False)}
        controls = []
        for conditioned in (False, True):
            for include_tombstones in (False, True):
                options = {"include_tombstones": include_tombstones}
                if conditioned:
                    options["where"] = [{"present": {"field": "title"}}]
                spec = request(self.schema, ["title", "body", "body", "body"], **options)
                label = ("conditional" if conditioned else "ordinary") + "-duplicate-projection"
                if include_tombstones:
                    label += "-include-tombstones"
                controls.append({"label": label, **self.sample(spec,
                    reference if conditioned else ordinary_reference, "operational", conditioned=conditioned)})
        snapshot = [[list(key), value] for key, value in sorted(self.expected.items())]
        return {"cells": cells, "records": len(self.expected), "input_sha256": value_hash(snapshot),
                "projection_controls": controls,
                "metadata_reference": "Same-home full projection with only predicate_limit; no reusable secondary pass A fields",
                "all_values_and_metadata_verified": True, "offset_limit_verified": True,
                "scope": "Warm repeats32/cell; fresh-process1/cell, each after restart; OS cache uncontrolled; no legacy body tombstone or concurrent mutation claim"}


def paired_conditions_match(first, last):
    a, b = first["conditions"], last["conditions"]
    if (a["input_sha256"] != b["input_sha256"] or len(a["cells"]) != 16
            or len(b["cells"]) != 16 or not a["all_values_and_metadata_verified"]
            or not b["all_values_and_metadata_verified"]):
        return False
    controls = a.get("projection_controls", []), b.get("projection_controls", [])
    if any(len(rows) != 4 for rows in controls) or (
            [(s["label"], s["request_sha256"], s["values_sha256"]) for s in controls[0]]
            != [(s["label"], s["request_sha256"], s["values_sha256"]) for s in controls[1]]):
        return False
    for left, right in zip(a["cells"], b["cells"]):
        size = 32 if left["phase"] == "warm" else 1
        if ((left["phase"], left["label"]) != (right["phase"], right["label"])
                or len(left["samples"]) != size or len(right["samples"]) != size
                or any(cell.get("phase_coverage", {}).get("matched") != size
                       or cell.get("phase_coverage", {}).get("complete") is not True
                       or any("server_request_ops" not in sample for sample in cell["samples"])
                       for cell in (left, right))
                or [(s["request_sha256"], s["values_sha256"]) for s in left["samples"]]
                != [(s["request_sha256"], s["values_sha256"]) for s in right["samples"]]):
            return False
    return True
