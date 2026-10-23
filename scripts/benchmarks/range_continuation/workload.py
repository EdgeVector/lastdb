"""A real daemon recipe for complete generated ranges and comparable narrow reads."""

import hashlib
import json

from ranges import COUNT, EDGE_HASH, EDGE_KEYS, FIELDS, HASH, SCHEMA, fields, key, latency, prefix, query, validate, walk


class Workload:
    def __init__(self, node, baseline):
        self.node, self.baseline, self.expected = node, baseline, {}
        self.offered = hashlib.sha256()

    def mutate(self, changes):
        mutations = []
        for action, hashed, ranged, revision in changes:
            values = {} if action in ("delete", "purge") else fields(hashed, ranged, revision)
            if action == "update":
                values = {name: values[name] for name in ("title", "body")}
            mutations.append({"type": "mutation", "schema": SCHEMA, "mutation_type": action,
                "durability": "durable", "key_value": {"hash": hashed, "range": ranged},
                "fields_and_values": values})
        request = {"mutations": mutations}
        result, _ = self.node.call("/api/mutations/batch", request)
        if result.get("count") != len(mutations) or result.get("durability") != "durable":
            raise RuntimeError("The generated mutations lack a complete durable acknowledgement")
        self.offered.update(json.dumps(request, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        for action, hashed, ranged, revision in changes:
            if action in ("delete", "purge"):
                self.expected.pop((hashed, ranged), None)
            else:
                self.expected[(hashed, ranged)] = fields(hashed, ranged, revision)

    def flush(self):
        result, _ = self.node.call("/api/db/flush", {})
        native = result.get("native_flush", {})
        if result.get("durable") is not True or native.get("completed_sequence", -1) < native.get("target_sequence", 0):
            raise RuntimeError("The generated home lacks a complete durable boundary")

    def seed(self):
        self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": FIELDS, "field_descriptions": {name: "range continuation " + name for name in FIELDS}}})
        for first in range(0, COUNT, 128):
            self.mutate([("create", HASH, key(number), "seed") for number in range(first, first + 128)])
        self.mutate([("create", EDGE_HASH, ranged, "seed") for ranged in EDGE_KEYS])
        self.flush()
        self.node.stop()
        self.node.start()

    def keyed(self, label):
        # Name every seeded coordinate, including keys that Delete/Purge removes.
        coordinates = [(HASH, key(number)) for number in range(COUNT)] + [(EDGE_HASH, value) for value in sorted(EDGE_KEYS)]
        for first in range(0, len(coordinates), 256):
            asked = coordinates[first:first + 256]
            request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 256,
                       "filter": {"HashRangeKeys": [list(item) for item in asked]}}
            result, _ = self.node.call("/api/query", request)
            wanted = sorted((item for item in asked if item in self.expected), key=lambda item: (item[1], item[0]))
            validate(result, self.expected, wanted)
            if result.get("has_more") is not False:
                raise RuntimeError("A complete known-key batch unexpectedly continues")
        return {"label": label, "known_coordinates": len(coordinates), "live_records": len(self.expected)}

    def edges(self):
        cases = [("late-half-open", query(HASH, key(16000), key(COUNT), 37)),
            ("late-prefix", prefix(HASH, "key-016", 37)),
            ("empty-interval", query(HASH, key(77), key(77), 7)),
            ("inverted-interval", query(HASH, key(78), key(77), 7)),
            ("empty-prefix", prefix(EDGE_HASH, "", 2)),
            ("empty-range-key", query(EDGE_HASH, "", "a", 2)),
            ("empty-range-only", query(EDGE_HASH, "", "\0", 2)),
            ("nul-prefix", prefix(EDGE_HASH, "a\0", 1)),
            ("nul-boundary", query(EDGE_HASH, "a\0", "aa", 1)),
            ("unicode-boundary", query(EDGE_HASH, "\u00e9", "\uffff", 1)),
            ("unicode-prefix", prefix(EDGE_HASH, "\U00010000", 1))]
        offset = query(HASH, key(16000), key(16128), 17)
        offset["offset"] = 7
        after = query(HASH, key(16000), key(16128), 17)
        after["cursor"] = {"hash": HASH, "range": key(16063)}
        cases.extend([("offset-window", offset), ("exclusive-cursor", after)])
        for hashed in ("a", "z"):
            tie = query(HASH, key(16000), key(16128), 17)
            tie["cursor"] = {"hash": hashed, "range": key(16063)}
            cases.append(("cross-hash-tie-" + hashed, tie))
        return [walk(self.node, self.expected, request, label) for label, request in cases]

    def run(self):
        self.seed()
        known = [self.keyed("seeded-and-reopened")]
        broad = [walk(self.node, self.expected, query(HASH, key(0), key(COUNT)),
                      "broad16384-range", self.baseline),
                 walk(self.node, self.expected, prefix(HASH, "key-"),
                      "broad16384-prefix", self.baseline)]
        broad.append(walk(self.node, self.expected,
            {"schema_name": SCHEMA, "fields": FIELDS, "limit": 256, "filter": {"HashKey": HASH}},
            "broad16384-hash"))
        measured = []
        for ordinal in range(32):
            first = ordinal * (COUNT - 128) // 31
            value = walk(self.node, self.expected, query(HASH, key(first), key(first + 128), 64),
                         "matched-narrow-" + str(ordinal))
            measured.extend(value["samples"])
        boundaries = self.edges()
        self.mutate([("delete", HASH, key(n), "removed") for n in (0, 255, 4095, 16000)]
                    + [("purge", HASH, key(n), "removed") for n in (64, 256, 8192, 16383)]
                    + [("update", HASH, key(16001), "changed")])
        self.flush()
        self.node.stop()
        self.node.start()
        known.append(self.keyed("delete-purge-and-reopened"))
        reopened = walk(self.node, self.expected, query(HASH, key(0), key(COUNT)),
                        "reopened-broad-range", self.baseline, reopened=True)
        preload = None
        if not self.baseline:
            self.node.stop()
            self.node.settings["LASTDB_SCHEMA_PRELOAD_SCHEMAS"] = SCHEMA
            self.node.start()
            before = self.node.status("preload-before")["native"]["native_disk"]["schema_keys"]
            value = walk(self.node, self.expected, query(HASH, key(0), key(COUNT)),
                         "candidate-preload-reopened-range")
            after = self.node.status("preload-after")["native"]["native_disk"]["schema_keys"]
            ready = (after["enabled"] and after["ready"] > 0 and after["disk_only"] == 0
                     and after["keys"] >= len(self.expected)
                     and after["memory_selections"] > before["memory_selections"])
            preload = {"actual_ready_selection_verified": ready, "before": before, "after": after,
                       "walk": value, "scope": "Ready preload selected actual keys" if ready else
                       "Policy requested but ready selection unproved; no preload coverage claim"}
        snapshot = [[hashed, ranged, value] for (hashed, ranged), value in sorted(self.expected.items())]
        return {"records_seeded": COUNT, "edge_records": len(EDGE_KEYS), "known_key_passes": known,
                "broad": broad, "boundaries": boundaries, "narrow_latency": latency(measured),
                "narrow_samples": measured, "reopened": reopened, "preload": preload,
                "ordered_mutation_sha256": self.offered.hexdigest(),
                "final_values_sha256": hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest(),
                "status": self.node.status("final"), "all_values_verified": True,
                "scope": "Broad baseline failures are unavailable operations, never fast latency samples; only identical narrow selectors form latency comparisons"}
