"""Actual daemon reads from independently seeded received and local scopes."""

from query import COUNT, EDGE_HASH, EDGE_KEYS, FIELDS, HASH, SCHEMA, TOTAL, key, latency, prefix, query, validate, walk
from ranges import comparison_tag
from seed import Seed
from conditions import Conditions, capture_phases


class Workload:
    def __init__(self, node, baseline, helper):
        self.node, self.baseline, self.seed = node, baseline, Seed(node, helper)

    def keyed(self, label):
        expected = self.seed.expected()
        coordinates = [(HASH, key(number)) for number in range(TOTAL)]
        coordinates.extend((EDGE_HASH, value) for value in sorted(EDGE_KEYS))
        for first in range(0, len(coordinates), 256):
            asked = coordinates[first:first + 256]
            request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 256,
                       "filter": {"HashRangeKeys": [list(item) for item in asked]}}
            result, _ = self.node.call("/api/query", request)
            wanted = sorted((item for item in asked if item in expected), key=lambda item: (item[1], item[0]))
            validate(result, expected, wanted)
            if result.get("has_more") is not False:
                raise RuntimeError("A complete received known-key batch unexpectedly continues")
        return {"label": label, "known_coordinates": len(coordinates), "live_records": len(expected)}

    def broad(self, label):
        expected = self.seed.expected()
        selectors = [("range", query(key(0), key(TOTAL))), ("prefix", prefix()),
            ("hash", {"schema_name": SCHEMA, "fields": FIELDS, "limit": 256,
                      "filter": {"HashKey": HASH}})]
        return [walk(self.node, expected, request, label + "-" + name, self.baseline)
                for name, request in selectors]

    def edges(self):
        cases = [("overlap-boundary", query(key(8160), key(8240), 7)),
            ("second-source-only", query(key(23000), key(23500), 37)),
            ("all-hidden-tail", query(key(24000), key(TOTAL), 37)),
            ("hidden-prefix-refill", query(key(5000), key(5800), 37)),
            ("empty-interval", query(key(77), key(77), 7)),
            ("inverted-interval", query(key(78), key(77), 7)),
            ("empty-prefix", prefix("", 2, EDGE_HASH)),
            ("empty-only", query("", "\0", 2, EDGE_HASH)),
            ("nul-prefix", prefix("a\0", 1, EDGE_HASH)),
            ("unicode-boundary", query("\u00e9", "\uffff", 1, EDGE_HASH)),
            ("unicode-prefix", prefix("\U00010000", 1, EDGE_HASH))]
        offset = query(key(8200), key(8328), 17)
        offset["offset"] = 71
        cases.append(("offset-after-dedup", offset))
        tag = comparison_tag(self.node, query(key(8200), key(8328), 17))
        for hashed in ("a", HASH, "z"):
            after = query(key(8200), key(8328), 17)
            after["cursor"] = {"hash": hashed, "range": key(8263), "comparison": tag}
            cases.append(("cross-hash-cursor-" + hashed, after))
        projections = [("range-field-only", query(key(8180), key(8250), 17), ["id"]),
            ("range-without-range-field", query(key(8180), key(8250), 17), ["bucket", "title", "body"]),
            ("prefix-without-range-field", prefix("key-00819", 7), ["title"]),
            ("hash-without-range-field", {"schema_name": SCHEMA, "limit": 3,
                "filter": {"HashKey": EDGE_HASH}}, ["body"])]
        cases.extend((label, {**request, "fields": fields}) for label, request, fields in projections)
        return [walk(self.node, self.seed.expected(), request, label) for label, request in cases]

    def preload(self):
        if self.baseline:
            return None
        self.node.stop()
        self.node.settings["LASTDB_SCHEMA_PRELOAD_SCHEMAS"] = SCHEMA
        self.node.start()
        before = self.node.status("preload-before")["native"]["native_disk"]["schema_keys"]
        broad = self.broad("preload-reopened")
        after = self.node.status("preload-after")["native"]["native_disk"]["schema_keys"]
        ready = (after["enabled"] and after["ready"] > 0 and after["disk_only"] == 0
                 and after["keys"] >= len(self.seed.expected())
                 and after["memory_selections"] > before["memory_selections"])
        return {"actual_ready_selection_verified": ready, "before": before, "after": after,
                "broad": broad, "scope": "Ready preload selected actual keys" if ready else
                "Policy requested but ready received selection remains unproved"}

    def run(self):
        self.seed.populate()
        broad = self.broad("seeded")
        known = [self.keyed("complete-known-key-prefill-immediately-before-latency")]
        memory_before = self.node.status("immediately-before-narrow-latency")
        measured = []
        self.node.capture_process = True
        try:
            for ordinal in range(32):
                first = ordinal * (TOTAL - 128) // 31
                if first < 5640 and first + 128 > 5000:
                    first = 6000
                if first + 128 > 24000:
                    first = 23000
                value = walk(self.node, self.seed.expected(), query(key(first), key(first + 128), 64),
                             "matched-narrow-" + str(ordinal))
                measured.extend(value["samples"])
        finally:
            self.node.capture_process = False
        memory_after = self.node.status("immediately-after-narrow-latency")
        narrow_coverage = capture_phases(self.node, measured, full_records=True)
        boundaries = self.edges()
        self.seed.changed()
        known.append(self.keyed("delete-purge-update-and-reopened"))
        reopened = self.broad("changed-reopened")
        conditions = Conditions(self.node, SCHEMA, self.seed.expected()).run()
        preload = self.preload()
        return {"received_records_per_source": COUNT, "sources": 2, "overlap_records": 8192,
            "known_key_passes": known, "broad": broad, "boundaries": boundaries,
            "reopened": reopened, "preload": preload, "narrow_latency": latency(measured),
            "narrow_samples": measured, "ordered_mutation_sha256": self.seed.offered.hexdigest(),
            "narrow_phase_coverage": narrow_coverage,
            "narrow_memory_before": memory_before, "narrow_memory_after": memory_after,
            "conditions": conditions,
            "final_values_sha256": self.seed.final_hash(), "seed_inputs": self.seed.paths,
            "status": self.node.status("final"), "all_values_verified": True,
            "scope": "Generated received-data query operations; seed time excluded; no cloud transport or power-loss claim"}
