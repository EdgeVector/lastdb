"""Finite generated mutations through the ordinary mutation manager, outside query timing."""

import hashlib
import json
import os
from pathlib import Path
import subprocess

from node import file_hash
from query import COUNT, EDGE_HASH, EDGE_KEYS, FIELDS, HASH, HIDDEN_INTERVALS, HIDDEN_SCHEMA, SCHEMA, SHIFT, TOTAL, fields, hidden, key
from condition_values import COUNT as CONDITION_COUNT, HASH as CONDITION_HASH, EXTRA_FIELDS, marker


class Seed:
    def __init__(self, node, helper):
        self.node, self.helper = node, helper
        self.sources = {source: {} for source in (None, 0, 1)}
        self.separate = {}
        self.offered, self.ordinal, self.paths = hashlib.sha256(), 0, []

    def schema(self):
        self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
            "schema_type": "HashRange",
            "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": FIELDS + EXTRA_FIELDS, "field_types": {"bucket": "String", "id": "String"},
            "field_descriptions": {
                name: "received range batches " + name for name in FIELDS + EXTRA_FIELDS}}})
        alternate_fields = FIELDS + EXTRA_FIELDS
        alternate_descriptions = {name: "received range batches " + name for name in alternate_fields}
        self.node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": HIDDEN_SCHEMA, "descriptive_name": HIDDEN_SCHEMA,
            "schema_type": "HashRange",
            "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": alternate_fields, "field_types": {"bucket": "String", "id": "String"},
            "field_descriptions": alternate_descriptions}})
        self.node.call("/api/db/flush", {})
        self.node.stop()
        (self.node.home / ".received-range-ci").write_text("generated CI input only\n")

    def batch(self, stream, source, coordinates, action="create", revision="seed", alternate=False):
        mutations = []
        for hashed, ranged in coordinates:
            self.ordinal += 1
            value = {} if action in ("delete", "purge") else fields(hashed, ranged, source, revision)
            mutations.append({"uuid": f"00000000-0000-4000-8000-{self.ordinal:012x}",
                "schema_name": HIDDEN_SCHEMA if alternate else SCHEMA, "fields_and_values": value,
                "key_value": {"hash": hashed, "range": ranged}, "pub_key": "generated-ci",
                "mutation_type": action, "synchronous": True,
                "source_file_name": marker(source) if hashed == CONDITION_HASH else None})
            if alternate:
                self.separate[(hashed, ranged)] = value
            elif action in ("delete", "purge"):
                self.sources[source].pop((hashed, ranged), None)
            else:
                self.sources[source][(hashed, ranged)] = value
        raw = json.dumps({"source": source, "mutations": mutations}, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.offered.update(raw)
        stream.write(raw)

    def operate(self, path):
        environment = {name: value for name, value in os.environ.items()
            if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))
            and name not in ("GH_TOKEN", "GITHUB_TOKEN")}
        environment.update(self.node.settings, LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
            LASTDB_HOME=str(self.node.home), FOLDDB_HOME=str(self.node.home), RUST_MIN_STACK="16777216")
        log_path = self.node.output / "seed.log"
        with log_path.open("ab") as log:
            result = subprocess.run([str(self.helper), str(self.node.home), str(path)], env=environment,
                stdin=subprocess.DEVNULL, stdout=log, stderr=log, check=False, timeout=600)
        if result.returncode != 0:
            # The helper prints its error last; keep it in the job log as well as the artifact.
            tail = log_path.read_bytes()[-2000:].decode(errors="replace")
            raise RuntimeError("The seed helper exits with status " + str(result.returncode) + ": " + tail)
        self.paths.append({"name": path.name, "sha256": file_hash(path), "bytes": path.stat().st_size})
        self.node.start()

    def populate(self):
        self.schema()
        path = self.node.output / "seed-input.jsonl"
        with path.open("xb") as stream:
            for source in (0, 1):
                shift = source * SHIFT
                for first in range(shift, shift + COUNT, 128):
                    self.batch(stream, source, [(HASH, key(number)) for number in range(first, first + 128)])
                self.batch(stream, source, [(EDGE_HASH, value) for value in EDGE_KEYS])
                for first in range(0, CONDITION_COUNT, 64):
                    self.batch(stream, source, [(CONDITION_HASH, key(number))
                                               for number in range(first, first + 64)])
            local = [(HASH, key(number)) for number in range(0, TOTAL, 16) if not hidden(number)]
            local.extend((EDGE_HASH, value) for value in EDGE_KEYS[::2])
            local.extend((CONDITION_HASH, key(number)) for number in range(0, CONDITION_COUNT, 16))
            for first in range(0, len(local), 128):
                self.batch(stream, None, local[first:first + 128])
            for start, end in HIDDEN_INTERVALS:
                for first in range(start, end, 128):
                    self.batch(stream, None, [(HASH, key(number)) for number in range(first, min(first + 128, end))],
                               alternate=True)
        self.operate(path)

    def changed(self):
        self.node.stop()
        path = self.node.output / "changed-input.jsonl"
        with path.open("xb") as stream:
            self.batch(stream, 0, [(HASH, key(number)) for number in (17, 255, 4095)], "delete")
            self.batch(stream, 1, [(HASH, key(number)) for number in (20001, 20002, 24575)], "purge")
            self.batch(stream, None, [(HASH, key(16000))], "update", "changed-local")
            self.batch(stream, 0, [(HASH, key(16001))], "update", "changed-source-zero")
            self.batch(stream, 0, [(CONDITION_HASH, key(2))], "delete")
            self.batch(stream, 1, [(CONDITION_HASH, key(3))], "purge")
            self.batch(stream, None, [(CONDITION_HASH, key(0))], "delete")
        self.operate(path)

    def expected(self):
        result = {}
        for source in (1, 0, None):
            for coordinate, value in self.sources[source].items():
                result[coordinate] = value
        return result

    def final_hash(self):
        snapshot = [[hashed, ranged, value] for (hashed, ranged), value in sorted(self.expected().items())]
        separate = [[hashed, ranged, value] for (hashed, ranged), value in sorted(self.separate.items())]
        return hashlib.sha256(json.dumps([snapshot, separate], sort_keys=True).encode()).hexdigest()
