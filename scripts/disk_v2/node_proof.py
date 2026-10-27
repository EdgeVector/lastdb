#!/usr/bin/env python3
"""Disk format v2 node proof (phase V3): a real lastdbd on a new home.

The candidate starts on an empty home. The home must be v2 from the first
start, with no v1 file. The candidate writes a seed with the shape of the
primary (seed.py) under the engine settings of the primary job, so the
records have the envelope forms of the primary (compressed and raw). Then:

1. every key of the seed reads back as the model expects;
2. the candidate writes more, and every key reads back as expected;
3. the home opens again, and every key reads back the same.

After each step the key list of each seed schema (GET /api/list, the
`lastdb list` pages) must hold each live key of the model and no key that
the model does not know. The open must keep the reads, the key lists, and
the logical atom bytes and atom count of each seed schema equal to the step
before it. Only the proof writes the seed schemas, so a change there comes
from the open.

A sampler reads the size of the node folder during the writes, the merges,
and the open. The proof fails when the v2 data is above 2.0x of the logical
atom bytes that the node reports (GET /api/db/schemas, the
`lastdb db schemas` walk), when the node reports under 16 MiB of logical
atoms, because a smaller home does not test the limit, or when the sampler
cannot read the node folder.
"""

import argparse
import hashlib
from pathlib import Path
import sys
import time
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))

from node import Node, save  # noqa: E402  (the benchmark daemon driver)
from sampler import Sampler, folder_sizes, v2_bytes  # noqa: E402
from seed import SCHEMAS, Seed, batch_request, canonical, query_request, schema_request  # noqa: E402
import cleanup  # noqa: E402

LIMIT = 2.0
MINIMUM_LOGICAL = 16 * 1024 * 1024
MIB = 1024 * 1024
READ_KEYS = 100
# The largest page of GET /api/list, and the pages that one schema may take.
LIST_PAGE = 1000
LIST_PAGES = 1000
START_SECONDS = 1800
SERIES_POINTS = 400
NODE_MARKER = "laststore-pack-node-v1"
RUN_NODE = b"format=4\nhome=disk-run\ncas=disk-cas\nranges=typed-v1\n"
# The settings of the primary's launchd job that the node reads: the
# envelope forms at rest (the KV seam compresses and writes binary
# envelopes), the atom size limit, the memory budget, and the admin timeout.
# The node does not read the other LASTDB_* settings of the job.
PRIMARY_SETTINGS = {
    "LASTDB_ENGINE": "laststore",
    "LASTDB_KV_AT_REST_COMPRESS": "1",
    "LASTDB_KV_AT_REST_RAW": "1",
    "LASTDB_AT_REST_RAW": "1",
    "LASTDB_MAX_ATOM_CONTENT_BYTES": "524288",
    "LASTDB_HASH_GROUP_WARM_BYTES": "4294967296",
    "LASTDB_RESIDENT_MAX_DEFERRED_BYTES": "67108864",
    "LASTDB_RSS_BUDGET_MULTIPLIER": "1.75",
    "LASTDBD_RSS_LIMIT_MB": "16384",
    "LASTDB_UDS_ADMIN_TIMEOUT_SECS": "3600",
}
CASE = "new-home"
# The generator number of the seed: the number this case had among the four
# cases of the conversion proof, so the seed data stays the same.
SEED_NUMBER = 2
PROFILE = {
    "seed": {"base": 3000, "creates": 1000, "updates": 4000, "deletes": 250},
    "v2": {"creates": 800, "updates": 3000, "deletes": 150, "recreates": 100},
}


class ProofError(RuntimeError):
    pass


def mib(value):
    return f"{value / MIB:.1f} MiB"


def brief(value):
    text = canonical(value)
    return text if len(text) <= 400 else text[:400] + "..."


def settle(data, seconds=180):
    """Wait until the node folder keeps one size for 3 seconds."""
    deadline, previous = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        current = folder_sizes(data)["allocated"]
        if current == previous:
            return True
        previous = current
        time.sleep(3)
    return False


class Case:
    def __init__(self, candidate, output):
        self.candidate = candidate
        self.settings, self.settings_name = PRIMARY_SETTINGS, "the primary job"
        self.folder = output / CASE
        self.folder.mkdir(parents=True)
        self.node = Node(candidate[0], candidate[1], self.folder, environment=dict(self.settings),
                         start_seconds=START_SECONDS)
        self.data = self.node.home / "data"
        self.sampler = Sampler(self.data)
        self.seed = Seed(SEED_NUMBER)
        self.phases, self.failures = [], []
        self.cleanup_receipt = None

    def execute(self):
        self.sampler.start()
        try:
            self.run()
        except Exception as failure:
            self.failures.append(f"{type(failure).__name__}: {failure}")
        finally:
            self.sampler.stop()
            try:
                self.node.close()
            except Exception as failure:
                self.failures.append(f"cleanup: {type(failure).__name__}: {failure}")
        if self.sampler.errors:
            self.failures.append(f"the sampler could not read the node folder "
                                 f"{len(self.sampler.errors)} times; the first: "
                                 f"{self.sampler.errors[0]}")
        report = {"case": CASE, "candidate": self.candidate[1], "settings": self.settings,
                  "settings_name": self.settings_name, "seed": self.seed.counts,
                  "phases": self.phases, "failures": self.failures,
                  "cleanup": self.cleanup_receipt,
                  "sampler_errors": self.sampler.errors[:20]}
        save(self.folder / "report.json", report)
        return report

    def run(self):
        self.seed_new_home()
        self.write_v2()
        self.reopen("open again", dict(self.settings))

    def seed_new_home(self):
        """The candidate starts on an empty home: the home must be v2 before
        the first write."""
        self.node.start()
        self.check_new_home()
        for schema in SCHEMAS:
            self.node.call("/api/schemas/seed-system", schema_request(schema))
        name = f"seed (new v2 home, {self.candidate[1][:12]})"
        writes = self.write(self.seed.phase(**PROFILE["seed"]))
        # The writes start from an empty home, under the 16 MiB that the limit
        # check needs. The window opens when they end, and stays open until
        # the v2 writes start.
        self.sampler.begin(name)
        settled = settle(self.data)
        logical = self.logical()
        reads = self.reads()
        listed = self.listed(name)
        self.check_new_home()
        self.phases.append({
            "name": name, "writes": writes, "settled": settled,
            "logical": logical, "reads": reads, "listed": listed, **self.disk(),
            "bounds": (logical["bytes"], logical["bytes"])})

    def check_new_home(self):
        marker = (self.data / NODE_MARKER).read_bytes()
        if marker != RUN_NODE:
            raise ProofError(f"a new home has the node marker {marker!r}, not the v2 marker")
        for name in ("disk-pack", "laststore-layout-v1"):
            if (self.data / name).exists():
                raise ProofError(f"a new v2 home has the v1 entry {name}")
        for name in ("disk-run", "disk-cas"):
            if not (self.data / name).is_dir():
                raise ProofError(f"a new v2 home lacks {name}")

    def write_v2(self):
        before = self.phases[-1]["logical"]["bytes"]
        batches = self.seed.phase(**PROFILE["v2"])
        self.close_phase(self.sampler.begin("writes on v2", writes=True))
        writes = self.write(batches)
        settled = settle(self.data)
        logical = self.logical()
        reads = self.reads()
        listed = self.listed("writes on v2")
        self.cleanup_receipt = cleanup.request_and_wait(self.node)
        save(self.folder / "cleanup-request.json", self.cleanup_receipt)
        self.node.stop()
        self.phases.append({
            "name": "writes on v2", "writes": writes, "settled": settled,
            "logical": logical, "reads": reads, "listed": listed, **self.disk(),
            "bounds": (before, logical["bytes"])})
        self.close_phase(self.sampler.end())

    def reopen(self, name, environment):
        self.node.environment = environment
        self.sampler.begin(name)
        self.node.start()
        cleanup.verify_saved(self.node, self.cleanup_receipt)
        settled = settle(self.data)
        logical = self.logical()
        reads = self.reads()
        listed = self.listed(name)
        self.node.stop()
        if (self.data / NODE_MARKER).read_bytes() != RUN_NODE:
            self.failures.append(f"{name}: the node marker is not the v2 marker")
        self.compare(name, logical, reads, listed)
        self.phases.append({
            "name": name, "settled": settled, "logical": logical,
            "reads": reads, "listed": listed, **self.disk(),
            "bounds": (logical["bytes"], logical["bytes"])})
        self.close_phase(self.sampler.end())

    def compare(self, name, logical, reads, listed):
        """A phase without writes must keep the reads, the key lists, and the
        logical atoms of each seed schema of the phase before it."""
        before = self.phases[-1]
        if reads["digest"] != before["reads"]["digest"]:
            self.failures.append(f"{name}: the reads differ from the reads before it")
        changed = [schema for schema in sorted(SCHEMAS)
                   if listed[schema] != before["listed"][schema]]
        if changed:
            self.failures.append(f"{name}: the key lists of {', '.join(changed)} differ from "
                                 "the lists before it")
        old, new = before["logical"]["schemas"], logical["schemas"]
        changed = [f"{schema} from {old.get(schema)} to {new.get(schema)}"
                   for schema in sorted(SCHEMAS) if old.get(schema) != new.get(schema)]
        if changed:
            self.failures.append(f"{name}: the logical [bytes, atoms] of a seed schema "
                                 f"changed: {'; '.join(changed)}")

    def close_phase(self, window):
        """Check the last phase against the samples of its window. Each window
        covers its phase up to the next window, or up to the node stop."""
        phase = self.phases[-1]
        start, end = phase.pop("bounds")
        phase.update(peak=self.gate(phase["name"], window, start, end),
                     samples=len(window["samples"]), torn=window["torn"],
                     seconds=window["seconds"], series=series(window))

    def disk(self):
        """The node folder sizes, and the allocated bytes of each home entry."""
        return {"disk": folder_sizes(self.data), "home": folder_sizes(self.node.home)["entries"]}

    def write(self, batches):
        total = sum(len(batch) for batch in batches)
        done, started = 0, time.monotonic()
        for batch in batches:
            result, _, _ = self.node.call("/api/mutations/batch", batch_request(batch))
            if result.get("count") != len(batch) or result.get("durability") != "durable":
                raise ProofError(f"a write lacks a complete durable acknowledgement: {brief(result)}")
            done += len(batch)
            self.sampler.progress.value = done / total
        result, _, _ = self.node.call("/api/db/flush", {})
        if result.get("durable") is not True:
            raise ProofError(f"the flush after the writes is not durable: {brief(result)}")
        return {"mutations": total, "requests": len(batches),
                "seconds": round(time.monotonic() - started, 1)}

    def logical(self):
        data, elapsed, _ = self.node.call("/api/db/schemas")
        report = data["schema_storage"]
        histogram = report.get("histogram") or {}
        return {"bytes": report["total_logical_bytes"], "atoms": report["total_atoms"],
                "p50": histogram.get("p50"), "milliseconds": round(elapsed),
                "schemas": {row["schema_name"]: [row["bytes"], row["atom_count"]]
                            for row in report["per_schema"]}}

    def reads(self):
        """Read every key of the model in batches. A live key must read its
        exact fields; a deleted key must read nothing."""
        by_schema = {}
        for (schema, hash_value, range_value), fields in sorted(self.seed.expected().items()):
            by_schema.setdefault(schema, []).append(((hash_value, range_value), fields))
        digest, live, absent, requests = hashlib.sha256(), 0, 0, 0
        for schema in sorted(by_schema):
            rows = by_schema[schema]
            for start in range(0, len(rows), READ_KEYS):
                chunk = rows[start:start + READ_KEYS]
                result, _, _ = self.node.call("/api/query",
                                              query_request(schema, [key for key, _ in chunk]))
                requests += 1
                found = {}
                for record in result.get("results", []):
                    key = (record["key"]["hash"], record["key"]["range"])
                    if key in found:
                        raise ProofError(f"{schema}: one read returned {key} two times")
                    found[key] = record["fields"]
                for key, fields in chunk:
                    got = found.pop(key, None)
                    if fields is None:
                        if got is not None:
                            raise ProofError(f"{schema}: the deleted key {key} reads back")
                        absent += 1
                    elif got is None:
                        raise ProofError(f"{schema}: the key {key} is missing")
                    elif canonical(got) != canonical(fields):
                        names = sorted(name for name in set(got) | set(fields)
                                       if canonical(got.get(name, "<absent>"))
                                       != canonical(fields.get(name, "<absent>")))
                        raise ProofError(f"{schema}: the key {key} reads other fields {names}: "
                                         f"read {brief({n: got.get(n, '<absent>') for n in names})}, "
                                         f"expected {brief({n: fields.get(n, '<absent>') for n in names})}")
                    else:
                        digest.update(canonical([schema, list(key), got]).encode())
                        live += 1
                if found:
                    raise ProofError(f"{schema}: a read returned keys that it did not ask for")
        return {"live": live, "absent": absent, "requests": requests, "digest": digest.hexdigest()}

    def listed(self, name):
        """List the keys of each seed schema. A list must hold each live key of
        the model and no key that the model does not know. A list that does
        not is a failure of phase `name`, and the case goes on, so the later
        phases still report. The result counts the listed keys that a delete
        removed; `compare` keeps that count."""
        live, deleted = {}, {}
        for (schema, hash_value, range_value), fields in self.seed.expected().items():
            target = deleted if fields is None else live
            target.setdefault(schema, set()).add((hash_value, range_value))
        result = {}
        for schema in sorted(SCHEMAS):
            expected, removed = live.get(schema, set()), deleted.get(schema, set())
            try:
                keys = self.list_keys(schema)
            except Exception as failure:
                self.failures.append(f"{name}: {schema}: the key list failed: "
                                     f"{type(failure).__name__}: {failure}")
                result[schema] = {"error": f"{type(failure).__name__}: {failure}"[:300]}
                continue
            missing = sorted(expected - keys)
            if missing:
                self.failures.append(f"{name}: {schema}: the key list lacks {len(missing)} "
                                     f"live keys, the first {missing[0]}")
            unknown = sorted(keys - expected - removed)
            if unknown:
                self.failures.append(f"{name}: {schema}: the key list holds {len(unknown)} "
                                     f"keys that the proof did not write, the first {unknown[0]}")
            result[schema] = {"keys": len(keys), "deleted": len(keys & removed),
                              "digest": hashlib.sha256(canonical(sorted(keys)).encode()).hexdigest()}
        return result

    def list_keys(self, schema):
        """Every key that GET /api/list gives for one schema, page by page."""
        keys, cursor = set(), None
        for _ in range(LIST_PAGES):
            path = f"/api/list?schema={quote(schema, safe='')}&limit={LIST_PAGE}"
            if cursor:
                path += f"&cursor={quote(cursor, safe='')}"
            data, _, _ = self.node.call(path)
            page = data["list"]
            for key in page["keys"]:
                pair = (key["hash"], key.get("range", ""))
                if pair in keys:
                    raise ProofError(f"{schema}: the key list gave {pair} two times")
                keys.add(pair)
            if not page["has_more"]:
                return keys
            cursor = page.get("next_cursor")
            if not cursor:
                raise ProofError(f"{schema}: a key list page has more keys and no cursor")
        raise ProofError(f"{schema}: the key list did not end after {LIST_PAGES} pages")

    def gate(self, name, window, start, end):
        """The highest v2 size over the logical atom bytes, sample by sample.
        In a write window the logical bytes go from `start` to `end` with the
        share of acknowledged mutations."""
        if min(start, end) < MINIMUM_LOGICAL:
            self.failures.append(f"{name}: the node reports {mib(min(start, end))} of logical "
                                 "atoms, under the 16 MiB that the limit check needs")
        worst = None
        for sample in window["samples"]:
            logical = start + (end - start) * sample["progress"]
            ratio = sample["v2"] / logical if logical else float("inf")
            if worst is None or ratio > worst["ratio"]:
                worst = {"ratio": round(ratio, 4), "v2_bytes": sample["v2"],
                         "logical_bytes": round(logical), "at_seconds": sample["at"]}
        if worst is None:
            self.failures.append(f"{name}: the sampler took no sample")
        elif worst["ratio"] > LIMIT:
            self.failures.append(f"{name}: the v2 data reached {worst['ratio']:.2f}x of the "
                                 f"logical atom bytes; the limit is {LIMIT}x")
        return worst


def series(window):
    samples = window["samples"]
    step = max(1, len(samples) // SERIES_POINTS)
    return [[sample["at"], sample["v2"], round(sample["progress"], 4)]
            for sample in samples[::step]]


def summary(report, candidate):
    lines = ["## Disk v2 node proof (V3)", "",
             f"Candidate `{candidate}` on a new home.", "",
             f"Limit: the v2 node folder at or under {LIMIT}x of the logical atom bytes "
             "(`GET /api/db/schemas`) at each sample, with at least 16 MiB of logical atoms.", ""]
    verdict = "FAIL" if report["failures"] else "PASS"
    counts = report["seed"]
    lines += [f"### {report['case']}: {verdict}", "",
              f"Engine settings of {report['settings_name']}. Mutations: "
              f"{counts['creates']:,} creates, {counts['updates']:,} updates, "
              f"{counts['deletes']:,} deletes, {counts['field_values']:,} field values.", "",
              "| Phase | Logical atoms | Node folder at end | Peak v2 / logical | Samples "
              "(torn) | Keys read | Keys listed |",
              "|---|---|---|---|---|---|---|"]
    for phase in report["phases"]:
        logical, disk, reads = phase["logical"], phase["disk"], phase["reads"]
        stored = v2_bytes(disk)
        peak = f"{phase['peak']['ratio']:.2f}x" if phase.get("peak") else "no sample"
        samples = f"{phase['samples']} ({phase['torn']})" if "samples" in phase else "-"
        listed = phase.get("listed") or {}
        keys = sum(value.get("keys", 0) for value in listed.values())
        removed = sum(value.get("deleted", 0) for value in listed.values())
        lines.append(
            f"| {phase['name']} | {mib(logical['bytes'])}, {logical['atoms']:,} atoms, "
            f"p50 {logical['p50']} B | {mib(stored)} "
            f"({stored / max(1, logical['bytes']):.2f}x) | "
            f"{peak} | {samples} | {reads['live']:,} equal, "
            f"{reads['absent']:,} absent | {keys:,}, {removed:,} of them deleted |")
    for failure in report["failures"]:
        lines.append(f"- FAIL: {failure}")
    if report.get("cleanup"):
        receipt = report["cleanup"]
        lines.append(f"Explicit cleanup: {receipt['phase']}, both stores, "
                     f"target {receipt['target']}, completed through {receipt['completed_through']}; "
                     "same-id repeat and saved receipt after reopen equal.")
    lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    candidate = (args.candidate.resolve(strict=True), args.candidate_revision)
    report = Case(candidate, args.output).execute()
    save(args.output / "node-proof.json", {"limit": LIMIT, "profile": PROFILE,
                                           "reports": [report]})
    print(summary(report, args.candidate_revision), flush=True)
    if report["failures"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
