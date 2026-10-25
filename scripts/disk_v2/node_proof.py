#!/usr/bin/env python3
"""Disk format v2 node proof (phase V1b): a real lastdbd converts a v1 home.

The home has the shape of the primary (seed.py). In case "format-1" the
program that runs the primary writes the seed: node format 1, key format 5,
and an outer envelope on each record. In case "format-2" the candidate writes
it as a v1 home of node format 2. These two cases run with the engine
settings of the primary job, so the records have the envelope forms of the
primary (compressed and raw). Case "format-1-defaults" repeats format-1 with
the default settings. In each case:

1. the seed program writes the seed, and every key reads back as the model
   expects;
2. the candidate opens the home with LASTDB_DISK_FORMAT=2, which converts it;
   every key reads back the same, and the v1 files stay in the kept folder;
3. the candidate writes more on v2, and every key reads back as expected;
4. the home opens again with the variable (no second conversion), then
   without it, and every key reads back the same.

A sampler reads the size of the node folder during the conversion, the
writes, the merges, and each open. The proof fails when the v2 data is above
2.0x of the logical atom bytes that the node reports (GET /api/db/schemas,
the `lastdb db schemas` walk), or when the node reports under 16 MiB of
logical atoms, because a smaller home does not test the limit.
"""

import argparse
import hashlib
from pathlib import Path
import re
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))

from node import Node, save  # noqa: E402  (the benchmark daemon driver)
from sampler import Sampler, folder_sizes, v2_bytes  # noqa: E402
from seed import SCHEMAS, Seed, batch_request, canonical, query_request, schema_request  # noqa: E402

LIMIT = 2.0
MINIMUM_LOGICAL = 16 * 1024 * 1024
MIB = 1024 * 1024
READ_KEYS = 100
START_SECONDS = 1800
SERIES_POINTS = 400
NODE_MARKER = "laststore-pack-node-v1"
FORMAT_1 = b"format=1\nhome=disk-pack\n"
FORMAT_2 = b"format=2\nhome=disk-pack\n"
# The settings of the primary's launchd job that both programs read: the
# envelope forms at rest (the KV seam compresses and writes binary
# envelopes), the atom size limit, the memory budget, and the admin timeout.
# Neither program reads the other LASTDB_* settings of the job.
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
# Case: (the program that writes the seed, its node marker, the engine settings).
CASES = {
    "format-1": ("primary", FORMAT_1, PRIMARY_SETTINGS),
    "format-2": ("candidate", FORMAT_2, PRIMARY_SETTINGS),
    "format-1-defaults": ("primary", FORMAT_1, {}),
}
RUN_NODE = b"format=3\nhome=disk-run\ncas=disk-cas\n"
KEPT = "data-disk-format-v1"
CONVERTED = "disk format conversion to v2 complete"
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


def listing(folder):
    """(relative path, size) of every file under folder, to see a change."""
    return sorted((str(path.relative_to(folder)), path.stat().st_size)
                  for path in folder.rglob("*") if path.is_file())


def log_size(path):
    return path.stat().st_size if path.exists() else 0


def conversion_report(path, start):
    """The values of the conversion line that the node logged after `start`."""
    if not path.exists():
        return None
    with path.open("rb") as stream:
        stream.seek(start)
        text = stream.read().decode(errors="replace")
    for line in text.splitlines():
        if CONVERTED not in line:
            continue
        plain = re.sub(r"\x1b\[[0-9;]*m", "", line)
        values = {}
        for name in ("records", "logical_bytes", "v1_disk_bytes", "v2_disk_bytes", "seconds"):
            found = re.search(rf"\b{name}=([0-9.]+)", plain)
            if found:
                values[name] = float(found.group(1))
        source = re.search(r"source_format=(\w+ \{ fenced: \w+ \})", plain)
        values["source_format"] = source.group(1) if source else None
        return values
    return None


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
    def __init__(self, name, seeder, candidate, output, number):
        self.name, self.seeder, self.candidate = name, seeder, candidate
        _, self.v1_marker, self.settings = CASES[name]
        self.folder = output / name
        self.folder.mkdir(parents=True)
        self.node = Node(seeder[0], seeder[1], self.folder, environment=dict(self.settings),
                         start_seconds=START_SECONDS)
        self.data, self.kept = self.node.home / "data", self.node.home / KEPT
        self.sampler = Sampler(self.data)
        self.seed = Seed(number)
        self.phases, self.failures = [], []

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
        report = {"case": self.name, "seed_program": self.seeder[1],
                  "candidate": self.candidate[1], "settings": self.settings,
                  "seed": self.seed.counts,
                  "phases": self.phases, "failures": self.failures,
                  "sampler_errors": self.sampler.errors[:20]}
        save(self.folder / "report.json", report)
        return report

    def run(self):
        self.seed_v1()
        self.convert()
        self.write_v2()
        kept = listing(self.kept)
        self.reopen("open again with LASTDB_DISK_FORMAT=2",
                    {**self.settings, "LASTDB_DISK_FORMAT": "2"})
        self.reopen("open again without LASTDB_DISK_FORMAT", dict(self.settings))
        if listing(self.kept) != kept:
            self.failures.append("the kept v1 folder changed after the conversion")

    def seed_v1(self):
        self.node.start()
        for name in SCHEMAS:
            self.node.call("/api/schemas/seed-system", schema_request(name))
        writes = self.write(self.seed.phase(**PROFILE["seed"]))
        logical = self.logical()
        reads = self.reads()
        self.node.stop()
        marker = (self.data / NODE_MARKER).read_bytes()
        if marker != self.v1_marker:
            raise ProofError(f"the seed home has the node marker {marker!r}")
        self.phases.append({"name": f"seed (v1, {self.seeder[1][:12]})", "gated": False,
                            "writes": writes, "logical": logical, "reads": reads,
                            **self.disk()})

    def convert(self):
        self.node.binary, self.node.revision = self.candidate
        self.node.environment = {**self.settings, "LASTDB_DISK_FORMAT": "2"}
        log_start = log_size(self.folder / "node.log")
        self.sampler.begin("conversion")
        started = time.monotonic()
        self.node.start()
        seconds = round(time.monotonic() - started, 1)
        self.check_converted()
        settled = settle(self.data)
        logical = self.logical()
        reads = self.reads()
        before = self.phases[-1]
        if reads["digest"] != before["reads"]["digest"]:
            self.failures.append("conversion: the reads differ from the reads before it")
        least = min(logical["bytes"], before["logical"]["bytes"])
        # The window stays open: the node runs on until the v2 writes start.
        self.phases.append({
            "name": "conversion (LASTDB_DISK_FORMAT=2)", "gated": True, "start_seconds": seconds,
            "settled": settled, "logical": logical, "reads": reads, **self.disk(),
            "kept_v1": folder_sizes(self.kept), "bounds": (least, least),
            "report": conversion_report(self.folder / "node.log", log_start)})

    def check_converted(self):
        marker = (self.data / NODE_MARKER).read_bytes()
        if marker != RUN_NODE:
            raise ProofError(f"after the conversion the node marker is {marker!r}")
        if (self.kept / NODE_MARKER).read_bytes() != self.v1_marker:
            raise ProofError("the kept folder lacks the v1 node marker")
        for name in ("disk-pack", "laststore-layout-v1"):
            if (self.data / name).exists() or not (self.kept / name).exists():
                raise ProofError(f"{name} is not in the kept folder only")
        for name in ("disk-run", "disk-cas"):
            if not (self.data / name).is_dir():
                raise ProofError(f"the v2 node folder lacks {name}")
        for name in ("disk-run.converting", "disk-cas.converting", "laststore-disk-convert-v2"):
            if (self.data / name).exists():
                raise ProofError(f"the conversion left {name}")

    def write_v2(self):
        before = self.phases[-1]["logical"]["bytes"]
        batches = self.seed.phase(**PROFILE["v2"])
        self.close_phase(self.sampler.begin("writes on v2", writes=True))
        writes = self.write(batches)
        settled = settle(self.data)
        logical = self.logical()
        reads = self.reads()
        self.node.stop()
        self.phases.append({
            "name": "writes on v2", "gated": True, "writes": writes, "settled": settled,
            "logical": logical, "reads": reads, **self.disk(),
            "bounds": (before, logical["bytes"])})
        self.close_phase(self.sampler.end())

    def reopen(self, name, environment):
        self.node.environment = environment
        log_start = log_size(self.folder / "node.log")
        self.sampler.begin(name)
        self.node.start()
        settled = settle(self.data)
        logical = self.logical()
        reads = self.reads()
        self.node.stop()
        if (self.data / NODE_MARKER).read_bytes() != RUN_NODE:
            self.failures.append(f"{name}: the node marker is not the v2 marker")
        if conversion_report(self.folder / "node.log", log_start):
            self.failures.append(f"{name}: the node converted the home again")
        if reads["digest"] != self.phases[-1]["reads"]["digest"]:
            self.failures.append(f"{name}: the reads differ from the reads before it")
        self.phases.append({
            "name": name, "gated": True, "settled": settled, "logical": logical,
            "reads": reads, **self.disk(), "bounds": (logical["bytes"], logical["bytes"])})
        self.close_phase(self.sampler.end())

    def close_phase(self, window):
        """Check the last phase against the samples of its window. Each window
        covers its phase up to the next window, or up to the node stop."""
        phase = self.phases[-1]
        start, end = phase.pop("bounds")
        phase.update(peak=self.gate(phase["name"], window, start, end),
                     samples=len(window["samples"]), seconds=window["seconds"],
                     series=series(window))

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


def summary(reports, primary, candidate):
    lines = ["## Disk v2 node proof (V1b)", "",
             f"Candidate `{candidate}`. Program that runs the primary: `{primary}`.", "",
             f"Limit: the v2 node folder at or under {LIMIT}x of the logical atom bytes "
             "(`GET /api/db/schemas`) at each sample, with at least 16 MiB of logical atoms.", ""]
    for report in reports:
        verdict = "FAIL" if report["failures"] else "PASS"
        counts = report["seed"]
        settings = "the primary job" if report["settings"] else "the defaults"
        lines += [f"### {report['case']}: {verdict}", "",
                  f"Seed program `{report['seed_program'][:12]}`, engine settings of {settings}. Mutations: "
                  f"{counts['creates']:,} creates, {counts['updates']:,} updates, "
                  f"{counts['deletes']:,} deletes, {counts['field_values']:,} field values.", "",
                  "| Phase | Logical atoms | Node folder at end | Peak v2 / logical | Samples | Keys read |",
                  "|---|---|---|---|---|---|"]
        for phase in report["phases"]:
            logical, disk, reads = phase["logical"], phase["disk"], phase["reads"]
            stored = v2_bytes(disk) if phase["gated"] else max(disk["allocated"], disk["apparent"])
            if phase.get("peak"):
                peak = f"{phase['peak']['ratio']:.2f}x"
            else:
                peak = "no sample" if phase["gated"] else "not checked (v1)"
            lines.append(
                f"| {phase['name']} | {mib(logical['bytes'])}, {logical['atoms']:,} atoms, "
                f"p50 {logical['p50']} B | {mib(stored)} "
                f"({stored / max(1, logical['bytes']):.2f}x) | "
                f"{peak} | {phase.get('samples', '-')} | {reads['live']:,} equal, "
                f"{reads['absent']:,} absent |")
        conversion = next((phase for phase in report["phases"]
                           if phase["name"].startswith("conversion")), None)
        if conversion and conversion.get("report"):
            values = conversion["report"]
            lines += ["", f"Conversion: {values.get('source_format')}, "
                          f"{int(values.get('records', 0)):,} records, "
                          f"{mib(values.get('logical_bytes', 0))} of record bytes, v1 "
                          f"{mib(values.get('v1_disk_bytes', 0))}, v2 "
                          f"{mib(values.get('v2_disk_bytes', 0))}, "
                          f"{values.get('seconds', 0):.1f} s. The node start took "
                          f"{conversion['start_seconds']} s."]
        for failure in report["failures"]:
            lines.append(f"- FAIL: {failure}")
        lines.append("")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--primary", type=Path, required=True,
                        help="the lastdbd of the program that runs the primary")
    parser.add_argument("--primary-revision", required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--case", choices=list(CASES), action="append")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    candidate = (args.candidate.resolve(strict=True), args.candidate_revision)
    primary = (args.primary.resolve(strict=True), args.primary_revision)
    reports = []
    for number, name in enumerate(args.case or list(CASES), start=1):
        seeder = primary if CASES[name][0] == "primary" else candidate
        reports.append(Case(name, seeder, candidate, args.output, number).execute())
    save(args.output / "node-proof.json", {"limit": LIMIT, "profile": PROFILE,
                                           "reports": reports})
    print(summary(reports, args.primary_revision, args.candidate_revision), flush=True)
    if any(report["failures"] for report in reports):
        sys.exit(1)


if __name__ == "__main__":
    main()
