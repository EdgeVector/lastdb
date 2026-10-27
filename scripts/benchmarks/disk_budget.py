"""Measure retained v2 disk space against the node's logical atom report."""

import math
import statistics

from growth import CASES as GROWTH_CASES, GrowthWorkload
from disk_deletes import delete_records, prepare_delete, verify_deleted
from node import Node, RUN_NODE_MARKER, save
from disk_usage import validate as validate_breakdown

LIMIT = 1.5
PERCENT = 10
FLOOR = 64 * 1024
CASES = (*GROWTH_CASES, "delete")
MEASURES = ("logical_atom_bytes", "retained_disk_bytes", "disk_to_logical_ratio",
            "max_disk_to_logical_ratio", "search_queue_bytes", "search_queue_payload_bytes",
            "search_queue_files", "other_home_bytes", "whole_home_disk_bytes",
            "limit_logical_bytes", "limit_database_bytes", "limit_headroom_bytes", "limit_headroom_percent",
            "starting_logical_atom_bytes", "starting_live_records", "deleted_records")


def require_search_off(node, output, phase):
    status, _, _ = node.call("/api/db/native-status")
    search = status.get("search")
    save(output / f"disk-search-{phase}.json", search)
    if not isinstance(search, dict) or search.get("enabled") is not False or search.get("status_error"):
        raise ValueError("The disk budget requires a daemon that reports Search off without a status error")


def logical_storage(node):
    report, _, _ = node.call("/api/db/schemas")
    report = report["schema_storage"]
    validate_logical(report)
    return report


def validate_logical(report):
    rows = report["per_schema"]
    if (not rows or report["total_logical_bytes"] != sum(row["bytes"] for row in rows)
            or report["total_atoms"] != sum(row["atom_count"] for row in rows)
            or any(type(row[key]) is not int or row[key] <= 0
                   for row in rows for key in ("bytes", "atom_count"))):
        raise ValueError("The logical atom report is incomplete")


def metrics(points):
    last = points[-1]
    breakdown = last["disk_breakdown"]
    worst = max(points, key=lambda p: max(p["disk_breakdown"]["database"].values())
                / p["logical_storage"]["total_logical_bytes"])
    logical = worst["logical_storage"]["total_logical_bytes"]
    retained = max(worst["disk_breakdown"]["database"].values())
    return {"logical_atom_bytes": last["logical_storage"]["total_logical_bytes"],
            "retained_disk_bytes": max(breakdown["database"].values()),
            "search_queue_bytes": max(breakdown["search_queue"].values()),
            "search_queue_payload_bytes": breakdown["search_queue"]["logical_bytes"],
            "search_queue_files": breakdown["search_queue_files"],
            "other_home_bytes": max(breakdown["other_home"].values()),
            "whole_home_disk_bytes": max(breakdown["whole_home"].values()),
            "limit_logical_bytes": logical, "limit_database_bytes": retained,
            "limit_headroom_bytes": LIMIT * logical - retained,
            "limit_headroom_percent": 100 * (1 - retained / (LIMIT * logical)),
            "starting_logical_atom_bytes": points[0]["logical_storage"]["total_logical_bytes"],
            "starting_live_records": points[0]["live_records"], "deleted_records": last["deleted_records"],
            "disk_to_logical_ratio": max(breakdown["database"].values()) / last["logical_storage"]["total_logical_bytes"],
            "max_disk_to_logical_ratio": max(
                max(p["disk_breakdown"]["database"].values()) / p["logical_storage"]["total_logical_bytes"] for p in points)}


class DiskBudgetWorkload(GrowthWorkload):
    case_names = GROWTH_CASES

    def run(self):
        # Nightly inserts can already exceed the fixed delete target. Give
        # deletes a fresh owned home instead of inheriting that larger corpus.
        output = self.output / "delete"
        output.mkdir()
        node = Node(self.node.binary, self.node.revision, output,
                    native_timing=self.node.native_timing, workers=self.node.workers,
                    environment=self.node.environment, start_seconds=self.node.start_seconds,
                    arguments=self.node.arguments)
        try:
            node.start()
            require_search_off(node, output, "before")
            marker = (node.home / "data" / "laststore-pack-node-v1").read_text()
            save(output / "disk-format.json", {"marker": marker})
            if marker != RUN_NODE_MARKER:
                raise ValueError("The delete case requires a fresh v2 run home")
            workload = DiskBudgetWorkload(node, {**self.recipe, "growth": self.recipe}, output)
            workload.case_names = ("delete",)
            GrowthWorkload.run(workload)
            require_search_off(node, output, "after")
            self.cases.update(workload.cases)
        finally:
            node.close()
        super().run()

    def checkpoint(self, number):
        result, _, _ = self.node.call("/api/db/flush", {})
        if result.get("durable") is not True:
            raise ValueError("The disk budget checkpoint lacks a durable flush")
        self.phase = "verify-disk-budget"
        self.read(0, self.live_records)
        verify_deleted(self)
        logical = logical_storage(self.node)
        self.phase = "settle-disk-budget"
        idle = []
        current, samples, settled = self.settle_disk(idle, self.node.disk_breakdown)
        inventory = getattr(self, "delete_start_records", 0) > 0
        status, _, _ = self.node.call("/api/db/native-status", {"inventory": True} if inventory else None)
        diagnostics = status.get("storage_diagnostics")
        if "delete_target_bytes" in self.recipe and (
                not isinstance(diagnostics, dict)
                or any(not isinstance(diagnostics.get(name), dict) for name in ("main", "cas"))):
            raise ValueError("The disk budget lacks both physical storage diagnostics")
        if inventory and any(not diagnostics[name].get("inventory", {}).get("complete")
                             for name in ("main", "cas")):
            raise ValueError("The delete checkpoint lacks a complete storage inventory")
        return {"round": number, "live_records": self.live_records,
                "delete_start_records": getattr(self, "delete_start_records", 0),
                "delete_keep_records": getattr(self, "delete_keep_records", 0),
                "deleted_records": getattr(self, "deleted_records", 0), "disk": current["whole_home"],
                "storage_diagnostics": diagnostics,
                "disk_breakdown": current,
                "logical_storage": logical, "settled_seconds": settled,
                "disk_samples": samples, "idle_memory_bytes": statistics.median(idle),
                "idle_memory_samples": idle}

    def measure(self, case):
        if case == "delete":
            prepare_delete(self, logical_storage)
        points = [self.checkpoint(0)]
        for number in range(1, self.recipe["rounds"] + 1):
            self.phase = case
            count = self.recipe["records"]
            if case == "fixed-read":
                self.read(0, count)
            elif case == "delete":
                target = (self.delete_start_records - self.delete_keep_records) * number // self.recipe["rounds"]
                delete_records(self, target - getattr(self, "deleted_records", 0))
            else:
                insert = case == "insert"
                self.write(self.live_records if insert else 0, count, f"disk-{case}-{number:03d}", insert)
                if insert:
                    self.live_records += count
            points.append(self.checkpoint(number))
            save(self.output / "growth-progress.json", {"case": case, "checkpoints": points})
        return {"checkpoints": points, "metrics": {
            "live_records": self.live_records, "idle_memory_bytes": points[-1]["idle_memory_bytes"],
            **metrics(points)}}


def validate(case, result, recipe):
    points = result["checkpoints"]
    if len(points) != recipe["rounds"] + 1:
        raise ValueError(f"{case}: missing disk checkpoints")
    for number, point in enumerate(points):
        removed = (points[0]["live_records"] - points[0]["delete_keep_records"]) * number // recipe["rounds"]
        expected = (points[0]["live_records"] - removed if case == "delete"
                    else recipe["records"] * (1 + number if case == "insert" else 1))
        if point["round"] != number or point["live_records"] != expected:
            raise ValueError(f"{case}: invalid disk checkpoint count")
        validate_logical(point["logical_storage"])
        if case == "delete":
            if (points[0]["logical_storage"]["total_logical_bytes"] < recipe["delete_target_bytes"]
                    or point["delete_start_records"] != points[0]["live_records"]
                    or point["delete_keep_records"] != (points[0]["live_records"] + 3) // 4
                    or point["deleted_records"] != removed):
                raise ValueError("The delete checkpoint has an invalid deletion count")
            if number and any(point["logical_storage"][name] >= points[number - 1]["logical_storage"][name]
                              for name in ("total_logical_bytes", "total_atoms")):
                raise ValueError("Deleted records did not reduce the logical data and atom count")
        validate_breakdown(point["disk_breakdown"])
        if point["disk_breakdown"]["search_queue_files"] != 0:
            raise ValueError(f"{case}: the disk budget requires an empty Search queue")
        if point["disk"] != point["disk_breakdown"]["whole_home"]:
            raise ValueError(f"{case}: the whole-home totals differ")
        if (set(point["disk"]) != {"logical_bytes", "allocated_bytes"}
                or any(type(value) is not int or value <= 0 for value in point["disk"].values())
                or not math.isfinite(point["settled_seconds"]) or point["settled_seconds"] < 3):
            raise ValueError(f"{case}: invalid settled disk sample")
        samples = point["disk_samples"]
        if len(samples) < 4:
            raise ValueError(f"{case}: missing disk size samples")
        stable = []
        for sample in reversed(samples):
            observed = {name: value for name, value in sample.items() if name != "elapsed_seconds"}
            validate_breakdown(observed)
            if observed != point["disk_breakdown"]:
                break
            stable.insert(0, sample)
        if len(stable) < 4 or stable[-1]["elapsed_seconds"] - stable[0]["elapsed_seconds"] < 3:
            raise ValueError(f"{case}: disk samples do not establish three stable seconds")
    if result["metrics"]["live_records"] != points[-1]["live_records"]:
        raise ValueError(f"{case}: final live count differs")
    expected = metrics(points)
    if any(not math.isfinite(result["metrics"][name]) or result["metrics"][name] != value
           for name, value in expected.items()):
        raise ValueError(f"{case}: invalid disk budget result")


def advisories(base, candidate):
    result = []
    for name in ("retained_disk_bytes", "search_queue_bytes"):
        if (candidate[name] - base[name] >= FLOOR
                and candidate[name] >= base[name] * (1 + PERCENT / 100)):
            result.append(name)
    return result


def render(comparison):
    lines = ["", "| Case | Logical atoms MiB, base → candidate | Database disk MiB, base → candidate | Final database ratio, base → candidate | Maximum database ratio, base → candidate |",
             "|---|---:|---:|---:|---:|"]
    for case, item in comparison.items():
        cells = [" → ".join(f"{item[label][name] / divisor:.3f}{unit}" for label in ("baseline", "candidate"))
                 for name, divisor, unit in (("logical_atom_bytes", 2**20, ""),
                                             ("retained_disk_bytes", 2**20, ""),
                                             ("disk_to_logical_ratio", 1, "×"),
                                             ("max_disk_to_logical_ratio", 1, "×"))]
        lines.append(f"| {case} | " + " | ".join(cells) + " |")
    lines.extend(["", "| Case | Search queue MiB, base → candidate | Queue files, base → candidate | Other home MiB, base → candidate | Whole home MiB, base → candidate |",
                  "|---|---:|---:|---:|---:|"])
    for case, item in comparison.items():
        cells = [" → ".join(f"{item[label][name] / divisor:.3f}" for label in ("baseline", "candidate"))
                 for name, divisor in (("search_queue_bytes", 2**20), ("search_queue_files", 1),
                                       ("other_home_bytes", 2**20), ("whole_home_disk_bytes", 2**20))]
        lines.append(f"| {case} | " + " | ".join(cells) + " |")
    lines.extend(["", "The ceiling is 1.5× logical atom bytes. Equality is within the budget.",
                  "Logical atom bytes use GET /api/db/schemas, the same plaintext atom measure as lastdb db schemas.",
                  "Database disk is the greater of file lengths and allocated blocks under data/ and blobs/.",
                  "The Search queue includes apps/search/inbox/; other home files and the whole home remain explicit.",
                  "Each category retains both file lengths and allocated blocks; its raw totals add exactly to the whole home.",
                  "Each checkpoint follows durable flush and verified keyed reads, then three stable seconds, with a 90-second deadline.",
                  "The v2 format uses its default automatic run merges. This run does not force compaction or measure transient disk peaks.",
                  "Search is explicitly off before and after each run; every checkpoint requires an empty Search queue.",
                  "This is one baseline/candidate pair with no repeat. The maximum includes every settled candidate checkpoint.",
                  "Any candidate checkpoint above 1.5× fails benchmark-complete and blocks merge after this single pass.",
                  "The cap is total database size, not an additional allowance. Deletes use the remaining logical atom bytes.",
                  "The delete case starts in a fresh home, reaches at least 10 MiB logical on PRs and nightly, then deletes 75% of its keys.",
                  "A database or Search queue increase of at least 10% and 64 KiB produces an advisory from this pass.",
                  "Equality passes. Relative database and Search queue growth remains advisory.",
                  "The absolute limit fails even if the baseline also exceeds it; this does not prove PR causation.",
                  "The generated v2 workload supplies a guard for this data shape, not a measurement of the primary.", ""])
    return "\n".join(lines)
