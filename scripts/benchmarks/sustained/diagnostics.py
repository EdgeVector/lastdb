"""Bounded numeric log capture for isolated write investigations."""

import json
import math
from pathlib import Path
import time

from diagnostic_events import MAX_FIELDS, NAME, integer, numeric_map, write_read_event
from diagnostic_report import render


MARKERS = {"LASTDB_WRITE_REQUEST_TIMING ": {"request"},
           "LASTDB_SNAPSHOT_TIMING ": {"scope", "prepare", "commit", "completion", "search_outbox", "merge", "write_reads"}}
MAX_LOG_BYTES, MAX_LINE_BYTES, MAX_EVENTS = 2 * 1024 ** 3, 256 * 1024, 2_000_000
MAX_WINDOWS, MAX_ERRORS = 64, 32
MAX_EXCLUDED_BYTES = 32 * 1024 ** 2
FILE_KINDS = ("published", "retired_held", "temporary", "unreferenced", "other_home")


class Metric:
    def __init__(self, distribution=False):
        self.count, self.total, self.maximum = 0, 0, 0
        self.bins = [0] * 512 if distribution else None

    def add(self, value):
        self.count += 1
        self.total += value
        self.maximum = max(self.maximum, value)
        if self.bins is not None:
            self.bins[min(511, int(math.log1p(value) * 10))] += 1

    def summary(self):
        result = {"reported": self.count, "total": self.total,
                  "mean": self.total / self.count if self.count else None, "max": self.maximum}
        if self.bins is not None:
            for label, fraction in (("p50", .5), ("p95", .95), ("p99", .99)):
                seen, value = 0, None
                for slot, count in enumerate(self.bins):
                    seen += count
                    if self.count and seen >= math.ceil(self.count * fraction):
                        value = min(self.maximum, math.expm1((slot + 1) / 10))
                        break
                result[label] = value
            result["percentiles"] = "fixed histogram upper bounds, approximately 10% bins"
        return result


def add_metrics(target, values, distribution=False):
    for name, value in values.items():
        if name not in target:
            if len(target) >= MAX_FIELDS:
                raise ValueError("Observed diagnostic field names exceed their bound")
            target[name] = Metric(distribution)
        target[name].add(value)


class Family:
    def __init__(self):
        self.events, self.labels = 0, {}
        self.phases, self.counts, self.scalars = {}, {}, {}

    def observe(self, event):
        self.events += 1
        add_metrics(self.phases, event.get("phases_us", {}), True)
        add_metrics(self.counts, event.get("counts", {}))
        names = ("total_us", "covered_us", "unassigned_us", "queue_wait_us", "body_bytes",
                 "rows", "requests", "inputs", "records", "run_bytes")
        add_metrics(self.scalars, {n: event[n] for n in names if event.get(n) is not None}, True)
        for name in ("status", "background", "ok", "work", "merge_kind"):
            if name in event:
                key = f"{name}={event[name]}"
                self.labels[key] = self.labels.get(key, 0) + 1

    def summary(self):
        return {"events": self.events, "labels": self.labels,
                **{name: {k: v.summary() for k, v in values.items()}
                   for name, values in (("phases_us", self.phases), ("counts", self.counts),
                                        ("scalars", self.scalars))}}


def validate_event(event, allowed):
    if not isinstance(event, dict) or event.get("kind") not in allowed:
        raise ValueError("A log marker has an unsupported event kind")
    if event["kind"] == "write_reads":
        return write_read_event(event)
    for name in ("started_us", "ended_us", "total_us"):
        integer(event.get(name))
    if event["ended_us"] < event["started_us"]:
        raise ValueError("A diagnostic event has reversed clock bounds")
    numeric_map(event.get("phases_us"))
    numeric_map(event.get("counts", {}))
    for name in ("covered_us", "unassigned_us", "queue_wait_us", "body_bytes", "rows",
                 "requests", "inputs", "records", "run_bytes"):
        if event.get(name) is not None:
            integer(event[name])
    if "covered_us" in event or "unassigned_us" in event:
        if ("covered_us" not in event or "unassigned_us" not in event
                or event["covered_us"] + event["unassigned_us"] != event["total_us"]):
            raise ValueError("Covered and unassigned time differ from capture time")
    for name in ("background", "ok", "work"):
        if name in event and type(event[name]) is not bool:
            raise ValueError("A diagnostic event flag is invalid")
    if event["kind"] == "request" and (type(event.get("status")) is not int or not 100 <= event["status"] <= 599):
        raise ValueError("The request status is invalid")
    if event["kind"] == "merge":
        if (type(event.get("home")) is not int or event["home"] not in (0, 1)
                or type(event.get("merge_kind")) is not int or event["merge_kind"] not in (0, 1, 2)):
            raise ValueError("The native merge home or kind is invalid")
    return event["kind"] if event["kind"] != "merge" else ("merge_main" if event["home"] == 0 else "merge_cas")


def validate_audit(audit):
    if not isinstance(audit, dict) or set(audit) != {"main", "cas"}:
        raise ValueError("A checkpoint lacks both native home audits")
    for home in audit.values():
        files = numeric_map(home["files"])
        for suffix in ("file_bytes", "allocated_bytes"):
            if sum(files[f"{kind}_{suffix}"] for kind in FILE_KINDS) != files[f"total_{suffix}"]:
                raise ValueError("Physical file categories do not reconcile")
        parts = [home["base"], home["merging"], *home["levels"]]
        if len(parts) > MAX_WINDOWS:
            raise ValueError("Native levels exceed the diagnostic bound")
        for part in parts:
            numeric_map(part)
        if sum(p["file_bytes"] for p in parts) != files["published_file_bytes"]:
            raise ValueError("Published run parts differ from published file bytes")
        inventory = home.get("inventory")
        if not isinstance(inventory, dict) or inventory.get("complete") is not True:
            raise ValueError("A checkpoint logical inventory is incomplete")
        collections = inventory["collections"]
        if not isinstance(collections, dict) or len(collections) > MAX_WINDOWS:
            raise ValueError("Logical collection labels exceed their bound")
        for name, row in collections.items():
            if not NAME.fullmatch(name):
                raise ValueError("A logical collection label is invalid")
            numeric_map(row)
        if sum(r["live_keys"] + r["deleted_keys"] for r in collections.values()) != inventory["merged_keys"]:
            raise ValueError("Logical collection key counts do not reconcile")


def validate_home_files(home_files):
    if not isinstance(home_files, dict):
        raise ValueError("The whole-home file observation is invalid")
    names = ("logical_bytes", "allocated_bytes", "unique_file_logical_bytes",
             "unique_file_allocated_bytes", "hardlink_duplicate_entries")
    for name in names:
        integer(home_files[name])
    families = home_files["families"]
    if not isinstance(families, dict) or len(families) > MAX_WINDOWS:
        raise ValueError("Whole-home file families exceed their bound")
    for name, row in families.items():
        if not isinstance(name, str) or not NAME.fullmatch(name):
            raise ValueError("A whole-home file family label is invalid")
        numeric_map(row)
        for field in ("files", "logical_bytes", "allocated_bytes"):
            integer(row[field])
    for field in ("logical_bytes", "allocated_bytes"):
        if sum(row[field] for row in families.values()) != home_files[field]:
            raise ValueError(f"Whole-home {field} families do not reconcile")
        if home_files[f"unique_file_{field}"] > home_files[field]:
            raise ValueError(f"Unique-inode {field} exceeds directory-entry bytes")
    if home_files["hardlink_duplicate_entries"] > sum(row["files"] for row in families.values()):
        raise ValueError("Duplicate hardlink entries exceed the observed file entries")


def search_policy(status):
    if not isinstance(status, dict) or type(status.get("enabled")) is not bool:
        raise ValueError("The checkpoint Search enable state is unavailable")
    if status["rebuild_required"] is not None and type(status["rebuild_required"]) is not bool:
        raise ValueError("The checkpoint Search rebuild state is invalid")
    if status["status_error"] is not None and not isinstance(status["status_error"], str):
        raise ValueError("The checkpoint Search status error is invalid")
    return {"enabled": status["enabled"], "rebuild_required": status["rebuild_required"],
            "max_batch_bytes": integer(status["max_batch_bytes"]),
            "max_batch_files": integer(status["max_batch_files"]),
            "status_error_present": status["status_error"] is not None}


class Diagnostics:
    def __init__(self, output, log_path):
        self.output, self.log_path = Path(output), Path(log_path)
        self.windows, self.checkpoints, self.errors = [], [], []
        self.crossing, self.unassigned, self.lines, self.events = {}, {}, 0, 0
        self.exclusions = []
        self.finished = False

    def error(self, message):
        if len(self.errors) < MAX_ERRORS:
            self.errors.append(str(message)[:500])

    def checkpoint(self, name, audit_dict, start_us, end_us, home_files=None, search_status=None):
        self._bounds(name, start_us, end_us)
        if len(self.checkpoints) >= MAX_WINDOWS or any(c["name"] == name for c in self.checkpoints):
            raise ValueError("Checkpoint names repeat or exceed their bound")
        checkpoint = {"name": name, "started_us": start_us, "ended_us": end_us,
                      "audit": audit_dict, "home_files": home_files, "search": None}
        self.checkpoints.append(checkpoint)
        try:
            checkpoint["search"] = search_policy(search_status)
            if checkpoint["search"]["status_error_present"] or checkpoint["search"]["rebuild_required"] is None:
                raise ValueError("The checkpoint Search marker status is unavailable")
        except (KeyError, TypeError, ValueError) as error:
            self.error(f"checkpoint {name}/search: {error}")
        try:
            validate_audit(audit_dict)
        except (KeyError, TypeError, ValueError) as error:
            self.error(f"checkpoint {name}: {error}")
        if home_files is not None:
            try:
                validate_home_files(home_files)
            except (KeyError, TypeError, ValueError) as error:
                self.error(f"checkpoint {name}/home_files: {error}")

    def interval(self, name, start_us, end_us, expected_requests=None, workload_complete=True):
        self._bounds(name, start_us, end_us)
        if type(workload_complete) is not bool or (not workload_complete and name != "seed"):
            raise ValueError("Only a seed interval can report an incomplete workload")
        if expected_requests is not None:
            integer(expected_requests)
        if len(self.windows) >= MAX_WINDOWS or any(w["name"] == name for w in self.windows):
            raise ValueError("Interval names repeat or exceed their bound")
        if any(start_us < w["ended_us"] and end_us > w["started_us"] for w in self.windows):
            raise ValueError("Diagnostic phase intervals overlap")
        self.windows.append({"name": name, "started_us": start_us, "ended_us": end_us,
                             "expected_requests": expected_requests, "workload_complete": workload_complete,
                             "families": {}, "crossing": {}})

    def exclude_bytes(self, name, start_offset, end_offset):
        self._bounds(name, start_offset, end_offset)
        if self.exclusions or end_offset - start_offset > MAX_EXCLUDED_BYTES:
            raise ValueError("Only one bounded intentional-crash log region is allowed")
        self.exclusions.append({"name": name, "start_offset": start_offset, "end_offset": end_offset,
                                "registered_bytes": end_offset - start_offset,
                                "excluded_bytes": 0, "excluded_lines": 0, "excluded_line_bytes": 0})

    def _bounds(self, name, start, end):
        if self.finished or not isinstance(name, str) or not NAME.fullmatch(name):
            raise ValueError("The diagnostic capture name or lifecycle is invalid")
        if integer(end) < integer(start):
            raise ValueError("Diagnostic phase clock bounds are reversed")

    def _event(self, event, allowed):
        family = validate_event(event, allowed)
        self.events += 1
        if self.events > MAX_EVENTS:
            raise ValueError("Native log event count exceeds its bound")
        start, end = event["started_us"], event["ended_us"]
        contained = [w for w in self.windows if w["started_us"] <= start < w["ended_us"] and end <= w["ended_us"]]
        if contained:
            families = contained[0]["families"]
            if family not in families:
                families[family] = Family()
            families[family].observe(event)
            return
        overlaps = [w for w in self.windows if start < w["ended_us"] and end > w["started_us"]]
        target = self.crossing if overlaps else self.unassigned
        target[family] = target.get(family, 0) + 1
        for window in overlaps:
            window["crossing"][family] = window["crossing"].get(family, 0) + 1

    def _read(self):
        size = self.log_path.stat().st_size
        if self.log_path.is_symlink() or size > MAX_LOG_BYTES:
            raise ValueError("The native log identity or size exceeds its bound")
        if any(region["end_offset"] > size for region in self.exclusions):
            raise ValueError("An excluded log region extends beyond the completed log")
        deadline = time.monotonic() + 180
        with self.log_path.open("rb") as source:
            while True:
                offset = source.tell()
                raw = source.readline(MAX_LINE_BYTES + 1)
                if not raw:
                    break
                self.lines += 1
                if len(raw) > MAX_LINE_BYTES or time.monotonic() > deadline:
                    raise ValueError("Native log line or parse time exceeds its bound")
                if self._excluded(offset, source.tell()):
                    continue
                line = raw.decode("utf-8", errors="replace")
                for marker, allowed in MARKERS.items():
                    if marker in line:
                        if self.events >= MAX_EVENTS:
                            raise ValueError("Native log event count exceeds its bound")
                        try:
                            self._event(json.loads(line.split(marker, 1)[1]), allowed)
                        except (ValueError, TypeError, KeyError) as error:
                            self.error(f"native log line {self.lines}: {error}")
                        break

    def _excluded(self, start, end):
        for region in self.exclusions:
            overlap = max(0, min(end, region["end_offset"]) - max(start, region["start_offset"]))
            if overlap:
                region["excluded_bytes"] += overlap
                region["excluded_lines"] += 1
                region["excluded_line_bytes"] += end - start
                return True
        return False

    def _seed_checkpoint_skipped(self):
        return (not self.checkpoints and len(self.windows) == 1
                and self.windows[0]["name"] == "seed"
                and self.windows[0].get("workload_complete") is False)

    def _search_enabled(self):
        if self._seed_checkpoint_skipped():
            # The driver stops before audits after an incomplete initial seed.
            # Numeric request coverage is still required independently below.
            return None
        seed = next((c for c in self.checkpoints if c["name"] == "seed"), {})
        selected = (seed.get("search") or {}).get("enabled")
        if type(selected) is not bool:
            self.error("The seed checkpoint lacks an explicit Search enable state")
        for checkpoint in self.checkpoints:
            if (checkpoint.get("search") or {}).get("enabled") is not selected:
                self.error(f"checkpoint {checkpoint['name']}: Search enable state differs from seed")
            if selected is False:
                home = checkpoint.get("home_files")
                if not isinstance(home, dict):
                    self.error(f"checkpoint {checkpoint['name']}: disabled Search lacks whole-home file coverage")
                elif home.get("families", {}).get("search_inbox", {}).get("files", 0):
                    self.error(f"checkpoint {checkpoint['name']}: disabled Search created inbox file entries")
        observed = sum(w["families"]["search_outbox"].events for w in self.windows
                       if "search_outbox" in w["families"])
        observed += self.crossing.get("search_outbox", 0) + self.unassigned.get("search_outbox", 0)
        if selected is False and observed:
            self.error("Disabled Search emitted search_outbox events")
        return selected

    def finish(self):
        if self.finished:
            raise ValueError("The diagnostic capture already finished")
        self.finished = True
        try:
            self._read()
        except (OSError, ValueError) as error:
            self.error(error)
        search_enabled = self._search_enabled()
        windows = []
        for window in sorted(self.windows, key=lambda w: w["started_us"]):
            families = {k: v.summary() for k, v in window["families"].items()}
            requests = families.get("request", {}).get("events", 0)
            expected = window["expected_requests"]
            if expected is not None and requests != expected:
                self.error(f"{window['name']}: request events {requests} differ from expected {expected}")
            if expected and window["name"].startswith("load"):
                required = ("request", "scope", "commit", "completion")
                for name in required + (("search_outbox",) if search_enabled is True else ()):
                    if not families.get(name, {}).get("events"):
                        self.error(f"{window['name']}: missing nonzero {name} event coverage")
            windows.append({**window, "families": families, "observed_requests": requests})
        if not self.windows or not self.events:
            self.error("The diagnostic capture lacks phase intervals or numeric log events")
        if any(r["excluded_bytes"] != r["registered_bytes"] for r in self.exclusions):
            self.error("The intentional-crash log region was not read completely")
        report = {"version": 1, "complete": not self.errors, "errors": self.errors,
                  "log_lines": self.lines, "events": self.events, "intervals": windows,
                  "crossing_events": self.crossing, "unassigned_events": self.unassigned,
                  "checkpoints": self.checkpoints, "log_exclusions": self.exclusions,
                  "search_enabled": search_enabled,
                  "skipped_checkpoints": [
                      {"name": name, "reason": "seed workload incomplete"}
                      for name in ("seed", "load", "idle", "reopen")
                  ] if self._seed_checkpoint_skipped() else []}
        self.output.mkdir(parents=True, exist_ok=True)
        (self.output / "diagnostics.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        (self.output / "diagnostic-summary.md").write_text(render(report))
        return {k: report[k] for k in ("complete", "errors", "events", "crossing_events", "unassigned_events", "search_enabled", "skipped_checkpoints")} | {
            "intervals": len(windows), "checkpoints": len(self.checkpoints),
            "log_exclusions": self.exclusions,
            "request_coverage": [{"interval": w["name"], "expected": w["expected_requests"],
                                  "observed": w["observed_requests"]} for w in windows],
            "detail_file": "diagnostics.json", "summary_file": "diagnostic-summary.md"}
