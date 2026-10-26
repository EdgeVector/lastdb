"""Open-loop application updates with bounded calls and independent low-rate reads."""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import math
import re
import time

from data import (BUCKET, IntegrityError, keyed_request, mutation_outcome,
                  range_request, validate_read, write_request)

KINDS = ("write", "keyed_read", "range_read")
ERROR_LIMIT = 16


class Histogram:
    """Fixed 512-bin histogram; percentile upper bounds, approximately 5% wide."""

    def __init__(self):
        self.bins = [0] * 512
        self.count, self.total, self.maximum = 0, 0.0, 0.0

    def add(self, milliseconds):
        value = max(0.0, milliseconds)
        bucket = min(len(self.bins) - 1, int(math.log1p(value) * 20))
        self.bins[bucket] += 1
        self.count += 1
        self.total += value
        self.maximum = max(self.maximum, value)

    def summary(self):
        result = {"count": self.count, "mean_ms": self.total / self.count if self.count else None,
                  "max_ms": self.maximum if self.count else None,
                  "percentiles": "fixed histogram upper bounds, about 5% bins"}
        for label, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99)):
            seen, value = 0, None
            for index, count in enumerate(self.bins):
                seen += count
                if self.count and seen >= math.ceil(self.count * fraction):
                    value = min(self.maximum, math.expm1((index + 1) / 20))
                    break
            result[label] = value
        return result


def category(error):
    message = str(error)
    if isinstance(error, IntegrityError):
        return "integrity"
    if "timed out" in message.lower() or "timeout" in message.lower():
        return "timeout"
    status = re.search(r"HTTP (\d{3})", message)
    if status and status[1] in ("200", "400", "401", "403", "404", "405", "413", "422"):
        return "integrity"
    return f"http_{status[1]}" if status else "request_error"


def _request(node, job, body_bytes):
    started = time.monotonic()
    result = {"finished": started, "rpc_ms": None, "success": 0, "failed": job["count"],
              "outcome_unknown": 0}
    try:
        numbers, kind = job["numbers"], job["kind"]
        if kind == "write":
            response, rpc, _ = node.call("/api/mutations/batch",
                write_request(numbers, job["revision"], body_bytes=body_bytes))
            good, failed, error = mutation_outcome(response, len(numbers))
            result.update(success=good, failed=failed, rpc_ms=rpc)
            if error:
                result.update(error=error, category="integrity", outcome_unknown=failed)
        else:
            request = (keyed_request(numbers) if kind == "keyed_read"
                       else range_request(numbers[0], len(numbers)))
            response, rpc, _ = node.call("/api/query", request, receipt=True)
            validate_read(response, numbers, body_bytes, ordered=kind == "range_read")
            result.update(success=len(numbers), failed=0, rpc_ms=rpc)
    except Exception as error:
        result.update(error=f"{type(error).__name__}: {str(error)[:400]}", category=category(error),
                      rpc_ms=getattr(error, "elapsed_ms", None))
        if job["kind"] == "write":
            # HTTP errors and timeouts do not prove that a mutation did not commit.
            result["outcome_unknown"] = job["count"]
    result["finished"] = time.monotonic()
    result["service_ms"] = (result["finished"] - started) * 1000
    return result


class Schedule:
    """Absolute due times; missed slots are dropped rather than queued or retried."""

    def __init__(self, args, metadata, rate, phase):
        self.start = time.monotonic()
        self.end = self.start + args.seconds
        self.rate, self.records, self.phase = rate, metadata["records"], phase
        self.batch = min(64, getattr(args, "batch", 64))
        self.total, self.offset = int(rate * args.seconds), 0
        self.ticks = {"keyed_read": 1, "range_read": 1}
        self.rates = {"keyed_read": getattr(args, "keyed_rate", 1.0),
                      "range_read": getattr(args, "range_rate", 1.0)}
        self.step = 1_000_003
        while math.gcd(self.step, self.records) != 1:
            self.step += 2

    def next_due(self):
        events = []
        if self.offset < self.total:
            count = min(self.batch, self.total - self.offset)
            events.append((self.start + (self.offset + count) / self.rate, "write"))
        for kind, rate in self.rates.items():
            if rate > 0:
                due = self.start + self.ticks[kind] / rate
                if due <= self.end:
                    events.append((due, kind))
        return min(events) if events else (math.inf, "write")

    def take(self, now):
        due, kind = self.next_due()
        if due > min(now, self.end):
            return None
        if kind == "write":
            index, count = self.offset, min(self.batch, self.total - self.offset)
            positions = range(index, index + count)
            numbers = [(self.phase * 7919 + position * self.step) % self.records for position in positions]
            first = self.start + (index + 1) / self.rate
            self.offset += count
        else:
            index = self.ticks[kind]
            self.ticks[kind] += 1
            first = due
            offset = (index * 7919 + self.phase * 17) % self.records
            if kind == "keyed_read":
                count = min(16, self.records)
                numbers = [(offset + n * self.step) % self.records for n in range(count)]
            else:
                count = min(32, self.records - offset, BUCKET - offset % BUCKET)
                numbers = list(range(offset, offset + count))
        return {"kind": kind, "count": count, "numbers": numbers, "due": due,
                "first": first, "revision": self.phase * 1_000_000_000 + index + 1}


class Totals:
    def __init__(self, window_end):
        self.rows = {kind: {"offered": 0, "submitted": 0, "skipped": 0, "success": 0,
                           "failed": 0, "outcome_unknown": 0, "requests": 0, "completed_requests": 0,
                           "success_within_window": 0, "success_during_drain": 0,
                           "rpc_unreported_requests": 0,
                           "rpc": Histogram(), "rpc_success": Histogram(), "rpc_failed": Histogram(),
                           "end_to_end": Histogram(), "end_success": Histogram(),
                           "end_failed": Histogram(), "errors": {}}
                     for kind in KINDS}
        self.errors, self.measurement_failed = [], False
        self.last_completion, self.window_end = 0.0, window_end

    def offered(self, job, submitted):
        row, count = self.rows[job["kind"]], job["count"]
        row["offered"] += count
        row["submitted" if submitted else "skipped"] += count
        if submitted:
            row["requests"] += 1

    def finish(self, job, result, rate, telemetry):
        row = self.rows[job["kind"]]
        row["success"] += result["success"]
        row["failed"] += result["failed"]
        row["outcome_unknown"] += result.get("outcome_unknown", 0)
        finished = "success_within_window" if result["finished"] <= self.window_end else "success_during_drain"
        row[finished] += result["success"]
        row["completed_requests"] += 1
        self.last_completion = max(self.last_completion, result["finished"])
        outcome = "failed" if result["failed"] else "success"
        if result["rpc_ms"] is not None:
            row["rpc"].add(result["rpc_ms"])
            row[f"rpc_{outcome}"].add(result["rpc_ms"])
        else:
            row["rpc_unreported_requests"] += 1
        for index in range(job["count"]):
            due = job["first"] + (index / rate if job["kind"] == "write" else 0)
            latency = (result["finished"] - due) * 1000
            row["end_to_end"].add(latency)
            row[f"end_{outcome}"].add(latency)
        if result.get("error"):
            name = result["category"]
            row["errors"][name] = row["errors"].get(name, 0) + result["failed"]
            fatal = name in ("integrity", "driver_error")
            self.measurement_failed |= fatal
            message = f"{job['kind']}: {result['error']}"
            if len(self.errors) < ERROR_LIMIT:
                self.errors.append(message)
            elif fatal:
                self.errors[-1] = message
            if fatal:
                telemetry.note_error(message)

    def snapshot(self, elapsed, rate, inflight):
        row = self.rows["write"]
        return {"phase": "load", "target_updates_per_second": rate,
                "elapsed_seconds": round(elapsed, 2), "inflight_requests": inflight,
                **{key: row[key] for key in ("offered", "submitted", "skipped", "success", "failed")}}

    def summary(self):
        return {kind: {**{name: value for name, value in row.items()
                          if not isinstance(value, Histogram)},
                       "rpc_batch_latency": row["rpc"].summary(),
                       "rpc_successful_batch_latency": row["rpc_success"].summary(),
                       "rpc_failed_batch_latency": row["rpc_failed"].summary(),
                       "schedule_to_completion_latency": row["end_to_end"].summary(),
                       "schedule_successful_batch_latency": row["end_success"].summary(),
                       "schedule_failed_batch_latency": row["end_failed"].summary()}
                for kind, row in self.rows.items()}


def _collect(pending, totals, rate, telemetry):
    done = [future for future in pending if future.done()]
    for future in done:
        job = pending.pop(future)
        try:
            result = future.result()
        except Exception as error:
            result = {"finished": time.monotonic(), "success": 0, "failed": job["count"],
                      "rpc_ms": None, "category": "driver_error", "outcome_unknown": job["count"],
                      "error": f"{type(error).__name__}: {str(error)[:400]}"}
        totals.finish(job, result, rate, telemetry)


def _submit_due(executor, pending, schedule, totals, node, body_bytes, workers):
    active = {kind: sum(job["kind"] == kind for job in pending.values()) for kind in KINDS}
    while True:
        job = schedule.take(time.monotonic())
        if job is None:
            break
        kind = job["kind"]
        # Reserve one slot for each read shape; there is no executor backlog.
        room = active[kind] < (workers - 2 if kind == "write" else 1)
        totals.offered(job, room)
        if room:
            pending[executor.submit(_request, node, job, body_bytes)] = job
            active[kind] += 1


def run_phase(node, args, metadata, rate, telemetry):
    """Offer the requested rate regardless of response time, then drain admitted calls."""
    workers = min(16, getattr(args, "workers", 16))
    if workers < 3 or rate <= 0 or args.seconds <= 0 or not metadata.get("complete"):
        raise ValueError("The load requires a complete seed, positive rate/duration, and at least three workers")
    if min(64, getattr(args, "batch", 64)) <= 0:
        raise ValueError("The mutation batch size must be positive")
    phase = metadata.get("phase_index", 0) + 1
    metadata["phase_index"] = phase
    schedule = Schedule(args, metadata, rate, phase)
    totals, pending = Totals(schedule.end), {}
    next_progress, next_sample = schedule.start + 60, schedule.start
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            while True:
                _collect(pending, totals, rate, telemetry)
                now = time.monotonic()
                if not totals.measurement_failed:
                    _submit_due(executor, pending, schedule, totals, node, metadata["body_bytes"], workers)
                snapshot = totals.snapshot(now - schedule.start, rate, len(pending))
                if now >= next_sample:
                    telemetry.set_load(snapshot)
                    next_sample = now + 1
                if now >= next_progress:
                    print(f"sustained phase: {snapshot}", flush=True)
                    next_progress += 60
                if not pending and (totals.measurement_failed or now >= schedule.end):
                    break
                due, _ = schedule.next_due()
                pause = max(.001, min(.05, due - time.monotonic()))
                if pending:
                    wait(pending, timeout=pause, return_when=FIRST_COMPLETED)
                else:
                    time.sleep(pause)
    except Exception as error:
        totals.measurement_failed = True
        message = f"driver: {type(error).__name__}: {str(error)[:400]}"
        totals.errors.append(message)
        telemetry.note_error(message)
        _collect(pending, totals, rate, telemetry)
    elapsed = time.monotonic() - schedule.start
    rows = totals.summary()
    write = rows["write"]
    result = {"target_updates_per_second": rate, "requested_seconds": args.seconds,
              "elapsed_seconds": elapsed, "drain_seconds": max(0, elapsed - args.seconds),
              "complete": not totals.measurement_failed and schedule.offset == schedule.total,
              "measurement_failed": totals.measurement_failed, "errors": totals.errors,
              "workers": workers, "maximum_inflight_write_requests": workers - 2,
              "batch": schedule.batch, "read_rates": schedule.rates, "operations": rows,
              **{key: write[key] for key in ("offered", "submitted", "skipped", "success", "failed")},
              "unacknowledged_outcome_unknown": write["outcome_unknown"],
              "success_definition": "Durable acknowledgement with a mutation identity for each item",
              "failed_definition": "No complete durable acknowledgement; not proof that data did not commit",
              "achieved_updates_per_second": write["success"] / max(elapsed, .001),
              "acknowledged_within_window": write["success_within_window"],
              "acknowledged_during_drain": write["success_during_drain"],
              "updates_per_second_within_window": write["success_within_window"] / args.seconds,
              "completion_rate_including_drain": True,
              "client_limited": write["skipped"] > 0,
              "overload_observed": write["failed"] > 0 or write["skipped"] > 0,
              "latency_scope": "Includes batch assembly, client scheduling, RPC, and response validation; "
                               "all completed requests, including errors; skipped slots have no completion. "
                               "Successful latency requires every item in a batch acknowledged; partial "
                               "acknowledgements remain in failed-batch latency and item success counts"}
    telemetry.set_load(totals.snapshot(elapsed, rate, 0))
    print(f"sustained phase complete: {result['target_updates_per_second']} updates/s, "
          f"success={result['success']}, failed={result['failed']}, skipped={result['skipped']}, "
          f"measurement_failed={result['measurement_failed']}", flush=True)
    return result
