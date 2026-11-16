"""Classify mutation-log HTTP results, exit codes, and latency/throughput summaries.

Exit codes: 0 complete, 1 correctness, 2 the $6 cap, 3 a capacity finding.
Exit 1 wins over 3, and 3 wins over 2.
"""

import json

CONTENTION_CODE = "retry_later"
CONTENTION_FIELD = "mutation_log.transaction.conflict"
THROTTLE_FIELDS = frozenset((
    "mutation_log.transaction.throttled",
    "mutation_log.submission.throttled",
    "mutation_log.query.throttled",
))
CONTENTION_RATE = 0.01
MAX_CONTENTION_TRIES = 20
QUIET_BASE_MS = 400
QUIET_CAP_MS = 8000
EXIT_OK = 0
EXIT_CORRECTNESS = 1
EXIT_CAP = 2
EXIT_SCALING = 3


class RetriesExhausted(Exception):
    """A conflict or throttle used every client retry and still failed."""


def is_contention(status, payload):
    """True only for the head-row transaction conflict."""
    return (status == 503 and isinstance(payload, dict)
            and payload.get("code") == CONTENTION_CODE
            and payload.get("field") == CONTENTION_FIELD)


def is_throttle(status, payload):
    """DynamoDB throttle. A capacity finding at every tier, once it succeeds."""
    return (status == 503 and isinstance(payload, dict)
            and payload.get("code") == CONTENTION_CODE
            and payload.get("field") in THROTTLE_FIELDS)


def is_duplicate(payload):
    """An accepted append that stored nothing."""
    if not isinstance(payload, dict):
        return False
    if any(payload.get(key) == "duplicate" for key in ("result", "outcome", "status")):
        return True
    duplicate = payload.get("duplicate")
    return duplicate is True or isinstance(duplicate, dict)


def already_created(operation, status, payload):
    """A rerun keeps the run id, so create-database answers 409."""
    return (operation == "create-database" and status == 409
            and isinstance(payload, dict) and payload.get("code") == "identity_conflict")


def backoff_seconds(tries, draw):
    """Bounded jittered delay. ``tries`` starts at 1. ``draw(n)`` is in ``0..n-1``."""
    return _jitter(min(2000, 50 * (2 ** (tries - 1))), draw)


def quiet_backoff_seconds(tries, draw):
    """Longer delay for the read-only logcheck that runs after writers stop."""
    return _jitter(min(QUIET_CAP_MS, QUIET_BASE_MS * (2 ** (tries - 1))), draw)


def _jitter(base, draw):
    low = base // 2
    return (low + draw(base - low + 1)) / 1000.0


def retry_action(throttle, contention, tries):
    """``retry``, ``unresolved``, ``fail``, or ``done`` for one 503."""
    if not throttle and not contention:
        return "done"
    if tries + 1 < MAX_CONTENTION_TRIES:
        return "retry"
    return "unresolved" if contention else "fail"


def scaling_finding(kind, contention, attempts):
    """100 writers, contention above 1% of write attempts."""
    return kind == "steady" and bool(attempts) and (contention / attempts) > CONTENTION_RATE


def capacity_finding(throttles, kind, contention, attempts):
    """A throttle, a fixed-tier conflict, or timed contention above 1%."""
    if throttles or (kind == "full" and contention):
        return True
    return scaling_finding(kind, contention, attempts)


def exit_code(stopped):
    return {
        "complete": EXIT_OK,
        "error": EXIT_CORRECTNESS,
        "cap": EXIT_CAP,
        "scaling": EXIT_SCALING,
    }[stopped]


def worst_exit(stopped_values):
    """Correctness outranks scaling, and scaling outranks the cap."""
    codes = [exit_code(value) for value in stopped_values]
    for code in (EXIT_CORRECTNESS, EXIT_SCALING, EXIT_CAP):
        if code in codes:
            return code
    return EXIT_OK


def percentile(samples, percent):
    if not samples:
        return 0
    ordered = sorted(samples)
    index = (len(ordered) - 1) * percent / 100
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize(samples, errors, elapsed_ms):
    seconds = elapsed_ms / 1000 if elapsed_ms else 0
    return {
        "p50_ms": percentile(samples, 50),
        "p95_ms": percentile(samples, 95),
        "p99_ms": percentile(samples, 99),
        "errors": errors,
        "requests": len(samples),
        "throughput_per_s": (len(samples) / seconds) if seconds else 0,
    }


def tier_summary(samples, errors, elapsed_ms, counters, estimate, tier_reads):
    """Per-tier latency, 503s, partition size, and reads charged this tier."""
    attempts = counters["write_attempts"]
    summary = summarize(samples, errors, elapsed_ms)
    summary.update({
        "contention_503": counters["contention_503"],
        "throttle_503": counters["throttle_503"],
        "other_503": counters["other_503"],
        "write_attempts": attempts,
        "contention_rate": (counters["contention_503"] / attempts) if attempts else 0,
        "throttle_rate": (counters["throttle_503"] / attempts) if attempts else 0,
        "contention_unresolved": counters.get("contention_unresolved", 0),
        "throttle_unresolved": counters.get("throttle_unresolved", 0),
        "unresolved_appends": counters.get("unresolved_appends", 0),
        "logcheck_recheck": bool(counters.get("logcheck_recheck", False)),
        "partition_bytes": estimate["partition_bytes"],
        "reads": estimate["reads"] - tier_reads,
        "recheck_reserve_reads": estimate.get("recheck_reserve_reads", 0),
        "recheck_reserve_usd": estimate.get("recheck_reserve_usd", 0),
    })
    return summary


def finding_message(throttles, kind, contention, attempts, unresolved=0, throttle_unresolved=0):
    parts = []
    if throttles:
        parts.append("throttle")
    if scaling_finding(kind, contention, attempts):
        parts.append("contention above 1 percent of write attempts")
    elif contention and not unresolved and kind == "full":
        parts.append("contention retried")
    if unresolved:
        parts.append("contention unresolved {0}".format(unresolved))
    if throttle_unresolved:
        parts.append("throttle unresolved {0}".format(throttle_unresolved))
    return ", ".join(parts)


def format_summary(summary):
    lines = []
    if summary.get("run"):
        lines.append("  run: {run}".format(**summary))
    lines.extend([
        "  latency_ms p50={p50_ms:.1f} p95={p95_ms:.1f} p99={p99_ms:.1f}".format(**summary),
        "  errors: {errors}".format(**summary),
        "  throughput: {throughput_per_s:.2f} req/s".format(**summary),
    ])
    filled = dict(summary)
    filled.setdefault("unresolved_appends", 0)
    filled.setdefault("logcheck_recheck", False)
    filled.setdefault("recheck_reserve_reads", 0)
    filled.setdefault("recheck_reserve_usd", 0)
    if "contention_503" in summary:
        lines.append(
            "  503 contention={contention_503} rate={contention_rate:.4f} "
            "throttle={throttle_503} throttle_rate={throttle_rate:.4f} "
            "other={other_503} unresolved={contention_unresolved} "
            "throttle_unresolved={throttle_unresolved} "
            "unresolved_appends={unresolved_appends} "
            "logcheck_recheck={logcheck_recheck} "
            "writes={write_attempts}".format(**filled)
        )
    if "partition_bytes" in summary:
        lines.append(
            "  modeled partition_bytes={partition_bytes} modeled reads={reads} "
            "modeled recheck_reserve_reads={recheck_reserve_reads} "
            "modeled recheck_reserve_usd={recheck_reserve_usd:.6f}".format(**filled)
        )
    return "\n".join(lines)


def write_summaries(path, reports):
    """Every tier, not only the last one."""
    path.write_text(json.dumps([item["summary"] for item in reports]), encoding="utf-8")


def render_summaries(payload):
    rows = payload if isinstance(payload, list) else [payload]
    return "\n".join(format_summary(item) for item in rows)
