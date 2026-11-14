"""Classify mutation-log HTTP results and the process exit codes.

A contention 503 is a RetryLater on the database head transaction. The
client retries at every writer tier. A successful retry is a capacity
finding for a fixed-size tier, and for the timed tier it is a finding when
contention exceeds 1% of write attempts. An unresolved write is a capacity
finding only when a complete log audit finds every accepted entry and no
partial write. Throttling is a capacity finding at every tier.

Exit codes: 0 complete, 1 correctness, 2 the $6 cap, 3 a capacity finding.
Exit 1 wins over 3, and 3 wins over 2.

The client makes up to twenty attempts. After each retryable failure the
wait is ``base = min(2000, 50 * 2^(attempt-1))`` milliseconds, jittered from
``base/2`` through ``base``. A read throttle that is still failing after
those attempts during logcheck does not fail the tier yet. Writers have
already stopped. The harness waits one quiet interval, then reads the log
again. That recheck uses the same attempt limit with a longer wait:
``base = min(8000, 400 * 2^(attempt-1))`` milliseconds, jittered the same
way. The tier is exit 1 only when that quiet recheck still cannot verify
the log.
"""

CONTENTION_CODE = "retry_later"
CONTENTION_FIELD = "mutation_log.transaction.conflict"
THROTTLE_FIELDS = frozenset((
    "mutation_log.transaction.throttled",
    "mutation_log.submission.throttled",
    "mutation_log.query.throttled",
))
CONTENTION_RATE = 0.01
# Eight tries left appends unstored at 100 writers on DEV. Twenty stored every append.
MAX_CONTENTION_TRIES = 20
QUIET_BASE_MS = 400
QUIET_CAP_MS = 8000
EXIT_OK = 0
EXIT_CORRECTNESS = 1
EXIT_CAP = 2
EXIT_SCALING = 3


def is_contention(status, payload):
    """True only for the head-row transaction conflict."""
    if status != 503 or not isinstance(payload, dict):
        return False
    return (
        payload.get("code") == CONTENTION_CODE
        and payload.get("field") == CONTENTION_FIELD
    )


class RetriesExhausted(Exception):
    """A conflict or throttle used every client retry and still failed."""


def is_throttle(status, payload):
    """DynamoDB throttle. A capacity finding at every tier, once it succeeds."""
    if status != 503 or not isinstance(payload, dict):
        return False
    return (
        payload.get("code") == CONTENTION_CODE
        and payload.get("field") in THROTTLE_FIELDS
    )


def is_duplicate(payload):
    """An accepted append that stored nothing."""
    if not isinstance(payload, dict):
        return False
    for key in ("result", "outcome", "status"):
        if payload.get(key) == "duplicate":
            return True
    duplicate = payload.get("duplicate")
    return duplicate is True or isinstance(duplicate, dict)


def already_created(operation, status, payload):
    """A rerun keeps the run id, so create-database answers 409."""
    if operation != "create-database" or status != 409 or not isinstance(payload, dict):
        return False
    return payload.get("code") == "identity_conflict"


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
    if contention:
        return "unresolved"
    return "fail"


def scaling_finding(kind, contention, attempts):
    """100 writers, contention above 1% of write attempts."""
    if kind != "steady" or not attempts:
        return False
    return (contention / attempts) > CONTENTION_RATE


def capacity_finding(throttles, kind, contention, attempts):
    """A throttle, a fixed-tier conflict, or timed contention above 1%."""
    if throttles:
        return True
    if kind == "full" and contention:
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
