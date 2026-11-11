"""Classify mutation-log HTTP results and the process exit codes.

A contention 503 is HTTP 503 whose body is a ``ProtocolError`` with code
``retry_later`` and field ``mutation_log.transaction.conflict``. At 5, 20,
and 100 writers, and on the trim tier, the client retries it. At 5 and 20
writers, and on the trim tier, a retry that succeeds is exit 3. At 100
writers a retry that succeeds and stays at or under 1% of write attempts
is exit 0. Above 1% at 100 writers is exit 3. A write that is still
failing after every retry is exit 3 only when that write left no partial
row, the log invariants hold, and nothing acknowledged was lost.
Otherwise it is exit 1. The same intact-log rule applies to a throttle
that is still failing after every retry. Any other trim failure,
including ``through`` below 128, a head cursor below 128, or fewer than
128 appends from the trim tier itself, is exit 1.

A throttle 503 is the same code with field
``mutation_log.transaction.throttled``,
``mutation_log.submission.throttled``, or ``mutation_log.query.throttled``.
A retry that succeeds is exit 3. Plain ``mutation_log.query`` stays exit 1.
``RetryLater`` is ``ErrorCode`` in ``cloud_backup_protocol/src/error.rs``
(lines 42-43). ``rename_all = "snake_case"`` (line 5) makes the wire code
``retry_later``. ``OutcomeUnknown`` (lines 40-41) is ``outcome_unknown``
and stops the run. ``ProtocolError`` serializes as ``{"code", "field"}``
(lines 47-54, ``deny_unknown_fields``).

Exit codes: 0 complete, 1 correctness, 2 the $6 cap, 3 a capacity finding.
Exit 1 wins over 3, and 3 wins over 2.
"""

CONTENTION_CODE = "retry_later"
CONTENTION_FIELD = "mutation_log.transaction.conflict"
THROTTLE_FIELDS = frozenset((
    "mutation_log.transaction.throttled",
    "mutation_log.submission.throttled",
    "mutation_log.query.throttled",
    "mutation_log.snapshot.throttled",
))
CONTENTION_RATE = 0.01
MAX_CONTENTION_TRIES = 8
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


class UnresolvedContention(RetriesExhausted):
    """A conflicted write used every client retry and still failed."""


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
    base = min(2000, 50 * (2 ** (tries - 1)))
    low = base // 2
    return (low + draw(base - low + 1)) / 1000.0


def retry_action(throttle, contention, tries, allow_contention):
    """``retry``, ``unresolved``, ``fail``, or ``done`` for one 503."""
    if not throttle and not contention:
        return "done"
    retryable = throttle or (contention and allow_contention)
    if retryable and tries + 1 < MAX_CONTENTION_TRIES:
        return "retry"
    if contention and allow_contention:
        return "unresolved"
    return "fail"


def scaling_finding(kind, contention, attempts):
    """100 writers, contention above 1% of write attempts."""
    if kind != "steady" or not attempts:
        return False
    return (contention / attempts) > CONTENTION_RATE


def capacity_finding(throttles, kind, contention, attempts):
    """A succeeded throttle, a retried conflict at 5, 20, or trim, or contention above 1%."""
    if throttles:
        return True
    if kind in ("full", "trim") and contention:
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
