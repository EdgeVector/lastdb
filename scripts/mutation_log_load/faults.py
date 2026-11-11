"""Classify mutation-log HTTP results and the process exit codes.

A contention 503 is HTTP 503 whose body is a ``ProtocolError`` with code
``retry_later`` and field ``mutation_log.transaction.conflict``.
``RetryLater`` is ``ErrorCode`` in ``cloud_backup_protocol/src/error.rs``
(lines 42-43). ``rename_all = "snake_case"`` (line 5) makes the wire code
``retry_later``. ``OutcomeUnknown`` (lines 40-41) is ``outcome_unknown`` and
is not contention. ``ProtocolError`` serializes as ``{"code", "field"}``
(lines 47-54, ``deny_unknown_fields``). The field string is the head-row
conflict raised in exemem ``uploads/store769.rs`` around lines 1004-1005
and 1023-1028.

Exit codes: 0 complete, 1 correctness, 2 the $6 cap, 3 scaling. Exit 3 is
only contention above 1% of write attempts at 100 writers. It is not a
correctness failure. Exit 1 wins over 3, and 3 wins over 2.
"""

CONTENTION_CODE = "retry_later"
CONTENTION_FIELD = "mutation_log.transaction.conflict"
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


def scaling_finding(kind, contention, attempts):
    """100 writers, contention above 1% of write attempts."""
    if kind != "steady" or not attempts:
        return False
    return (contention / attempts) > CONTENTION_RATE


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
