"""Writer counts and how long each level runs.

Five and twenty writers finish a closed script: ten appends, one list, and
one ack each. One hundred writers repeat that cycle for three minutes.
The trim case appends once for more than one hundred writers, then trims.
"""

APPENDS = 10
STEADY_MS = 3 * 60 * 1000
TRIM_WRITERS = 128


class Scenario:
    def __init__(self, kind, writers, rounds, steady_ms, trim):
        self.kind = kind
        self.writers = writers
        self.rounds = rounds
        self.steady_ms = steady_ms
        self.trim = trim


def scenario(level):
    if level == "trim":
        return Scenario("trim", TRIM_WRITERS, 1, None, True)
    if level in ("5", "20"):
        return Scenario("full", int(level), 1, None, False)
    if level == "100":
        return Scenario("steady", 100, None, STEADY_MS, False)
    raise ValueError("level")


def still_running(spec, elapsed_ms, completed_rounds):
    """Five and twenty ignore the clock. One hundred stops at three minutes."""
    if spec.steady_ms is not None:
        return elapsed_ms < spec.steady_ms
    return completed_rounds < spec.rounds


def operations(spec):
    if spec.kind == "trim":
        for writer in range(spec.writers):
            yield ("append", writer, 1)
        yield ("trim", spec.writers, 1)
        return
    first = 1
    while True:
        yield from _pass(spec.writers, first)
        if spec.kind == "full":
            return
        first += APPENDS


def _pass(writers, first):
    for writer in range(writers):
        for offset in range(APPENDS):
            yield ("append", writer, first + offset)
        yield ("list", writer, 0)
        yield ("ack", writer, first + APPENDS - 1)
