"""How many writers run, and which calls each level makes.

Five and twenty writers each append ten times, then ack the head cursor.
The default 100-writer tier is a preliminary correctness-and-contention run:
each writer stops after about 38 rounds, under the $6 ledger. The 3-minute
scale run stays behind ``scale=True`` until bounded reads land. Trim uses
128 devices. They append, then ack the head cursor, then snapshot and trim.
``through`` is a delivery sequence from head, never a writer sequence.
"""

APPENDS = 10
PRELIMINARY_ROUNDS = 38
STEADY_MS = 3 * 60 * 1000
TRIM_WRITERS = 128
LEVELS = ("5", "20", "100", "trim", "all")
PRELIMINARY_LABEL = "preliminary correctness-and-contention run"
SCALE_LABEL = "3-minute scale run"


class Scenario:
    def __init__(self, kind, writers, steady_ms, trim, steady_rounds=None):
        self.kind = kind
        self.writers = writers
        self.steady_ms = steady_ms
        self.trim = trim
        self.steady_rounds = steady_rounds


def scenario(level, scale=False):
    if level == "trim":
        return Scenario("trim", TRIM_WRITERS, None, True)
    if level in ("5", "20"):
        return Scenario("full", int(level), None, False)
    if level == "100":
        if scale:
            return Scenario("steady", 100, STEADY_MS, False)
        return Scenario("steady", 100, None, False, PRELIMINARY_ROUNDS)
    raise ValueError("level")


def phases(level, scale=False):
    if level == "all":
        return [scenario(name, scale) for name in ("5", "20", "100", "trim")]
    return [scenario(level, scale)]


def still_running(spec, elapsed_ms, completed=0):
    """Rounds stop the preliminary tier. The scale option stops on the clock."""
    if spec.steady_rounds is not None:
        return completed < spec.steady_rounds
    if spec.steady_ms is None:
        return True
    return elapsed_ms < spec.steady_ms


def writer_calls(spec, index):
    if spec.kind == "trim":
        return [("append", index, 1)]
    if spec.kind == "full":
        return [("append", index, seq) for seq in range(1, APPENDS + 1)]
    return [("append", index, 1)]
