"""How many writers run, and which calls each level makes.

Five, twenty, and one hundred writers each append ten times. The 100-writer
tier finishes that work. It does not stop after 38 rounds. The scale option
runs the 100-writer tier on a three-minute clock. Trim uses 128 writers,
then snapshots and trims the log at the database delivery head.
"""

APPENDS = 10
STEADY_MS = 3 * 60 * 1000
TRIM_WRITERS = 128
LEVELS = ("5", "20", "100", "trim", "all")
PRELIMINARY_LABEL = "finished 10-append run"
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
        return Scenario("full", 100, None, False)
    raise ValueError("level")


def phases(level, scale=False):
    if level == "all":
        return [scenario(name, scale) for name in ("5", "20", "100", "trim")]
    return [scenario(level, scale)]


def still_running(spec, elapsed_ms, completed=0):
    """A steady tier with a round limit stops at that limit. The scale option stops on the clock."""
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
