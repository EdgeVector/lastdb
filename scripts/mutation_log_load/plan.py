"""How many writers run, and which calls each level makes.

Five and twenty writers each append ten times, then ack the head cursor.
One hundred writers repeat append and that same ack for three minutes.
Trim uses 128 devices. They append, then ack the head cursor, then snapshot and trim.
``through`` is a delivery sequence from head, never a writer sequence.
"""

APPENDS = 10
STEADY_MS = 3 * 60 * 1000
TRIM_WRITERS = 128
LEVELS = ("5", "20", "100", "trim", "all")


class Scenario:
    def __init__(self, kind, writers, steady_ms, trim):
        self.kind = kind
        self.writers = writers
        self.steady_ms = steady_ms
        self.trim = trim


def scenario(level):
    if level == "trim":
        return Scenario("trim", TRIM_WRITERS, None, True)
    if level in ("5", "20"):
        return Scenario("full", int(level), None, False)
    if level == "100":
        return Scenario("steady", 100, STEADY_MS, False)
    raise ValueError("level")


def phases(level):
    if level == "all":
        return [scenario(name) for name in ("5", "20", "100", "trim")]
    return [scenario(level)]


def still_running(spec, elapsed_ms):
    if spec.steady_ms is None:
        return True
    return elapsed_ms < spec.steady_ms


def writer_calls(spec, index):
    if spec.kind == "trim":
        return [("append", index, 1)]
    if spec.kind == "full":
        return [("append", index, seq) for seq in range(1, APPENDS + 1)]
    return [("append", index, 1)]
