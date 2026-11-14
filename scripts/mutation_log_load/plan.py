"""Plan the fixed append tiers and the optional timed 100-writer tier."""

APPENDS = 10
STEADY_MS = 3 * 60 * 1000
WRITER_LEVELS = ("5", "20", "100")
NATIVE_CUTOFF = "private-native-cutoff"
PRIVATE_LEVELS = ("private", NATIVE_CUTOFF)
LEVELS = (*WRITER_LEVELS, "all", *PRIVATE_LEVELS)
PRELIMINARY_LABEL = "finished 10-append run"
SCALE_LABEL = "3-minute scale run"


class Scenario:
    def __init__(self, kind, writers, steady_ms=None):
        self.kind = kind
        self.writers = writers
        self.steady_ms = steady_ms


def scenario(level, scale=False):
    if level not in WRITER_LEVELS:
        raise ValueError("level")
    if level == "100" and scale:
        return Scenario("steady", 100, STEADY_MS)
    return Scenario("full", int(level))


def phases(level, scale=False):
    levels = WRITER_LEVELS if level == "all" else (level,)
    return [scenario(name, scale) for name in levels]
