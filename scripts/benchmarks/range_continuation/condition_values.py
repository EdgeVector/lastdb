"""Small keyed condition data within the existing generated daemon recipes."""

import hashlib

HASH, COUNT = "condition-reuse", 128
EXTRA_FIELDS = ["score", "optional", "never"]


def marker(source=None):
    return "condition-source-" + str(0 if source is None else source + 1)


def extend(values, source=None):
    if values["bucket"] != HASH:
        return values
    number = int(values["id"].rsplit("-", 1)[1])
    rank = 0 if source is None else source + 1
    # Only this fixed hash uses nested JSON. Eight 1KiB strings expose value
    # copies while the complete response stays below the existing 4MiB cap.
    seed = values["body"]
    values["body"] = {"source": marker(source), "identity": {"hash": HASH, "range": values["id"]},
        "parts": [{"position": part, "flags": [True, None, number],
            "payload": hashlib.shake_256((seed + str(part)).encode()).hexdigest(512)}
            for part in range(8)]}
    values["title"] = marker(source) + "-class-" + str(number % 4)
    values["score"] = rank * 100 + number % 16
    if number % 4:
        values["optional"] = None if number % 4 == 1 else (
            rank * 100 + number if number % 4 == 2 else "not-numeric")
    # `never` is declared but never written. Its captured map stays empty.
    return values
