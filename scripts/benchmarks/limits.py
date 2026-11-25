"""Absolute response-time limits for the candidate (Tom, 2026-11-25).

Each limit applies to the candidate's median across pairs, in milliseconds. A first-pass breach
starts the automatic confirmation pass; a breach of the same case and measure in both complete
passes fails `benchmark-complete`. The limits sit above the highest median seen on GitHub-hosted
Linux runners in the 10 main pushes and the nightly run before 2026-11-25 19:00 UTC, so runner
noise alone does not fail a PR. Relative changes against the base stay advisory.
"""

from __future__ import annotations

SINGLE_WRITE = {"p50_ms": 5.0, "p95_ms": 10.0}
BATCH_WRITE = {"p50_ms": 30.0, "p95_ms": 60.0}
SINGLE_READ = {"p50_ms": 1.0, "p95_ms": 2.0}
BATCH_READ = {"p50_ms": 10.0, "p95_ms": 15.0}

LIMITS = {
    "insert/1": SINGLE_WRITE,
    "update/1": SINGLE_WRITE,
    "concurrent-write/1": SINGLE_WRITE,
    "insert/128": BATCH_WRITE,
    "update/128": BATCH_WRITE,
    "cold-read/1": SINGLE_READ,
    "cold-read-spread/1": SINGLE_READ,
    "hot-read/1": SINGLE_READ,
    "concurrent-read/1": SINGLE_READ,
    "cold-read/32": BATCH_READ,
    "cold-read-spread/32": BATCH_READ,
    "hot-read/32": BATCH_READ,
    "concurrent-write/16": {"p50_ms": 50.0},
    "concurrent-read/16": {"p50_ms": 15.0},
}


def breaches(comparison):
    """(case, measure) pairs whose candidate median exceeds its absolute limit."""
    return {(case, name) for case, limits in LIMITS.items() if case in comparison
            for name, limit in limits.items() if comparison[case]["candidate"][name] > limit}


def decide(comparison, confirmation):
    """Rows for every limit breach of the first pass, with the repeat value and the verdict."""
    rows = []
    for case, name in sorted(breaches(comparison)):
        repeat = confirmation[case]["candidate"][name] if confirmation else None
        rows.append({"case": case, "metric": name, "limit": LIMITS[case][name],
                     "first": comparison[case]["candidate"][name], "repeat": repeat,
                     "failed": repeat is not None and repeat > LIMITS[case][name]})
    return rows


def limit_lines(comparison, rows):
    lines = ["", "## Absolute response-time limits (required)", "",
             "Candidate median across pairs. A first breach repeats the suite once;",
             "the same breach in both complete passes fails `benchmark-complete`.", "",
             "| Case | Measure | Limit ms | Candidate ms | Room |", "|---|---|---:|---:|---:|"]
    for case, limits in LIMITS.items():
        if case not in comparison:
            continue
        for name, limit in limits.items():
            value = comparison[case]["candidate"][name]
            lines.append(f"| {case} | {name} | {limit:.1f} | {value:.3f} | {100 * (1 - value / limit):+.0f}% |")
    if rows:
        lines.extend(["", "| Breach | First ms | Repeat ms | Result |", "|---|---:|---:|---|"])
        for row in rows:
            repeat = f"{row['repeat']:.3f}" if row["repeat"] is not None else "missing"
            verdict = "FAILED in both passes" if row["failed"] else "Cleared in the repeat"
            lines.append(f"| {row['case']} {row['metric']} ≤ {row['limit']:.1f} | {row['first']:.3f} | {repeat} | {verdict} |")
    return lines
