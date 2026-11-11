"""Latency percentiles and throughput for one load run."""

from faults import capacity_finding


def percentile(samples, percent):
    if not samples:
        return 0
    ordered = sorted(samples)
    index = (len(ordered) - 1) * percent / 100
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def summarize(samples, errors, elapsed_ms):
    seconds = elapsed_ms / 1000 if elapsed_ms else 0
    return {
        "p50_ms": percentile(samples, 50),
        "p95_ms": percentile(samples, 95),
        "p99_ms": percentile(samples, 99),
        "errors": errors,
        "requests": len(samples),
        "throughput_per_s": (len(samples) / seconds) if seconds else 0,
    }


def tier_summary(samples, errors, elapsed_ms, counters, estimate, tier_reads):
    """Per-tier latency, 503s, partition size, and reads charged this tier."""
    attempts = counters["write_attempts"]
    summary = summarize(samples, errors, elapsed_ms)
    summary.update({
        "contention_503": counters["contention_503"],
        "throttle_503": counters["throttle_503"],
        "other_503": counters["other_503"],
        "write_attempts": attempts,
        "contention_rate": (counters["contention_503"] / attempts) if attempts else 0,
        "throttle_rate": (counters["throttle_503"] / attempts) if attempts else 0,
        "partition_bytes": estimate["partition_bytes"],
        "reads": estimate["reads"] - tier_reads,
    })
    return summary


def finding_message(throttles, kind, contention, attempts):
    parts = []
    if throttles:
        parts.append("throttle")
    if capacity_finding(0, kind, contention, attempts):
        parts.append("contention above 1 percent of write attempts")
    return ", ".join(parts)


def format_summary(summary):
    lines = [
        "  latency_ms p50={p50_ms:.1f} p95={p95_ms:.1f} p99={p99_ms:.1f}".format(**summary),
        "  errors: {errors}".format(**summary),
        "  throughput: {throughput_per_s:.2f} req/s".format(**summary),
    ]
    if "contention_503" in summary:
        lines.append(
            "  503 contention={contention_503} rate={contention_rate:.4f} "
            "throttle={throttle_503} throttle_rate={throttle_rate:.4f} "
            "other={other_503} writes={write_attempts}".format(**summary)
        )
    if "partition_bytes" in summary:
        lines.append(
            "  partition_bytes={partition_bytes} reads={reads}".format(**summary)
        )
    return "\n".join(lines)


def combine_exit(load_status, cleanup_status):
    """A cleanup failure must not replace the load's own status."""
    if load_status:
        return load_status
    return cleanup_status
