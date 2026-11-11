"""Latency percentiles and throughput for one load run."""


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


def format_summary(summary):
    return "\n".join([
        "  latency_ms p50={p50_ms:.1f} p95={p95_ms:.1f} p99={p99_ms:.1f}".format(**summary),
        "  errors: {errors}".format(**summary),
        "  throughput: {throughput_per_s:.2f} req/s".format(**summary),
    ])
