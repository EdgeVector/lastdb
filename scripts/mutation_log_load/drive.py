"""Drive one guarded load. The transport is injected, so this does no I/O."""

import json

from cost import CapExceeded, format_estimate
from guard import prefix_for
from plan import operations, still_running
from stats import format_summary, summarize


def path_for(database_id, operation):
    prefix = prefix_for(database_id)
    return "/" + prefix + operation


def body_for(operation, writer, sequence):
    if operation == "trim":
        payload = {"writers": writer, "now_ms": sequence}
    else:
        payload = {
            "writer_key": "load-writer-{0}".format(writer),
            "device": "load-device",
            "writer_seq": sequence,
            "payload": "load",
        }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def drive(spec, database_id, transport, ledger, clock):
    """Run until the script ends, the steady window ends, or the cap trips.

    ``clock`` returns milliseconds. ``transport(method, path, body)`` returns
    ``(status, elapsed_ms)``. A status of 400 or more counts as an error.
    """
    samples, errors = [], 0
    started = clock()
    try:
        for operation, writer, sequence in operations(spec):
            # Full and trim scripts pass a zero round count, so they run to
            # the end. The steady level stops when the clock reaches 3 minutes.
            if not still_running(spec, clock() - started, 0):
                break
            body = body_for(operation, writer, sequence)
            written = len(body) if operation in ("append", "trim") else None
            ledger.charge(1, written)
            status, latency = transport(
                "POST", path_for(database_id, operation), body)
            samples.append(latency)
            if status >= 400:
                errors += 1
    except CapExceeded as exceeded:
        report = _report(samples, errors, clock() - started, exceeded.estimate)
        report["stopped"] = "cap"
        return report
    estimate = ledger.estimate()
    report = _report(samples, errors, clock() - started, estimate)
    report["stopped"] = "complete"
    return report


def _report(samples, errors, elapsed_ms, estimate):
    summary = summarize(samples, errors, elapsed_ms)
    text = format_estimate(estimate) + "\n" + format_summary(summary)
    return {"summary": summary, "estimate": estimate, "text": text}
