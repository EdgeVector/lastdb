"""Paired operation throughput, RPC/body cost and five-percent investigation signals."""

from pathlib import Path
import os
import statistics

from alternate_key_workload import require
from node import save
from record_replacement_workload import OPERATIONS


def quantile(values, fraction):
    ordered = sorted(values)
    return ordered[int((len(ordered) - 1) * fraction)]


def metrics(rows):
    require(bool(rows), "replacement report has no observations")
    groups = {}
    for row in rows:
        groups.setdefault(row["pair"], []).append(row)
    wall_ms = 0
    for items in groups.values():
        require(len(items) == OPERATIONS and len({r["ordinal"] for r in items}) == OPERATIONS,
                "replacement case lost successful operations")
        require(len({r["case_wall_ms"] for r in items}) == 1, "case wall duration differs")
        wall_ms += items[0]["case_wall_ms"]
    return {"successful_updates": len(rows), "p95_ms": quantile([r["latency_ms"] for r in rows], .95),
            "updates_per_second": len(rows) * 1000 / wall_ms,
            **{name: statistics.mean(r[name] for r in rows)
               for name in ("rpc_count", "request_body_bytes", "response_body_bytes")}}


def report(args, records):
    selected = [r for r in records if r["case"].startswith("replacement_")]
    lines = ["## Whole-record replacement preparation", "",
             "Complete records use acknowledged record revisions.",
             "Application cases include the keyed read, complete reply, serialization and durable write.",
             "Each worker owns distinct records; actual RPC and retry counts are retained. Zero conflicts do not establish contention cost.",
             "Request/response body bytes exclude HTTP headers. Throughput counts successful logical updates.",
             "Four AB/BA pairs and all per-pair results are retained; no best-run selection.",
             f"Each case has {OPERATIONS} successful updates, including 16 per client at 16 clients.",
             "Repeated >5% deterioration in at least three pairs is an investigation signal for this series.",
             "It does not alter the repository's global advisory or required completion gates.", "",
             "| Case | Base p95 ms | New p95 ms | p95 change | Throughput change | Investigate |",
             "|---|---:|---:|---:|---:|---|"]
    results = {}
    for case in sorted({r["case"] for r in selected}):
        arms = {label: metrics([r for r in selected if r["case"] == case and r["label"] == label])
                for label in ("baseline", "candidate")}
        pairs = []
        for pair in range(4):
            values = {label: metrics([r for r in selected if r["case"] == case
                                     and r["label"] == label and r["pair"] == pair])
                      for label in arms}
            pairs.append({"pair": pair, **values,
                          "p95_change_percent": (values["candidate"]["p95_ms"] / values["baseline"]["p95_ms"] - 1) * 100,
                          "throughput_change_percent": (values["candidate"]["updates_per_second"] /
                                                        values["baseline"]["updates_per_second"] - 1) * 100})
        latency = (arms["candidate"]["p95_ms"] / arms["baseline"]["p95_ms"] - 1) * 100
        throughput = (arms["candidate"]["updates_per_second"] / arms["baseline"]["updates_per_second"] - 1) * 100
        investigate = ((latency > 5 and sum(p["p95_change_percent"] > 5 for p in pairs) >= 3)
                       or (throughput < -5 and sum(p["throughput_change_percent"] < -5 for p in pairs) >= 3))
        results[case] = {**arms, "pairs": pairs, "investigate": investigate}
        lines.append(f"| {case} | {arms['baseline']['p95_ms']:.3f} | {arms['candidate']['p95_ms']:.3f} | "
                     f"{latency:+.1f}% | {throughput:+.1f}% | {'YES' if investigate else ''} |")
    text = "\n".join(lines) + "\n"
    save(args.output / "replacement-summary.json",
         {"baseline": args.baseline_revision, "candidate": args.candidate_revision, "cases": results})
    (args.output / "replacement-summary.md").write_text(text)
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as stream:
        stream.write(text)
    print(text)
