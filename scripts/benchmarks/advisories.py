"""Require a second complete comparison before an advisory notice."""

import json

from node import save


def suspects(comparison, kind):
    if kind in ("growth", "disk"):
        return {(case, name) for case, item in comparison.items() for name in item["advisories"]}
    return {(case, name) for case, item in comparison.items()
            for name, limit in (("p95_ms", 20), ("requests_per_second", -20))
            if (item["changes_percent"][name] >= limit if limit > 0
                else item["changes_percent"][name] <= limit)}


def load(output, compare, status="success"):
    manifest = json.loads((output / "benchmark.json").read_text())
    if not manifest["complete"] or status != "success":
        raise ValueError("The measurement did not complete successfully")
    comparison = compare(output, manifest)
    save(output / "comparison.json", comparison)
    return manifest, comparison


def confirm(output, manifest, comparison, compare, kind, status):
    if status != "success":
        raise ValueError("The automatic confirmation step did not complete successfully")
    initial = suspects(comparison, kind)
    repeated, second = set(), {}
    if initial:
        repeat = output / "confirmation"
        other, second = load(repeat, compare)
        for key in ("profile", "recipe", "driver_revision", "drivers", "binaries", "host"):
            if manifest[key] != other[key]:
                raise ValueError(f"The confirmation differs in {key}")
        if set(comparison) != set(second):
            raise ValueError("The confirmation measured different cases")
        repeated = suspects(second, kind)
    rows = [{"case": case, "metric": name,
             "initial": {label: comparison[case][label][name] for label in ("baseline", "candidate")},
             "repeat": {label: second[case][label][name] for label in ("baseline", "candidate")},
             "confirmed": (case, name) in repeated} for case, name in sorted(initial)]
    decision = {"kind": kind, "repeat_required": bool(initial), "results": rows,
                "repeat_only_suspects": sorted(repeated - initial)}
    save(output / "advisory-confirmation.json", decision)
    return decision


def values(pair, kind, metric=""):
    if metric.endswith("_ratio"):
        return " → ".join(f"{pair[label]:.3f}×" for label in ("baseline", "candidate"))
    divisor = 2**20 if kind in ("growth", "disk") else 1
    unit = " MiB" if kind in ("growth", "disk") else ""
    return " → ".join(f"{pair[label] / divisor:.3f}" for label in ("baseline", "candidate")) + unit


def lines(decision, required_metrics=()):
    result = ["", "## Automatic advisory confirmation", ""]
    if not decision["repeat_required"]:
        return "\n".join(result + ["No measure crossed its limit. CI skipped the repeat.", ""])
    result.extend(["CI repeated this suite once with the same programs, recipe, and fresh generated homes.",
                   "Only the same case and measure above its limit in both runs produces a notice.", "",
                   "| Case | Measure | First base → candidate | Repeat base → candidate | Result |",
                   "|---|---|---:|---:|---|"])
    for row in decision["results"]:
        state = "Did not repeat; no warning"
        if row["confirmed"]:
            state = "Required limit exceeded" if row["metric"] in required_metrics else "Confirmed advisory"
        result.append(f"| {row['case']} | {row['metric']} | {values(row['initial'], decision['kind'], row['metric'])} | "
                      f"{values(row['repeat'], decision['kind'], row['metric'])} | {state} |")
    result.extend(["", "A measure first above its limit in the repeat has no confirmation and produces no warning.",
                   "Both comparisons remain in the artifact. "
                   + ("Confirmed required limits block merge; other changes stay advisory." if required_metrics
                      else "Confirmed changes stay advisory."), ""])
    return "\n".join(result)


def notices(decision, required_metrics=()):
    for row in decision["results"]:
        if row["confirmed"]:
            required = row["metric"] in required_metrics
            level = "error" if required else "warning"
            title = "Required disk size limit exceeded" if required else f"Confirmed advisory {decision['kind']} change"
            print(f"::{level} title={title}::{row['case']}: {row['metric']}; "
                  f"first {values(row['initial'], decision['kind'], row['metric'])}; "
                  f"repeat {values(row['repeat'], decision['kind'], row['metric'])}. "
                  + ("Both comparisons exceeded the required limit. This blocks merge." if required
                     else "Both comparisons crossed the advisory limit. This does not block merge."))
