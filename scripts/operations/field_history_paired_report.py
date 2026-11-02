#!/usr/bin/env python3
"""Validate direct memory comparisons from real generated CI homes."""
import argparse
import json
import os
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from growth import CASES  # noqa: E402
from growth_report import percent, validate  # noqa: E402
from node import save  # noqa: E402

MIB = 1024 * 1024


def load_run(root, pair, label, recipe):
    directory = root / f"pair-{pair}-{label}"
    cases = json.loads((directory / "growth.json").read_text())
    if set(cases) != set(CASES):
        raise ValueError("The measured cases differ from the workload")
    snapshots = ["boot", "boot-idle"] + [
        f"{case}-{number}" for case in CASES for number in range(recipe["growth"]["rounds"] + 1)]
    for name in snapshots:
        point = json.loads((directory / f"{name}.json").read_text())
        if point.get("transparent_huge_pages_disabled") is not True or point["huge_page_bytes"] != 0:
            raise ValueError(f"{directory.name}/{name}: huge pages are not verified disabled")
    for case, result in cases.items():
        validate(case, result, recipe["growth"])
    cleanup = json.loads((directory / "cleanup.json").read_text())
    if not cleanup.get("removed") or cleanup.get("daemon_stopped") is not True:
        raise ValueError(f"{directory.name}: generated home cleanup is incomplete")
    return cases


def compare(runs, case):
    values = {label: [pair[label][case]["metrics"]["idle_memory_bytes"] for pair in runs]
              for label in ("baseline", "candidate")}
    means = {label: statistics.mean(samples) for label, samples in values.items()}
    differences = [right - left for left, right in zip(values["baseline"], values["candidate"])]
    kinds = {label: {kind: statistics.mean(
        pair[label][case]["checkpoints"][-1]["attribution"]["rss_by_kind"].get(kind, 0)
        for pair in runs) for kind in ("anonymous", "file", "thread_stack")} for label in values}
    return {"idle_memory_bytes": means, "difference_bytes": statistics.mean(differences),
            "difference_percent": percent(means["baseline"], means["candidate"]),
            "paired_difference_bytes": differences,
            "paired_difference_percent": [percent(left, right) for left, right in zip(
                values["baseline"], values["candidate"])],
            "final_mapping_bytes": kinds}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.output / "measurements"
    recipe = json.loads((root / "recipe.json").read_text())
    if recipe["disable_thp"] is not True or recipe["allocation_profile"] is not False:
        raise ValueError("The direct comparison needs disabled huge pages without allocation instrumentation")
    transfer = json.loads((args.output / "program-transfer.json").read_text())
    if any(transfer["programs"][label]["revision"] != recipe[label] for label in ("baseline", "candidate")):
        raise ValueError("The measured revisions differ from the verified program transfer")
    runs = [{label: load_run(root, pair, label, recipe) for label in ("baseline", "candidate")}
            for pair in range(recipe["pairs"])]
    result = {case: compare(runs, case) for case in CASES}
    save(args.output / "comparison.json", {"recipe": recipe, "cases": result})
    lines = ["## Direct paired field history memory comparison", "",
             f"Baseline: `{recipe['baseline']}`; candidate: `{recipe['candidate']}`.",
             f"Pairs: {recipe['pairs']}; one runner; alternating order; fresh generated homes.",
             "Every checkpoint reports disabled kernel huge pages and zero huge page bytes.", "",
             "| Case | Baseline MiB | Candidate MiB | Extra MiB | Extra % | Paired extra MiB |",
             "| --- | ---: | ---: | ---: | ---: | --- |"]
    for case, row in result.items():
        before, after = (row["idle_memory_bytes"][label] / MIB for label in ("baseline", "candidate"))
        spread = ", ".join(f"{value / MIB:+.2f}" for value in row["paired_difference_bytes"])
        lines.append(f"| {case} | {before:.2f} | {after:.2f} | {row['difference_bytes'] / MIB:+.2f} "
                     f"| {row['difference_percent']:+.2f}% | {spread} |")
    lines += ["", "These exact versions also differ by other intervening changes.",
              "This comparison measures their net difference; it does not attribute every byte to history.",
              "No primary access. This manual diagnostic does not replace benchmark-complete."]
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as summary:
        summary.write("\n".join(lines) + "\n")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
