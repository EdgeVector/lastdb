#!/usr/bin/env python3
"""Compare a fresh criterion bench run against the committed fold_db baseline.

The scheduled / label-gated `bench` CI job (.github/workflows/bench.yml) runs
the fold_db benches, then runs this script to flag perf regressions. There is
no other enforcement: the benches' own doc comments used to carry hand-typed
numbers that went stale and let the 2026-06-20 O(N-schema)-per-query wedge slip
past. This script makes the committed baseline
(fold_db/crates/core/benches/baseline/baseline.json) the source of truth.

For each guarded case it reads criterion's per-benchmark output
  <criterion-dir>/<group>/<id>/new/estimates.json
takes the MEDIAN point-estimate (ns), and FAILS the case when
  fresh_median_ns > baseline_ns * regression_factor.

regression_factor comes from the case (FLAT cases are tighter), else the
manifest's default_regression_factor (the card's ">2x" bar).

Exit codes: 0 = all guarded cases within ceiling; 1 = at least one regression;
2 = a usage / missing-data error (e.g. the bench run produced no estimates for a
guarded case — a structural problem the job should surface, not swallow).

Usage:
  bench-compare.py [--baseline PATH] [--criterion-dir PATH] [--factor F]

Defaults assume it is run from the repo root with criterion output under
`target/criterion` (criterion's default).
"""

import argparse
import json
import os
import sys

REPO_REL_BASELINE = "fold_db/crates/core/benches/baseline/baseline.json"


def env_truthy(name):
    return os.environ.get(name, "").lower() in ("1", "true", "yes")


def load_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def median_ns(criterion_dir, group, ident):
    """Return the median point-estimate (ns) for one criterion benchmark.

    Returns None when the estimates file is missing (the bench didn't run /
    produced no output for this case)."""
    est_path = os.path.join(criterion_dir, group, ident, "new", "estimates.json")
    if not os.path.isfile(est_path):
        return None
    est = load_json(est_path)
    # criterion 0.5 estimates.json: {"median": {"point_estimate": <ns>, ...}, ...}
    try:
        return float(est["median"]["point_estimate"])
    except (KeyError, TypeError, ValueError):
        return None


def fmt_ns(ns):
    if ns is None:
        return "n/a"
    if ns >= 1e9:
        return f"{ns / 1e9:.3f} s"
    if ns >= 1e6:
        return f"{ns / 1e6:.2f} ms"
    if ns >= 1e3:
        return f"{ns / 1e3:.1f} us"
    return f"{ns:.0f} ns"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", default=REPO_REL_BASELINE,
                    help=f"path to baseline.json (default: {REPO_REL_BASELINE})")
    ap.add_argument("--criterion-dir", default=os.path.join("target", "criterion"),
                    help="criterion output dir (default: target/criterion)")
    ap.add_argument("--factor", type=float, default=None,
                    help="override regression factor for ALL cases (else per-case / manifest default)")
    args = ap.parse_args()

    if not os.path.isfile(args.baseline):
        print(f"::error::baseline manifest not found: {args.baseline}", file=sys.stderr)
        return 2
    manifest = load_json(args.baseline)
    default_factor = float(manifest.get("default_regression_factor", 2.0))
    cases = manifest.get("cases", [])
    if not cases:
        print("::error::baseline manifest has no cases", file=sys.stderr)
        return 2

    if not os.path.isdir(args.criterion_dir):
        print(f"::error::criterion output dir not found: {args.criterion_dir} "
              "(did the bench run?)", file=sys.stderr)
        return 2

    regressions = []
    missing = []
    skipped = []
    ok = []
    fast_guard = env_truthy("FOLD_BENCH_FAST_GUARD")

    for case in cases:
        group = case["group"]
        ident = str(case["id"])
        baseline = float(case["baseline_ns"])
        factor = args.factor if args.factor is not None \
            else float(case.get("regression_factor", default_factor))
        ceiling = baseline * factor

        fresh = median_ns(args.criterion_dir, group, ident)
        label = f"{group}/{ident}"
        if fresh is None:
            if fast_guard and case.get("fast_guard_optional"):
                skipped.append(label)
                continue
            missing.append(label)
            continue
        ratio = fresh / baseline if baseline else float("inf")
        row = (label, baseline, fresh, ceiling, factor, ratio)
        if fresh > ceiling:
            regressions.append(row)
        else:
            ok.append(row)

    # Report.
    print("== fold_db bench guard ==")
    print(f"baseline: {args.baseline}")
    print(f"criterion: {args.criterion_dir}")
    print()
    header = f"{'case':<40} {'baseline':>12} {'measured':>12} {'ceiling':>12} {'ratio':>7}"
    print(header)
    print("-" * len(header))
    for label, base, fresh, ceil_, factor, ratio in sorted(ok + regressions):
        flag = "  REGRESSION" if fresh > ceil_ else ""
        print(f"{label:<40} {fmt_ns(base):>12} {fmt_ns(fresh):>12} "
              f"{fmt_ns(ceil_):>12} {ratio:>6.2f}x{flag}")
    for label in sorted(skipped):
        print(f"{label:<40} {'skipped':>12} {'fast guard':>12} {'n/a':>12} {'':>7}")

    rc = 0
    if missing:
        for label in missing:
            print(f"::error::no criterion estimates for guarded case '{label}' "
                  "— the bench did not run or produced no output", file=sys.stderr)
        rc = 2
    if regressions:
        for label, base, fresh, ceil_, factor, ratio in regressions:
            print(f"::error::perf regression in {label}: {fmt_ns(fresh)} > "
                  f"{factor:.2f}x baseline {fmt_ns(base)} (ceiling {fmt_ns(ceil_)})",
                  file=sys.stderr)
        rc = 1 if rc == 0 else rc

    if rc == 0:
        suffix = f"; skipped {len(skipped)} fast-guard optional cases" if skipped else ""
        print(f"\nAll {len(ok)} guarded cases within ceiling{suffix}.")
    return rc


if __name__ == "__main__":
    sys.exit(main())
