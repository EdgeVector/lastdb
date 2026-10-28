#!/usr/bin/env python3
"""Name the exact measured control for an explicitly recorded broken main build."""

import argparse
import re
import sys


# Main #657 used Option methods after #654 made schema_profile concrete.
# Required CI failed: https://github.com/EdgeVector/lastdb/actions/runs/40764592569
# The immediate predecessor passed required CI. Never select a fallback for
# another revision or hide a candidate compilation failure.
BROKEN_BASELINES = {
    "5e1856059cefc9a3ba719964a9914e23ca0c4c37": "558926c35ee278f5fff0e2e59d60a821905bab01",
}


def measurement_baseline(revision):
    return BROKEN_BASELINES.get(revision, revision)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("revision")
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("baseline must be a full 40-character Git SHA")
    baseline = measurement_baseline(args.revision)
    if baseline != args.revision:
        print(f"Known broken baseline {args.revision}; measured control {baseline}", file=sys.stderr)
    print(baseline)


if __name__ == "__main__":
    main()
