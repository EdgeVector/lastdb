#!/usr/bin/env python3
"""Restore whole-map keep_small persist so the scale test can go RED."""

from pathlib import Path

TARGET = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "db_operations"
    / "atom_store"
    / "mod.rs"
)
OLD = "self.write_keep_small_header_and_shards(&snapshot, Some(&dirty))"
NEW = "self.write_keep_small_header_and_shards(&snapshot, None)"


def main() -> None:
    text = TARGET.read_text()
    count = text.count(OLD)
    if count != 1:
        raise SystemExit(f"anchor count={count} path={TARGET} old={OLD!r}")
    TARGET.write_text(text.replace(OLD, NEW, 1))


if __name__ == "__main__":
    main()
