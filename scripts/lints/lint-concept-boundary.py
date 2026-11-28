#!/usr/bin/env python3
"""Fail when an app-owned sharing concept appears in LastDB non-app code.

Named presets (Share, Deliver, Org) and People ideas (circles, albums,
comments, social/* schemas) belong only in the People app
(EdgeVector/people). LastDB and ExeMem pass and enforce generic
configuration: one signed grant per member with role
read|submit|write|revoked, and one write lease per database.

This is a source check, not a unit test. Exit 0 on a clean tree, 1
otherwise. Extend CONCEPTS to ban more app vocabulary later.

Usage: lint-concept-boundary.py [--root DIR]
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Concept:
    """One app-owned idea that must not appear in LastDB product code."""

    concept_id: str
    belongs_in: str
    patterns: tuple[re.Pattern[str], ...]


def _p(*exprs: str) -> tuple[re.Pattern[str], ...]:
    return tuple(re.compile(e) for e in exprs)


# Add new app concepts here. Keep patterns specific enough to avoid
# protocol words such as ShareEnvironment or Delivery sequence.
CONCEPTS: tuple[Concept, ...] = (
    Concept(
        concept_id="people.schema.social",
        belongs_in="People app (EdgeVector/people)",
        patterns=_p(
            r"STANDARD_COMMENT_SCHEMA",
            r"social/Comment",
            r"social/AlbumItem",
            r"social/Album",
            r"social/Photo",
            r"social/Card",
            r"social/CircleNote",
            r"social/Circle",
            r"social/Note",
            r"social/Outside",
            r'namespace:\s*"social"',
            r'namespaces:\s*&\["social"\]',
            r'\("social",\s*1\)',
            r'\("social"\s*\|\s*"lastdb-files"',
            r'"lastdb-files"\s*\|\s*"social"',
        ),
    ),
    Concept(
        concept_id="people.preset.share_deliver_org",
        belongs_in="People app (EdgeVector/people)",
        patterns=_p(
            r"Share \(the default\)",
            r"\bpartially Org\b",
            # Deliver as a sharing preset, not Delivery/Delivered.
            r"(?<![A-Za-z])Deliver(?![A-Za-z])",
            # Org as a named sharing preset next to Share/Deliver wording.
            r"\bOrg\b(?=\s*:\s*`?(?:members|anyone|direct|read))",
            r"\|\s*Org\s*\|",
        ),
    ),
    Concept(
        concept_id="people.app.circles_albums",
        belongs_in="People app (EdgeVector/people)",
        patterns=_p(
            r"people-circles",
            r"shared-albums",
            r"@lastdb/people-client",
            r"peopleClient",
            r"\bcircles\b",
        ),
    ),
)

# Paths scanned relative to the repo root. Product surface only.
# Intentionally not scanned (examples / People-driving recipes stay there):
# docs/, AGENTS.md, README.md, scripts/operations/, lastdb_app_sdk/README.md,
# schema_service/ (canonical field registry, not share presets).
SCAN_PREFIXES: tuple[str, ...] = (
    "fold_db/",
    "lastdb_node/",
    "cloud_backup_protocol/",
    "mutation_log/",
    "database_policy/",
    "lastdb_app_sdk/src/",
    "lastdb_host/",
    "lastdb_uds/",
    "lastdb_identity/",
    "folddb_profile/",
    "app_identity_crypto/",
    "observability/",
)

SCAN_SUFFIXES: tuple[str, ...] = (
    ".rs",
    ".ts",
    ".tsx",
    ".js",
    ".mjs",
    ".cjs",
    ".json",
)

# Short allowlist: path prefix or exact relative path -> reason.
# Keep this short. Prefer deleting the leak over growing the list.
ALLOWLIST: dict[str, str] = {}


def _repo_root(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit.resolve()
    return Path(__file__).resolve().parents[2]


def _tracked_files(repo: Path) -> list[str]:
    proc = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "-z"],
        capture_output=True,
        check=False,
    )
    if proc.returncode == 0:
        return [p.decode("utf-8", "replace") for p in proc.stdout.split(b"\0") if p]
    found: list[str] = []
    for root, dirnames, filenames in os.walk(repo):
        dirnames[:] = sorted(d for d in dirnames if d not in (".git", "target", "node_modules"))
        found.extend((Path(root) / f).relative_to(repo).as_posix() for f in sorted(filenames))
    return found


def _allow_reason(rel: str) -> str | None:
    if rel in ALLOWLIST:
        return ALLOWLIST[rel]
    for prefix, reason in ALLOWLIST.items():
        if prefix.endswith("/") and rel.startswith(prefix):
            return reason
    return None


def _in_scope(rel: str) -> bool:
    if not any(rel.startswith(p) for p in SCAN_PREFIXES):
        return False
    return any(rel.endswith(s) for s in SCAN_SUFFIXES)


def _scan_file(repo: Path, rel: str) -> list[str]:
    reason = _allow_reason(rel)
    if reason is not None:
        return []
    text = (repo / rel).read_text(encoding="utf-8", errors="replace")
    hits: list[str] = []
    seen: set[tuple[str, int]] = set()
    for concept in CONCEPTS:
        for idx, line in enumerate(text.splitlines(), 1):
            if not any(pattern.search(line) for pattern in concept.patterns):
                continue
            key = (concept.concept_id, idx)
            if key in seen:
                continue
            seen.add(key)
            hits.append(
                f"ERROR: {rel}:{idx}: concept {concept.concept_id} "
                f"belongs in {concept.belongs_in}, not LastDB\n"
                f"    {line.strip()}"
            )
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=None, help="Repository root")
    args = parser.parse_args()
    repo = _repo_root(args.root)
    tracked = _tracked_files(repo)
    errors: list[str] = []
    scanned = 0
    for rel in tracked:
        if not _in_scope(rel):
            continue
        if not (repo / rel).is_file():
            continue
        scanned += 1
        errors.extend(_scan_file(repo, rel))
    if errors:
        for msg in errors:
            print(msg, file=sys.stderr)
        print(
            f"lint-concept-boundary: FAIL — {len(errors)} hit(s) in {scanned} file(s). "
            "App presets and People schemas belong in EdgeVector/people; "
            "LastDB may only carry generic share configuration.",
            file=sys.stderr,
        )
        return 1
    print(
        f"lint-concept-boundary: ok — {scanned} file(s), "
        f"{len(CONCEPTS)} concept group(s), {len(ALLOWLIST)} allowlist entr(y/ies)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
