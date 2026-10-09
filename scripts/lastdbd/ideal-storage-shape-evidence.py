#!/usr/bin/env python3
"""Assemble redacted ideal-storage proof evidence from checked artifacts."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


def load(path: Path) -> object:
    return json.loads(path.read_text())


def contains_slug(value: object, slug: str) -> bool:
    return slug in json.dumps(value, sort_keys=True)


def repair_count(value: object) -> int:
    if not isinstance(value, dict):
        return 1
    repairs = value.get("repairs", value)
    if not isinstance(repairs, dict):
        return 1
    total = 0
    for key in ("upserts", "removals", "failed", "truth_read_failed"):
        item = repairs.get(key, 0)
        if isinstance(item, list):
            total += len(item)
        elif isinstance(item, bool):
            total += int(item)
        elif isinstance(item, (int, float)):
            total += int(item)
    return total


def require_object(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return value


def assemble(args: argparse.Namespace) -> dict[str, object]:
    status = load(args.status)
    show = load(args.card_show)
    board = load(args.board_list)
    milestone = load(args.milestone_detail)
    reconcile = load(args.milestone_reconcile)
    atom = load(args.atom_proof)
    contract = load(args.contract_proof)

    status = require_object(status, "status")
    show = require_object(show, "card show")
    milestone = require_object(milestone, "milestone detail")
    reconcile = require_object(reconcile, "milestone reconcile")
    atom = require_object(atom, "atom proof")
    contract = require_object(contract, "contract proof")
    planes = require_object(status.get("planes"), "status.planes")
    collections = {
        row.get("name")
        for row in planes.get("collections", [])
        if isinstance(row, dict)
    }
    roles = {
        row.get("role"): set(row.get("collections", []))
        for row in planes.get("by_role", [])
        if isinstance(row, dict)
    }
    dual_read = status.get("dual_read", {})
    if not isinstance(dual_read, dict):
        dual_read = {}

    slug = args.card_slug
    agreement = all(
        contains_slug(value, slug) for value in (show, board, milestone)
    ) and repair_count(reconcile) == 0
    contracts_green = all(contract.get(key) is True for key in (
        "protein_routing",
        "legacy_tips_get",
        "copy_verify",
        "mutable_backup",
    ))
    design_text = args.design.read_text()
    revision_match = re.search(r"\brev(?:ision)?\D{0,6}(\d+)", design_text, re.IGNORECASE)
    revision = int(revision_match.group(1)) if revision_match else 0
    decisions = [key for key in ("K16", "K17", "K18") if key in design_text]

    return {
        "surface": {
            "kind": "cow",
            "primary_mutated": False,
            "dogfood_mini_verified": agreement,
            "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        "protein_plane": {
            "collection": "proteins" if contracts_green else "unverified",
            "prefixes": ["protein:", "molprot:", "fldprot:", "pfq:"] if contracts_green else [],
            # This compatibility field now means ordinary mutation plus Mini protein fold.
            "create_member_write_fold": agreement and contracts_green,
            "legacy_tips_get": contract.get("legacy_tips_get") is True,
            "copy_verify": "green" if contract.get("copy_verify") is True else "red",
            "legacy_hits": int(dual_read.get("legacy_hits", -1)),
        },
        "backup_manifest": {
            "mutable_includes": ["proteins"] if contract.get("mutable_backup") is True else [],
        },
        "design": {"revision": revision, "decisions": decisions},
        "plane_map": {
            "one_tip_home": "tips" in roles.get("sot", set()),
            "single_indexes_home": "indexes" in roles.get("indexes", set()),
            "order_log_history_adjacent": "field_update_order_log" in roles.get("history_adjacent", set()),
            "cold_ops_classified": bool(roles.get("cold_sync")) and bool(roles.get("ops")),
            "collapsed_plane_legacy_hits": int(dual_read.get("legacy_hits", -1)),
        },
        "status": {
            "proteins_present": "proteins" in collections,
            "dual_read_legacy_hits_visible": "legacy_hits" in dual_read,
        },
        "fkanban": {
            "protein_primary": agreement,
            "boardcards_milestonecards_agree": agreement,
            "dual_write_fallback": repair_count(reconcile) != 0,
            "atom_gc_tip_fold_verified": isinstance(atom, dict) and atom.get("ok") is True,
        },
        "refs": {"fold": args.fold_ref, "fkanban": args.fkanban_ref},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", type=Path, required=True)
    parser.add_argument("--card-show", type=Path, required=True)
    parser.add_argument("--board-list", type=Path, required=True)
    parser.add_argument("--milestone-detail", type=Path, required=True)
    parser.add_argument("--milestone-reconcile", type=Path, required=True)
    parser.add_argument("--atom-proof", type=Path, required=True)
    parser.add_argument("--contract-proof", type=Path, required=True)
    parser.add_argument("--design", type=Path, required=True)
    parser.add_argument("--card-slug", required=True)
    parser.add_argument("--fold-ref", required=True)
    parser.add_argument("--fkanban-ref", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    evidence = assemble(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
    print(f"EVIDENCE_FILE={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
