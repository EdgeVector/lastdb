#!/usr/bin/env python3
"""Score a LastDB /api/status?recent=1 envelope for the partition-guard CoW bar.

Bars (card lastdb-partition-guard-cow-ring-replay-20260909):
  - product reads with no all_group_walks: partition_read_rejections == 0
  - those same reads: cold_shard_loads <= 16
  - Card / BoardCards list-shaped reads: cold_shard_loads <= 16

Startup/admin samples with all_group_walks > 0 are excluded from the 16-group
bar. Missing rejection/walk fields mean zero (serde omits zeros).
"""

from __future__ import annotations

import json
import sys
from typing import Any

MAX_PRODUCT_GROUP_LOADS = 16
STATUS_KIND = {"status"}
WALK_EXEMPT_KIND = {"status"}


def _as_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _recent(envelope: dict[str, Any]) -> list[dict[str, Any]]:
    status = envelope.get("status")
    if not isinstance(status, dict):
        status = envelope
    ops = status.get("request_ops")
    if not isinstance(ops, dict):
        return []
    recent = ops.get("recent")
    if not isinstance(recent, list):
        return []
    return [row for row in recent if isinstance(row, dict)]


def _is_product_read(row: dict[str, Any]) -> bool:
    kind = str(row.get("kind") or "").strip().lower()
    if kind in STATUS_KIND or kind in WALK_EXEMPT_KIND:
        return False
    if _as_int(row.get("all_group_walks")) > 0:
        return False
    path = str(row.get("path") or "")
    if kind in {"query", "get", "list", "get-keys"}:
        return True
    if "/api/query" in path or "/api/list" in path or "/api/get" in path:
        return True
    return False


def _is_card_list(row: dict[str, Any]) -> bool:
    schema = str(row.get("schema") or "")
    path = str(row.get("path") or "")
    kind = str(row.get("kind") or "").strip().lower()
    lowered = schema.lower()
    if "card" in lowered or "boardcards" in lowered:
        return kind in {"query", "list", "get-keys"} or "/api/list" in path or "/api/query" in path
    if "/api/list" in path and kind in {"query", "list", "get-keys", ""}:
        return True
    return False


def score(envelope: dict[str, Any]) -> dict[str, Any]:
    recent = _recent(envelope)
    product = [row for row in recent if _is_product_read(row)]
    card_list = [row for row in recent if _is_card_list(row) and _as_int(row.get("all_group_walks")) == 0]
    rejections = []
    for row in product:
        n = _as_int(row.get("partition_read_rejections"))
        if n:
            rejections.append(
                {
                    "client": row.get("client") or "unknown",
                    "kind": row.get("kind") or "",
                    "schema": row.get("schema") or "",
                    "path": row.get("path") or "",
                    "partition_read_rejections": n,
                }
            )
    product_loads = [_as_int(row.get("cold_shard_loads")) for row in product]
    card_loads = [_as_int(row.get("cold_shard_loads")) for row in card_list]
    rejected_sum = sum(item["partition_read_rejections"] for item in rejections)
    max_product_loads = max(product_loads) if product_loads else 0
    max_card_loads = max(card_loads) if card_loads else 0
    reasons = []
    if rejected_sum != 0:
        reasons.append(f"product partition_read_rejections={rejected_sum} (want 0)")
    if product and max_product_loads > MAX_PRODUCT_GROUP_LOADS:
        reasons.append(
            f"product cold_shard_loads max={max_product_loads} (want <= {MAX_PRODUCT_GROUP_LOADS})"
        )
    if card_list and max_card_loads > MAX_PRODUCT_GROUP_LOADS:
        reasons.append(
            f"card-list cold_shard_loads max={max_card_loads} (want <= {MAX_PRODUCT_GROUP_LOADS})"
        )
    ok = not reasons
    return {
        "ok": ok,
        "recent": len(recent),
        "product_reads": len(product),
        "card_list_reads": len(card_list),
        "rejected_sum": rejected_sum,
        "max_product_cold_shard_loads": max_product_loads,
        "max_card_list_cold_shard_loads": max_card_loads,
        "max_product_group_loads": MAX_PRODUCT_GROUP_LOADS,
        "rejections": rejections,
        "reasons": reasons,
    }


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: partition-guard-ring-replay-score.py <status-recent.json>", file=sys.stderr)
        return 64
    path = argv[1]
    with open(path, encoding="utf-8") as handle:
        envelope = json.load(handle)
    if not isinstance(envelope, dict):
        print("RED score: envelope is not an object", file=sys.stderr)
        return 1
    result = score(envelope)
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
