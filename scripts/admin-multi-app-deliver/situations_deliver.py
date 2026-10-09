#!/usr/bin/env python3
"""Deliver slim fsituations Situation + Notice rows to the admin consumer.

The situations CLI does not yet ship `deliver-status`. This owner-side helper
stages a multi-leg snapshot (active postures + recent notices) using the same
kanban-consumer public keys as the other admin tabs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Reuse the kanban hourly Mini client / recipient helpers.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "admin-kanban-hourly-deliver"))
import deliver as kanban_deliver  # noqa: E402


SITUATION_FIELDS = ["slug", "title", "summary", "status", "severity", "updated_at"]
NOTICE_FIELDS = ["slug", "kind", "title", "summary", "at"]
DEFAULT_MAX = 5


def load_recipient() -> dict[str, str]:
    json_path = Path(
        os.environ.get("LASTDB_ADMIN_DELIVER_RECIPIENT_JSON_PATH")
        or Path.home() / ".lastdb" / "admin-deliver-recipient.json"
    )
    if json_path.is_file():
        return kanban_deliver.normalize_recipient(json.loads(json_path.read_text()))
    # Fall back to env/AWS via kanban helper with empty config body.
    return kanban_deliver.load_recipient(
        {
            "recipient": {
                "secret_id": os.environ.get(
                    "LASTDB_ADMIN_DELIVER_SECRET_ID", "ExememKanbanConsumer-prod"
                ),
                "region": os.environ.get("AWS_REGION", "us-east-1"),
            }
        }
    )


def find_schema(client: kanban_deliver.MiniClient, descriptive: str) -> str:
    payload = kanban_deliver._extract_payload(client.request("GET", "/api/schemas"))
    rows = payload.get("schemas") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise kanban_deliver.DeliverError("schema list missing schemas[]")
    matches = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("owner_app_id") == "fsituations"
        and row.get("descriptive_name") == descriptive
        and kanban_deliver._schema_row_name(row)
    ]
    data = [row for row in matches if row.get("has_data") is True]
    pick = (data or matches)
    if not pick:
        raise kanban_deliver.DeliverError(f"no fsituations/{descriptive} schema found")
    return str(kanban_deliver._schema_row_name(pick[0]))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--approve", action="store_true", help="approve+send after stage")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--max-records", type=int, default=DEFAULT_MAX)
    args = p.parse_args(argv)

    sock = (
        os.environ.get("LASTDB_SOCKET_PATH")
        or str(Path.home() / ".lastdb" / "data" / "folddb.sock")
    )
    recipient = load_recipient()
    body: dict = {
        "recipient_pubkey": recipient["recipient_pubkey"],
        "recipient_display_name": recipient.get(
            "recipient_display_name", "admin-situations-consumer"
        ),
        "messaging_public_key": recipient["messaging_public_key"],
        "messaging_pseudonym": recipient["messaging_pseudonym"],
        "mode": "snapshot",
        "max_records": args.max_records,
    }

    if args.dry_run:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "socket": sock,
                    "max_records": args.max_records,
                    "fields": {"situation": SITUATION_FIELDS, "notice": NOTICE_FIELDS},
                },
                indent=2,
            )
        )
        return 0

    client = kanban_deliver.MiniClient(socket_path=sock)
    sit = find_schema(client, "Situation")
    notice = find_schema(client, "Notice")
    body["legs"] = [
        {"schema_name": sit, "fields": list(SITUATION_FIELDS)},
        {"schema_name": notice, "fields": list(NOTICE_FIELDS)},
    ]
    staged = kanban_deliver.unwrap(client.request("POST", "/api/sharing/deliver", body))
    delivery_id = kanban_deliver.extract_delivery_id(staged)
    print(f"STAGED situations delivery_id={delivery_id}")
    if not args.approve:
        print("left staged (pass --approve to send)")
        return 0
    approved = kanban_deliver.unwrap(
        client.request("POST", f"/api/sharing/deliveries/{delivery_id}/approve", {})
    )
    shared = approved.get("shared")
    print(f"APPROVED delivery_id={delivery_id} shared={shared}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except kanban_deliver.DeliverError as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2) from e
