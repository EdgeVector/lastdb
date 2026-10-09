#!/usr/bin/env python3
"""Fallback slim routines fleet deliver when the CLI slice is oversized.

Stages RoutineFleetSnapshot (no rows_json) + a SampleN of RoutineStatus rows.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "admin-kanban-hourly-deliver"))
import deliver as kanban_deliver  # noqa: E402


def load_recipient() -> dict[str, str]:
    json_path = Path.home() / ".lastdb" / "admin-deliver-recipient.json"
    if json_path.is_file():
        return kanban_deliver.normalize_recipient(json.loads(json_path.read_text()))
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


def find_schema(client: kanban_deliver.MiniClient, descriptive: str, app_id: str) -> str:
    payload = kanban_deliver._extract_payload(client.request("GET", "/api/schemas"))
    rows = payload.get("schemas") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        raise kanban_deliver.DeliverError("schema list missing schemas[]")
    matches = [
        row
        for row in rows
        if isinstance(row, dict)
        and row.get("owner_app_id") == app_id
        and row.get("descriptive_name") == descriptive
        and kanban_deliver._schema_row_name(row)
    ]
    data = [row for row in matches if row.get("has_data") is True]
    pick = data or matches
    if not pick:
        raise kanban_deliver.DeliverError(f"no {app_id}/{descriptive} schema found")
    return str(kanban_deliver._schema_row_name(pick[0]))


def main() -> int:
    sock = (
        os.environ.get("LASTDB_SOCKET_PATH")
        or str(Path.home() / ".lastdb" / "data" / "folddb.sock")
    )
    recipient = load_recipient()
    client = kanban_deliver.MiniClient(socket_path=sock)
    snap = find_schema(client, "RoutineFleetSnapshot", "routines")
    status = find_schema(client, "RoutineStatus", "routines")
    body = {
        "recipient_pubkey": recipient["recipient_pubkey"],
        "recipient_display_name": "admin-routines-consumer",
        "messaging_public_key": recipient["messaging_public_key"],
        "messaging_pseudonym": recipient["messaging_pseudonym"],
        "mode": "snapshot",
        "max_records": 8,
        "legs": [
            {
                "schema_name": snap,
                "fields": [
                    "slug",
                    "captured_at",
                    "row_count",
                    "run_summary_count",
                    "situations_ok",
                    "situations_error",
                ],
                "hash_keys": ["fleet-latest"],
            },
            {
                "schema_name": status,
                "fields": [
                    "id",
                    "status",
                    "harness",
                    "model",
                    "last_outcome",
                    "last_exit",
                    "running",
                    "next_fire",
                    "last_run",
                    "updated_at",
                ],
            },
        ],
    }
    staged = kanban_deliver.unwrap(client.request("POST", "/api/sharing/deliver", body))
    delivery_id = kanban_deliver.extract_delivery_id(staged)
    print(f"STAGED slim-routines delivery_id={delivery_id}")
    approved = kanban_deliver.unwrap(
        client.request("POST", f"/api/sharing/deliveries/{delivery_id}/approve", {})
    )
    print(f"APPROVED delivery_id={delivery_id} shared={approved.get('shared')}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except kanban_deliver.DeliverError as e:
        print(f"error: {e}", file=sys.stderr)
        raise SystemExit(2) from e
