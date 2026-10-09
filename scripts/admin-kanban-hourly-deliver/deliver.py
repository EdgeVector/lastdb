#!/usr/bin/env python3
"""Hourly admin Kanban deliver / 60s object snapshot publisher.

Snapshot and mailbox legs list a board column through BoardCards HashRange
(hash=board, prefix=`{column}#`). That is O(log M) under one partition.
They do not query Card with field-equality `where` / `in(column)` (a product
scan). Card is point-get only (`hash_keys`) for mailbox chunks after BoardCards
already returned the slugs.

Config is a durable JSON file (default:
  $LASTDB_HOME/admin-kanban-hourly-deliver.json or ~/.lastdb/...).
Schema hashes come from live kanban `schemaHashes.board_cards` /
`schemaHashes.card`. Recipient keys never live in git.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import http.client
from urllib.parse import quote
from dataclasses import dataclass
from pathlib import Path
from typing import Any


# BoardCards payload fields (fkanban src/schemas.ts). Snapshot legs may only
# project this set. `milestone` is omitted from the default snapshot projection
# because a missing atom silently drops the whole row.
BOARD_CARDS_FIELDS = (
    "board",
    "sk",
    "slug",
    "title",
    "column",
    "position",
    "assignee",
    "tags",
    "deps",
    "surfaces",
    "created_at",
    "created_by",
    "updated_at",
    "db",
    "repo",
    "base",
    "kind",
    "block_status",
    "block_reason",
    "north_star",
    "milestone",
    "pr_url",
    "branch",
    "layout",
)
BOARD_CARDS_FIELD_SET = set(BOARD_CARDS_FIELDS)

# Slim snapshot / factory projection. `slug` leads so a missing trailing atom
# does not drop the row. No `body`. No `milestone` (gating).
DEFAULT_FIELDS = [
    "slug",
    "title",
    "column",
    "position",
    "tags",
    "updated_at",
    "board",
    "assignee",
    "repo",
    "kind",
    "north_star",
    "pr_url",
]

# Extra fields for the admin Kanban Factory theater (assignee → Active Hands).
# Keep bodies out — sealed Exemem messages are ~64KB capped.
FACTORY_FIELDS = DEFAULT_FIELDS + [
    "block_status",
    "branch",
]

DEFAULT_SCHEMA_NAME = "fkanban/Card"
DEFAULT_BOARD_CARDS_SCHEMA_NAME = "fkanban/BoardCards"
DEFAULT_BOARD = "default"
DEFAULT_SINCE_FIELD = "updated_at"
DEFAULT_ORDER_BY = "updated_at"
# Full-board default (Mini gzip-before-JWE, 2026-07-17). Exemem still caps
# sealed blobs at ~64KB; gzip of slim Card fields holds a whole board.
DEFAULT_MAX_RECORDS = 2000
DEFAULT_MAX_RECORDS_PER_COLUMN = 2000
DEFAULT_COLUMNS = ["backlog", "todo", "doing", "done"]
# "all" / empty / "none" → no time window (deliver entire columns).
DEFAULT_SINCE = "all"
# Empirically: ~60 molecule-signed cards seal under Exemem's 87382 base64
# cap after gzip-before-JWE (2026-07-17 full-board probe). Stay under that
# by chunking stage+approve into multiple mailbox messages; admin merges.
DEFAULT_SEAL_SAFE_RECORDS = 55

# Factory profile: full board + theater fields. Same high caps as default after
# Mini compress-before-encrypt; no last-day filter.
FACTORY_SINCE = "all"
FACTORY_MAX_BY_COLUMN = {
    "doing": 2000,
    "todo": 2000,
    "backlog": 2000,
    "done": 2000,
}


class DeliverError(RuntimeError):
    pass


class _UnixHttpResponse(http.client.HTTPResponse):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock)


@dataclass
class MiniClient:
    socket_path: str
    user_hash: str | None = None
    timeout_s: float = 120.0

    def __post_init__(self) -> None:
        self.socket_path = str(Path(self.socket_path).expanduser())

    def _auto_identity(self) -> str | None:
        if self.user_hash:
            return self.user_hash
        body = self.request("GET", "/api/system/auto-identity", attach_identity=False)
        if isinstance(body, dict) and body.get("user_hash"):
            self.user_hash = str(body["user_hash"])
            return self.user_hash
        # unwrap common envelope shapes
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(data, dict) and data.get("user_hash"):
            self.user_hash = str(data["user_hash"])
            return self.user_hash
        return None

    def request(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        *,
        attach_identity: bool = True,
    ) -> Any:
        payload = b"" if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers = [
            f"{method} {path} HTTP/1.1",
            "Host: lastdb",
            "X-LastDB-Client: admin-kanban-hourly-deliver",
            "Connection: close",
        ]
        if body is not None:
            headers.append("Content-Type: application/json")
            headers.append(f"Content-Length: {len(payload)}")
        if attach_identity:
            uh = self._auto_identity()
            if uh:
                headers.append(f"X-User-Hash: {uh}")
        raw = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii") + payload
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout_s)
            sock.connect(self.socket_path)
            sock.sendall(raw)
            resp = _UnixHttpResponse(sock)
            resp.begin()
            data = resp.read()
            text = data.decode("utf-8", errors="replace")
            status = resp.status
            try:
                parsed = json.loads(text) if text else None
            except json.JSONDecodeError:
                parsed = text
            if status >= 400:
                raise DeliverError(f"{method} {path} HTTP {status}: {text[:1200]}")
            return parsed
        except OSError as error:
            raise DeliverError(f"socket {self.socket_path}: {error}") from error
        finally:
            sock.close()


def default_config_path() -> Path:
    home = os.environ.get("LASTDB_HOME") or os.environ.get("FOLDDB_HOME")
    base = Path(home).expanduser() if home else Path.home() / ".lastdb"
    return base / "admin-kanban-hourly-deliver.json"


def default_kanban_config_path() -> Path:
    override = os.environ.get("KANBAN_CONFIG") or os.environ.get("FKANBAN_CONFIG")
    if override:
        return Path(override).expanduser()
    primary = Path.home() / ".kanban" / "config.json"
    if primary.is_file():
        return primary
    return Path.home() / ".fkanban" / "config.json"


_SCHEMA_HASH_ENV = {
    "card": "LASTDB_ADMIN_DELIVER_CARD_SCHEMA_HASH",
    "board_cards": "LASTDB_ADMIN_DELIVER_BOARD_CARDS_SCHEMA_HASH",
}


def configured_schema_hash(kind: str) -> str | None:
    env_key = _SCHEMA_HASH_ENV.get(kind)
    if env_key:
        override = os.environ.get(env_key)
        if override:
            return override
    cfg_path = default_kanban_config_path()
    if not cfg_path.is_file():
        return None
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    schema_hashes = cfg.get("schemaHashes") if isinstance(cfg, dict) else None
    value = schema_hashes.get(kind) if isinstance(schema_hashes, dict) else None
    return value if isinstance(value, str) and value else None


def configured_card_schema_hash() -> str | None:
    return configured_schema_hash("card")


def configured_board_cards_schema_hash() -> str | None:
    return configured_schema_hash("board_cards")


def membership_schema_name(cfg: dict[str, Any] | None = None) -> str:
    """Live BoardCards hash, else the BoardCards alias. Never a stale Card hash."""
    from_cfg = None
    if isinstance(cfg, dict):
        raw = cfg.get("board_cards_schema") or cfg.get("membership_schema")
        if isinstance(raw, str) and raw.strip() and raw.strip() not in (
            DEFAULT_SCHEMA_NAME,
            "fkanban/Card",
        ):
            from_cfg = raw.strip()
    return (
        configured_board_cards_schema_hash()
        or from_cfg
        or DEFAULT_BOARD_CARDS_SCHEMA_NAME
    )


def card_schema_name(cfg: dict[str, Any] | None = None) -> str:
    from_cfg = None
    if isinstance(cfg, dict):
        raw = cfg.get("card_schema")
        if isinstance(raw, str) and raw.strip():
            from_cfg = raw.strip()
    return configured_card_schema_hash() or from_cfg or DEFAULT_SCHEMA_NAME


def snapshot_fields(fields: list[str]) -> list[str]:
    """Intersect requested fields with BoardCards. Keep slug first."""
    seen: set[str] = set()
    out: list[str] = []
    for name in ["slug", *fields]:
        if name not in BOARD_CARDS_FIELD_SET or name in seen:
            continue
        if name == "milestone":
            continue
        seen.add(name)
        out.append(name)
    if "slug" not in seen:
        out.insert(0, "slug")
    return out


def column_range_prefix(column: str) -> str:
    return f"{column}#"


def load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise DeliverError(f"config not found: {path}")
    raw = path.read_text(encoding="utf-8")
    data = json.loads(raw)
    if not isinstance(data, dict):
        raise DeliverError("config root must be a JSON object")
    return data


def load_recipient(cfg: dict[str, Any]) -> dict[str, str]:
    """Return {recipient_pubkey, messaging_public_key, messaging_pseudonym, display_name?}."""
    env_raw = os.environ.get("LASTDB_ADMIN_DELIVER_RECIPIENT_JSON")
    if env_raw:
        data = json.loads(env_raw)
        return normalize_recipient(data)

    rec = cfg.get("recipient") or {}
    if not isinstance(rec, dict):
        raise DeliverError("config.recipient must be an object")

    if rec.get("recipient_pubkey") and rec.get("messaging_public_key") and rec.get("messaging_pseudonym"):
        return normalize_recipient(rec)

    secret_id = rec.get("secret_id") or os.environ.get("LASTDB_ADMIN_DELIVER_SECRET_ID")
    if secret_id:
        region = rec.get("region") or os.environ.get("AWS_REGION") or "us-east-1"
        out = subprocess.check_output(
            [
                "aws",
                "secretsmanager",
                "get-secret-value",
                "--secret-id",
                str(secret_id),
                "--region",
                str(region),
                "--query",
                "SecretString",
                "--output",
                "text",
            ],
            text=True,
        )
        data = json.loads(out)
        return normalize_recipient(data)

    raise DeliverError(
        "recipient not configured: set recipient fields in config, "
        "LASTDB_ADMIN_DELIVER_RECIPIENT_JSON, or recipient.secret_id / "
        "LASTDB_ADMIN_DELIVER_SECRET_ID (AWS Secrets Manager)"
    )


def normalize_recipient(data: dict[str, Any]) -> dict[str, str]:
    # Accept both enroll-kanban-consumer bundle shape and stage body field names.
    pubkey = (
        data.get("recipient_pubkey")
        or data.get("ed25519_public_key")
        or data.get("identity_public_key")
    )
    msg_pk = data.get("messaging_public_key") or data.get("x25519_public_key")
    pseudo = data.get("messaging_pseudonym") or data.get("pseudonym")
    if not pubkey or not msg_pk or not pseudo:
        raise DeliverError(
            "recipient JSON must include recipient_pubkey (or ed25519_public_key), "
            "messaging_public_key (or x25519_public_key), messaging_pseudonym"
        )
    out = {
        "recipient_pubkey": str(pubkey),
        "messaging_public_key": str(msg_pk),
        "messaging_pseudonym": str(pseudo),
    }
    display = data.get("recipient_display_name") or data.get("display_name") or "admin-kanban-consumer"
    out["recipient_display_name"] = str(display)
    return out


def parse_since_duration_secs(raw: str) -> int:
    """Match Mini's since parser: 24h / 30m / 3600 / 3600s."""
    trimmed = (raw or "").strip()
    if not trimmed:
        raise DeliverError("since must not be empty")
    split_at = 0
    while split_at < len(trimmed) and trimmed[split_at].isdigit():
        split_at += 1
    if split_at == 0:
        raise DeliverError(f"invalid since duration '{raw}'")
    amount = int(trimmed[:split_at])
    unit = trimmed[split_at:].strip().lower()
    mult = {
        "": 1,
        "s": 1,
        "sec": 1,
        "secs": 1,
        "second": 1,
        "seconds": 1,
        "m": 60,
        "min": 60,
        "mins": 60,
        "minute": 60,
        "minutes": 60,
        "h": 3600,
        "hr": 3600,
        "hrs": 3600,
        "hour": 3600,
        "hours": 3600,
        "d": 86400,
        "day": 86400,
        "days": 86400,
    }.get(unit)
    if mult is None:
        raise DeliverError(f"invalid since duration '{raw}'")
    return amount * mult


def since_threshold_unix(since: str) -> int:
    return int(time.time()) - parse_since_duration_secs(since)


def resolve_per_column_limits(
    columns: list[str],
    cfg: dict[str, Any],
    *,
    default_per: int,
) -> dict[str, int]:
    """Return {column: predicate_limit}.

    Accepts:
      max_records_per_column: 4
      max_records_per_column: { "doing": 16, "todo": 12, ... }
      max_records_by_column:  { same map form }
    """
    raw = cfg.get("max_records_by_column")
    if raw is None:
        raw = cfg.get("max_records_per_column")
    if raw is None:
        raw = cfg.get("max_per_column")
    if isinstance(raw, dict):
        out: dict[str, int] = {}
        for col in columns:
            if col in raw:
                out[col] = int(raw[col])
            else:
                out[col] = int(default_per)
        return out
    if raw is None:
        per = default_per
    else:
        per = int(raw)
    return {col: per for col in columns}


def since_is_all(since: str) -> bool:
    s = (since or "").strip().lower()
    return s in ("", "all", "none", "0", "*")


def build_column_legs(
    *,
    schema_name: str,
    fields: list[str],
    columns: list[str],
    since: str,
    since_field: str,
    order_by: str,
    order: str,
    max_per_column: int | dict[str, int],
    board: str = DEFAULT_BOARD,
) -> list[dict[str, Any]]:
    """One BoardCards HashRangePrefix leg per column (range under one hash).

    `since` / `order_by` are retained on the stage body for logs. They are not
    applied as Card field predicates — that path is a product scan.
    """
    del since, since_field, order_by, order, max_per_column
    projected = snapshot_fields(fields)
    legs: list[dict[str, Any]] = []
    for col in columns:
        legs.append(
            {
                "schema_name": schema_name,
                "fields": list(projected),
                "filter": {
                    "HashRangePrefix": {
                        "hash": board,
                        "prefix": column_range_prefix(col),
                    }
                },
            }
        )
    return legs


def apply_profile(cfg: dict[str, Any]) -> dict[str, Any]:
    """Merge a named profile into config (shallow). Config keys win over profile."""
    profile = str(cfg.get("profile") or "").strip().lower()
    if not profile or profile in ("default", "hourly", "none"):
        return cfg
    if profile not in ("factory", "factory-1m", "kanban-factory"):
        raise DeliverError(
            f"unknown profile {profile!r} (supported: default, factory)"
        )
    base = {
        "fields": list(FACTORY_FIELDS),
        "since": FACTORY_SINCE,
        "since_field": DEFAULT_SINCE_FIELD,
        "columns_include": list(DEFAULT_COLUMNS),
        "order_by": DEFAULT_ORDER_BY,
        "order": "desc",
        "max_records_by_column": dict(FACTORY_MAX_BY_COLUMN),
        "max_records": sum(FACTORY_MAX_BY_COLUMN.values()),
        "profile": "factory",
    }
    # Config overrides profile (except profile name itself).
    merged = {**base, **{k: v for k, v in cfg.items() if k != "profile" or v}}
    merged["profile"] = "factory"
    # If caller set scalar max_records_per_column and not the map, keep scalar.
    return merged


def build_stage_body(cfg: dict[str, Any], recipient: dict[str, str]) -> dict[str, Any]:
    cfg = apply_profile(cfg)
    fields = cfg.get("fields") or DEFAULT_FIELDS
    if not isinstance(fields, list) or not fields:
        raise DeliverError("config.fields must be a non-empty list")
    columns = cfg.get("columns_include") or list(DEFAULT_COLUMNS)
    if not isinstance(columns, list) or not columns:
        raise DeliverError("config.columns_include must be a non-empty list")
    columns = [str(c) for c in columns]
    since = str(cfg.get("since") or DEFAULT_SINCE)
    since_field = str(cfg.get("since_field") or DEFAULT_SINCE_FIELD)
    order_by = str(cfg.get("order_by") or DEFAULT_ORDER_BY)
    order = str(cfg.get("order") or "desc")
    board = str(cfg.get("board") or DEFAULT_BOARD).strip() or DEFAULT_BOARD
    schema_name = membership_schema_name(cfg)
    card_schema = card_schema_name(cfg)
    projected = snapshot_fields(list(fields) if isinstance(fields, list) else DEFAULT_FIELDS)
    limits = resolve_per_column_limits(
        columns, cfg, default_per=DEFAULT_MAX_RECORDS_PER_COLUMN
    )
    # Total budget (informational + single-leg fallback). Multi-leg uses per-column.
    max_records = int(cfg.get("max_records") or max(DEFAULT_MAX_RECORDS, sum(limits.values())))

    body: dict[str, Any] = {
        "recipient_pubkey": recipient["recipient_pubkey"],
        "recipient_display_name": recipient.get("recipient_display_name", "admin-kanban-consumer"),
        "messaging_public_key": recipient["messaging_public_key"],
        "messaging_pseudonym": recipient["messaging_pseudonym"],
        "mode": cfg.get("mode") or "snapshot",
        # Retained for logging / dry-run readability; Mini stages from legs[].
        "schema_name": schema_name,
        "card_schema": card_schema,
        "board": board,
        "fields": list(projected),
        "since": since,
        "since_field": since_field,
        "columns_include": columns,
        "order_by": order_by,
        "order": order,
        "max_records": max_records,
        "max_records_per_column": limits,
        "legs": build_column_legs(
            schema_name=schema_name,
            fields=list(projected),
            columns=columns,
            since=since,
            since_field=since_field,
            order_by=order_by,
            order=order,
            max_per_column=limits,
            board=board,
        ),
    }
    if cfg.get("profile"):
        body["profile"] = cfg["profile"]
    if cfg.get("where") is not None:
        # Explicit single-leg override: drop multi-leg fairness and use body-level
        # predicates the way older Mini stage shorthand expects.
        body.pop("legs", None)
        body["where"] = cfg["where"]
    return body


def _extract_payload(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    data = result.get("data")
    if isinstance(data, dict):
        return data
    return result


def _schema_row_name(row: dict[str, Any]) -> str | None:
    name = row.get("name") or row.get("identity_hash")
    return str(name) if name else None


def _descriptive_name_for_alias(schema_name: str) -> str | None:
    if schema_name in (DEFAULT_SCHEMA_NAME, "fkanban/Card"):
        return "Card"
    if schema_name in (DEFAULT_BOARD_CARDS_SCHEMA_NAME, "fkanban/BoardCards"):
        return "BoardCards"
    return None


def resolve_stage_schema_name(client: MiniClient, schema_name: str) -> str:
    """Resolve a configured schema alias to the active canonical node name.

    Direct `/api/schema/{name}` lookup is the common path. Current Mini fleets can
    have ambiguous historical `fkanban/Card` aliases, so for the default Card
    and BoardCards slices we fall back to the active schema list and pick the
    loaded `owner_app_id=fkanban` row.
    """
    try:
        encoded = quote(schema_name, safe="")
        payload = _extract_payload(client.request("GET", f"/api/schema/{encoded}"))
        schema = payload.get("schema")
        if isinstance(schema, dict):
            resolved = _schema_row_name(schema)
            if resolved:
                return resolved
    except DeliverError:
        if _descriptive_name_for_alias(schema_name) is None:
            raise

    descriptive = _descriptive_name_for_alias(schema_name)
    if descriptive is None:
        return schema_name

    payload = _extract_payload(client.request("GET", "/api/schemas"))
    rows = payload.get("schemas")
    if not isinstance(rows, list):
        raise DeliverError("schema list response missing schemas[]")

    matches = [
        row for row in rows
        if isinstance(row, dict)
        and row.get("owner_app_id") == "fkanban"
        and row.get("descriptive_name") == descriptive
        and _schema_row_name(row)
    ]
    if not matches:
        raise DeliverError(f"could not resolve active fkanban/{descriptive} schema")

    data_matches = [row for row in matches if row.get("has_data") is True]
    candidates = data_matches or matches
    configured_hash = (
        configured_board_cards_schema_hash()
        if descriptive == "BoardCards"
        else configured_card_schema_hash()
    )
    if configured_hash:
        configured_matches = [
            row for row in candidates
            if _schema_row_name(row) == configured_hash
        ]
        if len(configured_matches) == 1:
            return configured_hash

    if not candidates:
        raise DeliverError(f"could not resolve active fkanban/{descriptive} schema")

    def _sort_key(row: dict[str, Any]) -> tuple[int, str]:
        name = str(_schema_row_name(row) or "")
        has = 0 if row.get("has_data") is True else 1
        return (has, name)

    candidates = sorted(candidates, key=_sort_key)
    if len(candidates) > 1:
        names = ", ".join(str(_schema_row_name(row)) for row in candidates)
        print(
            f"WARN: ambiguous active fkanban/{descriptive} schemas ({names}); "
            f"using {_schema_row_name(candidates[0])}",
            file=sys.stderr,
        )
    return str(_schema_row_name(candidates[0]))


def unwrap(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise DeliverError(f"expected object response, got {type(result).__name__}")
    if isinstance(result.get("data"), dict):
        return result["data"]
    return result


def extract_delivery_id(staged: dict[str, Any]) -> str:
    for key in ("delivery_id", "id"):
        if staged.get(key):
            return str(staged[key])
    delivery = staged.get("delivery")
    if isinstance(delivery, dict):
        for key in ("delivery_id", "id"):
            if delivery.get(key):
                return str(delivery[key])
    pending = staged.get("pending")
    if isinstance(pending, dict) and pending.get("id"):
        return str(pending["id"])
    raise DeliverError(f"could not find delivery_id in stage response: {json.dumps(staged)[:500]}")


def _preview_record_count(staged: dict[str, Any]) -> int | None:
    delivery = staged.get("delivery") if isinstance(staged.get("delivery"), dict) else staged
    if not isinstance(delivery, dict):
        return None
    preview = delivery.get("preview")
    if isinstance(preview, dict) and preview.get("record_count") is not None:
        try:
            return int(preview["record_count"])
        except (TypeError, ValueError):
            pass
    records = delivery.get("records")
    if isinstance(records, list):
        return len(records)
    if delivery.get("record_count") is not None:
        try:
            return int(delivery["record_count"])
        except (TypeError, ValueError):
            pass
    return None


def query_column_slugs(
    client: MiniClient,
    *,
    schema_name: str,
    column: str,
    since: str,
    since_field: str,
    order_by: str,
    order: str,
    limit: int,
    board: str = DEFAULT_BOARD,
) -> list[str]:
    """List card slugs in a column via BoardCards HashRangePrefix."""
    del since, since_field, order_by, order
    slugs: list[str] = []
    offset = 0
    page = min(200, max(1, limit))
    while len(slugs) < limit:
        body = {
            "schema_name": schema_name,
            "fields": ["slug"],
            "filter": {
                "HashRangePrefix": {
                    "hash": board,
                    "prefix": column_range_prefix(column),
                }
            },
            "limit": page,
            "offset": offset,
        }
        raw = client.request("POST", "/api/query", body)
        payload = unwrap(raw) if isinstance(raw, dict) else {}
        results = payload.get("results") if isinstance(payload, dict) else None
        if not isinstance(results, list) or not results:
            break
        for row in results:
            if not isinstance(row, dict):
                continue
            fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
            slug = fields.get("slug") if isinstance(fields, dict) else None
            if slug is None:
                key = row.get("key")
                if isinstance(key, dict) and key.get("range"):
                    # BoardCards range is column#position#slug.
                    parts = str(key["range"]).split("#")
                    if len(parts) >= 3:
                        slug = parts[-1]
            if slug is not None:
                slugs.append(str(slug))
            if len(slugs) >= limit:
                break
        if not payload.get("has_more"):
            break
        offset += len(results)
        if len(results) < page:
            break
    return slugs[:limit]


def chunk_slugs(slugs: list[str], size: int) -> list[list[str]]:
    if size <= 0:
        raise DeliverError("seal_safe_records must be positive")
    return [slugs[i : i + size] for i in range(0, len(slugs), size)]


def build_slug_chunk_body(
    *,
    base: dict[str, Any],
    schema_name: str,
    fields: list[str],
    column: str,
    slugs: list[str],
    order_by: str,
    order: str,
) -> dict[str, Any]:
    """One seal-safe delivery for a single column page of slugs."""
    body = {
        k: v
        for k, v in base.items()
        if k
        not in {
            "legs",
            "where",
            "columns_include",
            "since",
            "since_field",
            "max_records",
            "max_records_per_column",
        }
    }
    body["schema_name"] = schema_name
    body["fields"] = list(fields)
    # Point-get Card by slug. Do not re-scan Card with in(column)/in(slug).
    del order_by, order
    body["legs"] = [
        {
            "schema_name": schema_name,
            "fields": list(fields),
            "hash_keys": list(slugs),
        }
    ]
    body["max_records"] = len(slugs)
    body["columns_include"] = [column]
    return body


def stage_and_approve(
    client: MiniClient,
    body: dict[str, Any],
    *,
    auto_approve: bool,
    cleanup_staged: bool,
    label: str,
) -> int:
    """Stage (+ optional approve) one delivery. Returns shared record count."""
    staged_raw = client.request("POST", "/api/sharing/deliver", body)
    staged = unwrap(staged_raw)
    delivery_id = extract_delivery_id(staged)
    record_count = _preview_record_count(staged)
    print(f"STAGED {label} delivery_id={delivery_id} records={record_count}")

    if not auto_approve:
        if cleanup_staged:
            rejected_raw = client.request(
                "POST", f"/api/sharing/deliveries/{delivery_id}/reject", {}
            )
            rejected = unwrap(rejected_raw)
            status = rejected.get("status") if isinstance(rejected, dict) else None
            print(f"REJECTED {label} delivery_id={delivery_id} status={status or 'ok'}")
            return int(record_count or 0)
        print(f"auto_approve=false — leaving staged {label} delivery_id={delivery_id}")
        return int(record_count or 0)

    try:
        approved_raw = client.request(
            "POST", f"/api/sharing/deliveries/{delivery_id}/approve", {}
        )
    except DeliverError as err:
        # Leave a clean pending queue on seal-cap failures.
        try:
            client.request("POST", f"/api/sharing/deliveries/{delivery_id}/reject", {})
        except DeliverError:
            pass
        raise DeliverError(f"{label} approve failed: {err}") from err
    approved = unwrap(approved_raw)
    shared = approved.get("shared")
    print(f"APPROVED {label} delivery_id={delivery_id} shared={shared}")
    if shared is not None:
        try:
            return int(shared)
        except (TypeError, ValueError):
            pass
    return int(record_count or 0)


def run(cfg_path: Path, *, dry_run: bool, force: bool, cleanup_staged: bool) -> int:
    cfg = load_json(cfg_path)
    if not cfg.get("enabled", True) and not force:
        print(f"disabled (enabled=false in {cfg_path}); pass --force to run anyway")
        return 0
    if cleanup_staged and cfg.get("auto_approve", True):
        raise DeliverError("--cleanup-staged requires auto_approve=false in config")

    sock = (
        cfg.get("socket")
        or os.environ.get("LASTDB_SOCKET_PATH")
        or str(Path.home() / ".lastdb" / "data" / "folddb.sock")
    )
    recipient = load_recipient(cfg)
    body = build_stage_body(cfg, recipient)
    seal_safe = int(cfg.get("seal_safe_records") or DEFAULT_SEAL_SAFE_RECORDS)

    client: MiniClient | None = None
    if not dry_run:
        client = MiniClient(socket_path=str(sock), user_hash=cfg.get("user_hash"))
        resolved_schema = resolve_stage_schema_name(client, str(body["schema_name"]))
        body["schema_name"] = resolved_schema
        for leg in body.get("legs") or []:
            if isinstance(leg, dict):
                leg["schema_name"] = resolved_schema
        body["card_schema"] = resolve_stage_schema_name(
            client, str(body.get("card_schema") or card_schema_name(cfg))
        )

    print(
        json.dumps(
            {
                "config": str(cfg_path),
                "socket": sock,
                "schema_name": body["schema_name"],
                "card_schema": body.get("card_schema"),
                "board": body.get("board"),
                "since": body["since"],
                "since_field": body["since_field"],
                "columns_include": body["columns_include"],
                "max_records": body["max_records"],
                "max_records_per_column": body.get("max_records_per_column"),
                "legs": len(body.get("legs") or []),
                "seal_safe_records": seal_safe,
                "auto_approve": bool(cfg.get("auto_approve", True)),
                "cleanup_staged": cleanup_staged,
                "dry_run": dry_run,
            },
            indent=2,
        )
    )

    if dry_run:
        # Never print recipient keys.
        safe = {k: v for k, v in body.items() if k not in {
            "recipient_pubkey", "messaging_public_key", "messaging_pseudonym"
        }}
        safe["recipient"] = "(redacted)"
        print("stage body (recipient redacted):")
        print(json.dumps(safe, indent=2))
        print(
            f"note: live run chunk-stages by column at seal_safe_records={seal_safe} "
            "so Exemem 64KB seal cap is not exceeded; admin merges mailbox chunks"
        )
        return 0

    assert client is not None
    auto_approve = bool(cfg.get("auto_approve", True))
    schema_name = str(body["schema_name"])
    card_schema = str(body.get("card_schema") or card_schema_name(cfg))
    board = str(body.get("board") or DEFAULT_BOARD)
    fields = list(body.get("fields") or DEFAULT_FIELDS)
    columns = [str(c) for c in (body.get("columns_include") or DEFAULT_COLUMNS)]
    since = str(body.get("since") or DEFAULT_SINCE)
    since_field = str(body.get("since_field") or DEFAULT_SINCE_FIELD)
    order_by = str(body.get("order_by") or DEFAULT_ORDER_BY)
    order = str(body.get("order") or "desc")
    limits = body.get("max_records_per_column") or {}
    if not isinstance(limits, dict):
        limits = {c: int(limits) for c in columns}

    total_shared = 0
    chunk_n = 0
    for col in columns:
        col_limit = int(limits.get(col, DEFAULT_MAX_RECORDS_PER_COLUMN))
        slugs = query_column_slugs(
            client,
            schema_name=schema_name,
            column=col,
            since=since,
            since_field=since_field,
            order_by=order_by,
            order=order,
            limit=col_limit,
            board=board,
        )
        print(f"column {col}: {len(slugs)} cards (limit {col_limit})")
        if not slugs:
            continue
        for page_i, page in enumerate(chunk_slugs(slugs, seal_safe)):
            chunk_n += 1
            chunk_body = build_slug_chunk_body(
                base=body,
                schema_name=card_schema,
                fields=fields,
                column=col,
                slugs=page,
                order_by=order_by,
                order=order,
            )
            label = f"{col}#{page_i + 1}/{((len(slugs) - 1) // seal_safe) + 1} n={len(page)}"
            total_shared += stage_and_approve(
                client,
                chunk_body,
                auto_approve=auto_approve,
                cleanup_staged=cleanup_staged,
                label=label,
            )
    print(f"DONE chunks={chunk_n} total_shared≈{total_shared}")
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--config",
        type=Path,
        default=default_config_path(),
        help=f"durable config JSON (default: {default_config_path()})",
    )
    p.add_argument("--dry-run", action="store_true", help="print plan; no socket writes")
    p.add_argument("--force", action="store_true", help="run even if enabled=false")
    p.add_argument(
        "--cleanup-staged",
        action="store_true",
        help=(
            "with auto_approve=false, stage a real slice, verify it is listed, "
            "then reject it so no pending delivery remains"
        ),
    )
    p.add_argument(
        "--write-example-config",
        type=Path,
        metavar="PATH",
        help="write example config JSON to PATH and exit",
    )
    args = p.parse_args(argv)

    if args.write_example_config:
        path_str = str(args.write_example_config)
        factory = "factory" in path_str or args.write_example_config.name.startswith("factory")
        if factory:
            example = {
                "enabled": True,
                "auto_approve": True,
                "profile": "factory",
                "socket": str(Path.home() / ".lastdb" / "data" / "folddb.sock"),
                "board": DEFAULT_BOARD,
                "schema_name": DEFAULT_BOARD_CARDS_SCHEMA_NAME,
                "fields": FACTORY_FIELDS,
                "since": FACTORY_SINCE,
                "since_field": DEFAULT_SINCE_FIELD,
                "columns_include": list(DEFAULT_COLUMNS),
                "order_by": DEFAULT_ORDER_BY,
                "order": "desc",
                "max_records_by_column": dict(FACTORY_MAX_BY_COLUMN),
                "max_records": sum(FACTORY_MAX_BY_COLUMN.values()),
                "recipient": {
                    "secret_id": "ExememKanbanConsumer-prod",
                    "region": "us-east-1",
                    "_comment": "Or set LASTDB_ADMIN_DELIVER_RECIPIENT_JSON with the three public keys",
                },
                "_comment": (
                    "Kanban Factory admin feed: slim full-board slice. "
                    "Cadence: launchd StartInterval=60 (see launchd/com.edgevector.admin-kanban-factory-deliver.plist)."
                ),
            }
        else:
            example = {
                "enabled": True,
                "auto_approve": True,
                "socket": str(Path.home() / ".lastdb" / "data" / "folddb.sock"),
                "board": DEFAULT_BOARD,
                "schema_name": DEFAULT_BOARD_CARDS_SCHEMA_NAME,
                "fields": DEFAULT_FIELDS,
                "since": DEFAULT_SINCE,
                "since_field": DEFAULT_SINCE_FIELD,
                "columns_include": list(DEFAULT_COLUMNS),
                "order_by": DEFAULT_ORDER_BY,
                "order": "desc",
                "max_records_per_column": DEFAULT_MAX_RECORDS_PER_COLUMN,
                "max_records": DEFAULT_MAX_RECORDS_PER_COLUMN * len(DEFAULT_COLUMNS),
                "recipient": {
                    "secret_id": "ExememKanbanConsumer-prod",
                    "region": "us-east-1",
                    "_comment": "Or set LASTDB_ADMIN_DELIVER_RECIPIENT_JSON with the three public keys"
                },
            }
        args.write_example_config.parent.mkdir(parents=True, exist_ok=True)
        args.write_example_config.write_text(json.dumps(example, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {args.write_example_config}")
        return 0

    try:
        return run(
            args.config,
            dry_run=args.dry_run,
            force=args.force,
            cleanup_staged=args.cleanup_staged,
        )
    except DeliverError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as e:
        print(f"error: aws/secrets failed: {e}", file=sys.stderr)
        return 2
    except json.JSONDecodeError as e:
        print(f"error: invalid JSON: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
