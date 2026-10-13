"""Config loading, schema names, recipient and stage-body builders."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any
from deliver_constants import BOARD_CARDS_FIELD_SET, DEFAULT_FIELDS, FACTORY_FIELDS, DEFAULT_SCHEMA_NAME, DEFAULT_BOARD_CARDS_SCHEMA_NAME, DEFAULT_BOARD, DEFAULT_SINCE_FIELD, DEFAULT_ORDER_BY, DEFAULT_MAX_RECORDS, DEFAULT_MAX_RECORDS_PER_COLUMN, DEFAULT_COLUMNS, DEFAULT_SINCE, FACTORY_SINCE, FACTORY_MAX_BY_COLUMN
from deliver_client import DeliverError


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
