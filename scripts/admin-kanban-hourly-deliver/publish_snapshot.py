#!/usr/bin/env python3
"""Publish a LastDB query snapshot to S3 (overwrite by key; no messaging append).

Calls Mini `POST /api/sharing/snapshot` which materializes the same query-shaped
slice as deliver, seals it for the recipient, and returns:

  snapshot_key = hex(sha256(canonical_query_json || 0x00 || recipient_id))

Then PUTs the sealed blob to:

  s3://$BUCKET/delivery-snapshots/{snapshot_key}

replacing any previous object at that key.

Config: same JSON as hourly deliver (~/.lastdb/admin-kanban-hourly-deliver.json)
plus optional:

  snapshot_bucket   (default: LASTDB_SNAPSHOT_BUCKET or prod web bucket)
  snapshot_prefix   (default: delivery-snapshots)
  snapshot_region   (default: us-east-1)
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, TextIO

# Reuse deliver helpers
sys.path.insert(0, str(Path(__file__).resolve().parent))
import deliver  # noqa: E402


DEFAULT_BUCKET = os.environ.get(
    "LASTDB_SNAPSHOT_BUCKET",
    "exememstack-prod-webbucket12880f5b-ixyv8wmjrojm",
)
DEFAULT_PREFIX = os.environ.get("LASTDB_SNAPSHOT_PREFIX", "delivery-snapshots")
DEFAULT_REGION = os.environ.get("AWS_REGION", "us-east-1")


def snapshot_lock_path(cfg_path: Path) -> Path:
    return cfg_path.with_name(cfg_path.name + ".lock")


def try_acquire_snapshot_lock(cfg_path: Path) -> TextIO | None:
    """Non-blocking exclusive lock. None means a previous snapshot still runs."""
    path = snapshot_lock_path(cfg_path)
    handle = path.open("a", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        return None
    handle.seek(0)
    handle.truncate()
    handle.write(f"pid={os.getpid()}\n")
    handle.flush()
    return handle


def release_snapshot_lock(handle: TextIO | None) -> None:
    if handle is None:
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


def s3_put(local: Path, bucket: str, key: str, region: str) -> None:
    subprocess.check_call(
        [
            "aws",
            "s3",
            "cp",
            str(local),
            f"s3://{bucket}/{key}",
            "--region",
            region,
            "--content-type",
            "application/octet-stream",
            "--cache-control",
            "no-cache, max-age=0",
        ],
        stdout=subprocess.DEVNULL,
    )


def run(cfg_path: Path, *, dry_run: bool) -> int:
    cfg = deliver.load_json(cfg_path)
    lock = None if dry_run else try_acquire_snapshot_lock(cfg_path)
    if not dry_run and lock is None:
        print(
            json.dumps(
                {
                    "skipped": True,
                    "reason": "previous-snapshot-still-running",
                    "lock": str(snapshot_lock_path(cfg_path)),
                }
            )
        )
        return 0
    try:
        return _run_locked(cfg_path, cfg, dry_run=dry_run)
    finally:
        release_snapshot_lock(lock)


def _run_locked(cfg_path: Path, cfg: dict[str, Any], *, dry_run: bool) -> int:
    sock = (
        cfg.get("socket")
        or os.environ.get("LASTDB_SOCKET_PATH")
        or str(Path.home() / ".lastdb" / "data" / "folddb.sock")
    )
    recipient = deliver.load_recipient(cfg)
    body = deliver.build_stage_body(cfg, recipient)
    # Snapshot path uses legs only (full board, no messaging size chunking).
    body.pop("since", None)
    body.pop("since_field", None)
    body.pop("columns_include", None)
    body.pop("max_records", None)
    body.pop("max_records_per_column", None)
    body.pop("profile", None)
    body.pop("card_schema", None)

    bucket = str(cfg.get("snapshot_bucket") or DEFAULT_BUCKET)
    prefix = str(cfg.get("snapshot_prefix") or DEFAULT_PREFIX).strip("/")
    region = str(cfg.get("snapshot_region") or DEFAULT_REGION)

    client: deliver.MiniClient | None = None
    if not dry_run:
        client = deliver.MiniClient(socket_path=str(sock), user_hash=cfg.get("user_hash"))
        resolved = deliver.resolve_stage_schema_name(client, str(body["schema_name"]))
        body["schema_name"] = resolved
        for leg in body.get("legs") or []:
            if isinstance(leg, dict):
                leg["schema_name"] = resolved

    plan = {
        "socket": sock,
        "schema_name": body["schema_name"],
        "board": body.get("board"),
        "legs": len(body.get("legs") or []),
        "bucket": bucket,
        "prefix": prefix,
        "dry_run": dry_run,
    }
    print(json.dumps(plan, indent=2))

    if dry_run:
        safe = {
            k: v
            for k, v in body.items()
            if k
            not in {
                "recipient_pubkey",
                "messaging_public_key",
                "messaging_pseudonym",
            }
        }
        safe["recipient"] = "(redacted)"
        print("snapshot body (recipient redacted):")
        print(json.dumps(safe, indent=2))
        return 0

    assert client is not None
    raw = client.request("POST", "/api/sharing/snapshot", body)
    payload = deliver.unwrap(raw)
    key = payload.get("snapshot_key")
    blob_b64 = payload.get("encrypted_blob")
    n = payload.get("record_count")
    if not key or not blob_b64:
        raise deliver.DeliverError(f"snapshot response missing key/blob: {json.dumps(payload)[:400]}")

    # CloudFront SPA routing function rewrites extensionless keys; use .bin.
    object_key = f"{prefix}/{key}.bin"
    recipient_id = str(payload.get("recipient_id") or recipient["messaging_pseudonym"])
    # Discoverable pointer for admin SPA (still one overwrite slot per recipient+channel).
    alias_key = f"{prefix}/aliases/{recipient_id}/kanban.json"
    import base64
    from datetime import datetime, timezone

    blob = base64.b64decode(blob_b64)
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tmp:
        tmp.write(blob)
        tmp_path = Path(tmp.name)
    try:
        s3_put(tmp_path, bucket, object_key, region)
    finally:
        tmp_path.unlink(missing_ok=True)

    alias = {
        "snapshot_key": key,
        "recipient_id": recipient_id,
        "channel": "kanban",
        "record_count": n,
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "path": f"/{object_key}",
        "transport": "object_snapshot",
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tmp:
        json.dump(alias, tmp, separators=(",", ":"))
        alias_path = Path(tmp.name)
    try:
        subprocess.check_call(
            [
                "aws",
                "s3",
                "cp",
                str(alias_path),
                f"s3://{bucket}/{alias_key}",
                "--region",
                region,
                "--content-type",
                "application/json",
                "--cache-control",
                "no-cache, max-age=0",
            ],
            stdout=subprocess.DEVNULL,
        )
    finally:
        alias_path.unlink(missing_ok=True)

    out = {
        "snapshot_key": key,
        "s3_uri": f"s3://{bucket}/{object_key}",
        "public_path": f"/{object_key}",
        "alias_path": f"/{alias_key}",
        "record_count": n,
        "bytes": len(blob),
        "transport": "object_snapshot",
    }
    print(json.dumps(out, indent=2))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--config",
        type=Path,
        default=deliver.default_config_path(),
        help="deliver config JSON",
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    try:
        return run(args.config, dry_run=args.dry_run)
    except deliver.DeliverError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
