#!/usr/bin/env python3
"""Prove a multi-slot guaranteed write with two real ephemeral Mini nodes.

The script creates two throwaway homes, enrolls two Exemem DEV identities,
creates a shared org target on Mini A, grants Mini B writer access, and races
one three-slot set grant from each node's own cloud credential. It prints only
non-secret evidence. It never uses the primary LastDB home.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_BOOTSTRAP_SECRET = "dogfood-onboarding-dev-api-key"
SLOTS = [
    {"field_id": "git_ref_oid", "slot": "bWFpbg", "expected_version": None},
    {"field_id": "merge_fence", "slot": "Y3ItNDI", "expected_version": None},
    {"field_id": "pack_digest", "slot": "cmVwby1tYWlu", "expected_version": None},
]


class ProofError(RuntimeError):
    pass


def dev_api_url() -> str:
    path = ROOT / "folddb_profile" / "environments.json"
    with path.open() as source:
        return json.load(source)["environments"]["dev"]["exemem_api"]


def secret_value(slug: str) -> str:
    result = subprocess.run(
        ["lastsecrets", "get", slug], text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise ProofError(f"could not read lastsecrets://{slug}")
    value = result.stdout.strip()
    if not value:
        raise ProofError(f"lastsecrets://{slug} was empty")
    return value


def request_json(url: str, body: dict, api_key: str) -> tuple[int, dict]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-API-Key": api_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        raw = error.read().decode(errors="replace")
        try:
            return error.code, json.loads(raw)
        except json.JSONDecodeError:
            return error.code, {"error": "non-json response"}


def require_ok(label: str, status: int, response: dict) -> dict:
    if status < 300 and response.get("ok") is True:
        return response
    reason = response.get("reason") or response.get("error") or "unknown error"
    raise ProofError(f"{label} failed: HTTP {status}: {reason}")


def mint_invite(api_url: str, api_key: str) -> str:
    response = require_ok(
        "mint DEV invite", *request_json(f"{api_url}/api/auth/invite-codes", {}, api_key)
    )
    code = response.get("code")
    if not isinstance(code, str) or not code:
        raise ProofError("DEV invite response did not contain a code")
    return code


def connect_dev(home: Path, invite_code: str, lastdb: str) -> None:
    result = subprocess.run(
        [lastdb, "--data-dir", str(home), "connect", "--env", "dev", "--invite-code-stdin"],
        input=invite_code + "\n",
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "FOLDDB_DISABLE_KEYCHAIN": "1"},
    )
    if result.returncode:
        # Do not include command output: fresh enrollment can print a recovery phrase.
        raise ProofError(f"DEV enrollment failed for {home.name}")


def cloud_config(home: Path) -> dict:
    with (home / "cloud_sync.json").open() as source:
        config = json.load(source)
    if not isinstance(config.get("api_url"), str) or not isinstance(config.get("api_key"), str):
        raise ProofError(f"invalid cloud_sync.json in {home.name}")
    return config


def start_mini(home: Path, lastdbd: str) -> subprocess.Popen[str]:
    log = (home / "mini.log").open("w")
    return subprocess.Popen(
        [lastdbd, "--data-dir", str(home)],
        text=True,
        stdout=log,
        stderr=subprocess.STDOUT,
        env={**os.environ, "FOLDDB_DISABLE_KEYCHAIN": "1"},
    )


def stop_mini(process: subprocess.Popen[str] | None) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=15)


def uds_json(home: Path, method: str, path: str, body: dict | None, db_locator: str | None) -> dict:
    socket_path = home / "data" / "folddb.sock"
    payload = b"" if body is None else json.dumps(body).encode()
    headers = [
        f"{method} {path} HTTP/1.1",
        "Host: localhost",
        "X-LastDB-Client: real-mini-guaranteed-write-proof",
        "Connection: close",
    ]
    if db_locator:
        headers.append(f"X-LastDB-Db: {db_locator}")
    if payload:
        headers.extend(["Content-Type: application/json", f"Content-Length: {len(payload)}"])
    request = ("\r\n".join(headers) + "\r\n\r\n").encode() + payload
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(10)
        client.connect(str(socket_path))
        client.sendall(request)
        response = bytearray()
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            response.extend(chunk)
    head, separator, raw = bytes(response).partition(b"\r\n\r\n")
    if not separator or b" 200 " not in head.split(b"\r\n", 1)[0]:
        status = head.split(b"\r\n", 1)[0].decode(errors="replace")
        detail = raw.decode(errors="replace").strip()[:500]
        raise ProofError(f"Mini {home.name} rejected {path}: {status}: {detail or 'no error detail'}")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as error:
        raise ProofError(f"Mini {home.name} returned invalid JSON for {path}") from error


def wait_for_mini(home: Path, process: subprocess.Popen[str]) -> dict:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise ProofError(f"Mini {home.name} exited during startup")
        try:
            return uds_json(home, "GET", "/api/system/auto-identity", None, None)
        except (OSError, ProofError):
            time.sleep(0.2)
    raise ProofError(f"Mini {home.name} did not become ready")


def start_ready_mini(home: Path, lastdbd: str) -> tuple[subprocess.Popen[str], dict]:
    """Start a Mini with bounded retries for transient DEV bootstrap failures."""
    last_error: ProofError | None = None
    for attempt in range(1, 4):
        process = start_mini(home, lastdbd)
        try:
            return process, wait_for_mini(home, process)
        except ProofError as error:
            last_error = error
            stop_mini(process)
            if attempt < 3:
                time.sleep(attempt * 2)
    raise ProofError(f"Mini {home.name} did not become ready after 3 attempts") from last_error


def retry_uds_json(home: Path, method: str, path: str, body: dict, db_locator: str) -> dict:
    """Retry the idempotent org ceremony calls after a transient DEV 500."""
    last_error: ProofError | None = None
    for attempt in range(1, 4):
        try:
            return uds_json(home, method, path, body, db_locator)
        except ProofError as error:
            last_error = error
            if attempt < 3:
                time.sleep(attempt * 2)
    raise ProofError(f"Mini {home.name} could not complete {path} after 3 attempts") from last_error


def proof_request(org_hash: str, contender: str) -> dict:
    return {
        "action": "guaranteed_write_set_cas",
        "org_hash": org_hash,
        "model_version": 1,
        "guaranteed_version": contender,
        "guaranteed_payload": base64.urlsafe_b64encode(f"sealed-{contender}".encode()).decode().rstrip("="),
        "guaranteed_write_set": SLOTS,
    }


def set_get(api_url: str, api_key: str, org_hash: str) -> tuple[int, dict]:
    return request_json(
        f"{api_url}/api/sync/presign", {"action": "guaranteed_write_set_get", "org_hash": org_hash}, api_key
    )


def slot_versions(head: dict) -> set[str]:
    return {slot.get("version", "") for slot in head.get("slots", [])}


def grant_acknowledges_complete_set(head: dict, version: str) -> bool:
    """Return true only when the head acknowledges the complete requested set."""
    expected_slots = {(slot["field_id"], slot["slot"]) for slot in SLOTS}
    grant = head.get("last_grant")
    if not isinstance(grant, dict) or grant.get("version") != version:
        return False
    grant_slots = {
        (slot.get("field_id"), slot.get("slot"))
        for slot in grant.get("slots", [])
        if isinstance(slot, dict)
    }
    state_slots = {
        (slot.get("field_id"), slot.get("slot"))
        for slot in head.get("slots", [])
        if isinstance(slot, dict) and slot.get("version") == version
    }
    return len(grant.get("slots", [])) == len(expected_slots) and grant_slots == expected_slots and expected_slots <= state_slots


def run_proof(args: argparse.Namespace) -> dict:
    # macOS may expand its default temporary directory to a path that is too
    # long for Mini's Unix control socket. `/private/tmp` keeps both node
    # socket paths below the sockaddr_un limit.
    root = Path(tempfile.mkdtemp(prefix="lgw-", dir="/private/tmp"))
    home_a, home_b = root / "node-a", root / "node-b"
    process_a: subprocess.Popen[str] | None = None
    process_b: subprocess.Popen[str] | None = None
    try:
        bootstrap_key = secret_value(args.bootstrap_api_key_secret)
        api_url = args.api_url or dev_api_url()
        connect_dev(home_a, mint_invite(api_url, bootstrap_key), args.lastdb)
        config_a = cloud_config(home_a)
        connect_dev(home_b, mint_invite(config_a["api_url"], config_a["api_key"]), args.lastdb)
        config_b = cloud_config(home_b)

        # These throwaway DEV homes explicitly permit org target registration.
        # The primary home has no such policy file.
        for home in (home_a, home_b):
            (home / "org_sync_registration.json").write_text(
                json.dumps({"allow_registration": True}) + "\n"
            )

        # The DEV auth bootstrap can return a transient 500. Boot one node at
        # a time, and retry each fresh daemon at most three times.
        process_a, identity_a = start_ready_mini(home_a, args.lastdbd)
        process_b, identity_b = start_ready_mini(home_b, args.lastdbd)
        user_a = identity_a.get("user_hash")
        user_b = identity_b.get("user_hash")
        if not isinstance(user_a, str) or not isinstance(user_b, str) or user_a == user_b:
            raise ProofError("the two Minis did not produce distinct DEV principals")

        org_hash = secrets.token_hex(32)
        org_slug = f"gw-proof-{org_hash[:12]}"
        db_locator = f"lastdb://org/{org_slug}/shared"
        e2e_key_b64 = base64.b64encode(secrets.token_bytes(32)).decode()
        register = {"org_hash": org_hash, "e2e_key_b64": e2e_key_b64, "slug": org_slug}
        retry_uds_json(home_a, "POST", "/api/org/sync/register", register, db_locator)
        retry_uds_json(
            home_a,
            "POST",
            "/api/org/sync/grant-member",
            {"org_hash": org_hash, "target_user_hash": user_b, "role": "writer"},
            db_locator,
        )
        retry_uds_json(home_b, "POST", "/api/org/sync/register", register, db_locator)

        for name, config in [("A", config_a), ("B", config_b)]:
            initial = require_ok(f"node {name} initial set read", *set_get(config["api_url"], config["api_key"], org_hash))
            if initial.get("latest") is not None:
                raise ProofError(f"node {name} found a nonempty fresh org head")

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            futures = {
                "node-a": pool.submit(request_json, config_a["api_url"] + "/api/sync/presign", proof_request(org_hash, "node-a"), config_a["api_key"]),
                "node-b": pool.submit(request_json, config_b["api_url"] + "/api/sync/presign", proof_request(org_hash, "node-b"), config_b["api_key"]),
        }
            results = {name: future.result() for name, future in futures.items()}
        winners = [name for name, (status, body) in results.items() if status < 300 and body.get("ok") is True]
        if len(winners) != 1:
            outcomes = {
                name: {
                    "status": status,
                    "ok": body.get("ok"),
                    "reason": body.get("reason") or body.get("error"),
                }
                for name, (status, body) in results.items()
            }
            raise ProofError(f"the concurrent race did not produce exactly one winner: {outcomes}")

        stop_mini(process_b)
        process_b, _ = start_ready_mini(home_b, args.lastdbd)
        observed = require_ok("node B peer read after restart", *set_get(config_b["api_url"], config_b["api_key"], org_hash))
        head = observed.get("latest")
        winner = winners[0]
        expected_version = winner
        if not isinstance(head, dict) or len(head.get("slots", [])) != len(SLOTS):
            raise ProofError("peer did not read the complete durable set head")
        if slot_versions(head) != {expected_version} or not grant_acknowledges_complete_set(head, expected_version):
            raise ProofError("peer observed a split or wrong winner")

        return {
            "ok": True,
            "node_a_user_hash": user_a,
            "node_b_user_hash": user_b,
            "org_hash": org_hash,
            "db_locator": db_locator,
            "slot_count": len(SLOTS),
            "winner": winner,
            "loser": "node-b" if winner == "node-a" else "node-a",
            "peer_restart": "node-b",
            "peer_slot_versions": sorted(slot_versions(head)),
            "grant_acknowledged_complete_set": True,
        }
    finally:
        stop_mini(process_a)
        stop_mini(process_b)
        if args.keep:
            print(json.dumps({"kept_root": str(root)}))
        else:
            shutil.rmtree(root, ignore_errors=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lastdb", default="lastdb")
    parser.add_argument("--lastdbd", default="lastdbd")
    parser.add_argument("--api-url", help="DEV Exemem API URL; default comes from environments.json")
    parser.add_argument("--bootstrap-api-key-secret", default=DEFAULT_BOOTSTRAP_SECRET)
    parser.add_argument("--keep", action="store_true", help="keep only this run's fresh temporary root for diagnosis")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        print(json.dumps(run_proof(parse_args()), sort_keys=True))
    except ProofError as error:
        print(f"proof failed: {error}", file=sys.stderr)
        sys.exit(1)
