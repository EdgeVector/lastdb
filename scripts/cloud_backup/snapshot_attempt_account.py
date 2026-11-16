"""Provision and retire one signed DEV snapshot CI attempt."""

from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
import re
import time
from urllib.parse import parse_qsl, urlsplit
import uuid

from snapshot_cleanup import (CLEANUP_BUDGET, DATABASE_PREFIXES, GRANT_TTL_MS, RunAttempt,
                              SnapshotOwner, released_bytes)
from support import (CREDENTIALS_FILE, OperationError, decode_json, dev_endpoint, private_file,
                     read_json, require, stable, write_private)


ROOT = "/api/cloud/backups/v2/dev-snapshot-proof"
AUDIENCE = "lastdb-dev-snapshot"
MAX_REPLY = 2_000_000
MAX_PRIOR_ATTEMPTS = 50
STATUS_WORKERS = 8
ATTEMPT_MISSING = {"error": "snapshot_proof_attempt_missing"}
ATTEMPT_ACTIVE = {"error": "snapshot_proof_attempt_active"}
BROKER_REFUSALS = {"snapshot_proof_scope", "snapshot_proof_attempt", "snapshot_proof_body",
                   "snapshot_proof_identity", "snapshot_proof_conflict", "snapshot_proof_integrity",
                   "snapshot_proof_inspect_through", "snapshot_proof_database_version"}


def _request(host, method, path, token, value=None):
    headers = {"Authorization": "Bearer " + token, "Accept": "application/json"}
    raw = None
    if value is not None:
        raw = json.dumps(value, separators=(",", ":")).encode("ascii")
        headers["Content-Type"] = "application/json"
    connection = http.client.HTTPSConnection(host, timeout=25)
    try:
        connection.request(method, path, body=raw, headers=headers)
        response = connection.getresponse()
        payload = response.read(MAX_REPLY + 1)
    except (OSError, http.client.HTTPException):
        return 503, None
    finally:
        connection.close()
    require(len(payload) <= MAX_REPLY, "snapshot_attempt_response_size")
    if response.status in (429, 502, 503, 504):
        return 503, None
    try:
        return response.status, decode_json(payload)
    except OperationError:
        if response.status == 200:
            raise
        return response.status, None


def _oidc():
    url = urlsplit(os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL", ""))
    require(
        url.scheme == "https" and url.hostname == url.netloc and url.hostname.endswith(".actions.githubusercontent.com")
        and url.path.startswith("/") and not url.fragment,
        "snapshot_attempt_oidc_endpoint",
    )
    request_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "")
    require(re.fullmatch(r"[!-~]{1,16384}", request_token), "snapshot_attempt_oidc_token")
    query = parse_qsl(url.query, keep_blank_values=True)
    require(not any(name == "audience" for name, _ in query), "snapshot_attempt_oidc_audience")
    path = url.path + "?" + (url.query + "&" if url.query else "") + "audience=" + AUDIENCE
    status, reply = _request(url.hostname, "GET", path, request_token)
    if status == 503:
        return None
    require(status == 200 and isinstance(reply, dict), "snapshot_attempt_oidc_service")
    token = reply.get("value")
    require(isinstance(token, str) and re.fullmatch(r"[!-~]{1,16384}", token), "snapshot_attempt_oidc_reply")
    return token


def _broker(action, value, *, token=None):
    # Obtain a fresh GitHub token for each operation. The upload-grant drain can
    # outlive the short-lived token first issued to the workflow.
    if token is None:
        token = _oidc()
    if token is None:
        return 503, None
    return _request(urlsplit(dev_endpoint()).hostname, "POST", f"{ROOT}/{action}", token, value)


def _require_success(status, response, operation):
    if status != 200:
        code = response.get("error") if isinstance(response, dict) else None
        detail = f"HTTP {status}"
        if isinstance(code, str) and code in BROKER_REFUSALS:
            detail += f" {code}"
        raise OperationError(f"snapshot_attempt_{operation}", service_field=detail)


def _binding(reply, attempt, *, historical=False):
    databases = {attempt.database_for(version) for version in DATABASE_PREFIXES} if historical else {attempt.database}
    require(isinstance(reply, dict) and isinstance(reply.get("database_id"), str)
            and reply["database_id"] in databases, "snapshot_attempt_database")
    account = reply.get("account_id")
    require(isinstance(account, str) and re.fullmatch(r"[0-9a-f]{32}", account)
            and reply.get("account_generation") == "1", "snapshot_attempt_scope")
    return account, reply["database_id"]


def _private_value(path, cap):
    stream, info = private_file(path)
    with stream:
        require(0 < info.st_size <= cap, "snapshot_attempt_local_file")
        value = stream.read(cap + 1)
        require(len(value) == info.st_size and stable(info, os.fstat(stream.fileno())), "snapshot_attempt_local_file")
    return value


def provision(programs, root, attempt):
    from node_auth import NodeKey, account_id

    home = root / "enrollment"
    phrase_path = home / "recovery-phrase.txt"
    initialized = programs.command(None, ["admin", "backup", "init", "--env", "dev", "--into", str(home),
                                          "--recovery-phrase-out", str(phrase_path), "--json"])
    key = NodeKey.from_seed(_private_value(home / "identity.key", 32))
    account = account_id(key.public)
    require(initialized.get("ok") is True and initialized.get("account_id") == account
            and initialized.get("environment") == "dev"
            and initialized.get("network_request") is False, "snapshot_attempt_initialization")
    phrase = _private_value(phrase_path, 4096)
    device = str(uuid.uuid4())
    # The product's native enrollment reads this exact local UUID before signup.
    write_private(home / ".native_backup_device_id", device.encode("ascii"))
    request = {"public_key": key.public.hex(), "device_id": device, "database_id_version": attempt.database_version}
    for _ in range(4):
        status, reply = _broker("provision", request)
        if status != 503:
            break
        time.sleep(2)
    _require_success(status, reply, "provision")
    require(isinstance(reply, dict) and set(reply) == {"account_id", "account_generation", "database_id"}
            and _binding(reply, attempt) == (account, attempt.database), "snapshot_attempt_provision")
    connected = programs.command(home, ["backup", "connect", "--env", "dev", "--json"], timeout=60)
    require(connected.get("ok") is True and connected.get("account_id") == account
            and connected.get("registration_attempted") is True and connected.get("credentials_saved") is True,
            "snapshot_attempt_enrollment")
    saved = read_json(home / CREDENTIALS_FILE, 16384)
    require(isinstance(saved, dict) and isinstance(saved.get("connection"), dict), "snapshot_attempt_enrollment_scope")
    api_key = saved.get("api_key")
    require(saved.get("api_url") == dev_endpoint() and saved["connection"].get("account_id") == account
            and isinstance(api_key, str) and re.fullmatch(r"[!-~]{1,4096}", api_key), "snapshot_attempt_enrollment_scope")
    return SnapshotOwner(key, reply["account_generation"], phrase, api_key)


def _progress(reply, attempt, *, historical):
    scope = _binding(reply, attempt, historical=historical)
    require(set(reply) == {"phase", "account_id", "account_generation", "database_id", "retry_after_ms", "released_bytes"},
            "snapshot_attempt_progress")
    phase = reply["phase"]
    wait = reply["retry_after_ms"]
    released = released_bytes(reply["released_bytes"])
    require(phase in ("open", "retiring", "complete") and type(wait) is int and 0 <= wait <= GRANT_TTL_MS
            and (phase == "retiring" or wait == 0) and (phase == "complete" or released == 0),
            "snapshot_attempt_progress")
    return scope, phase, wait, released


def resume_instruction(run_id, number):
    return ("gh workflow run snapshot-roundtrip-dev.yml -R EdgeVector/lastdb --ref main "
            f"-f mode=resume-cleanup -f original_run_id={run_id} -f original_run_attempt={number}")


def _request_prior_attempt(action, attempt, token):
    for _ in range(3):
        status, reply = _broker(action, attempt.identity(), token=token)
        if status != 503:
            break
        time.sleep(2)
    return status, reply


def _reconcile_prior_attempt(attempt, token):
    operation = "status"
    status, reply = _request_prior_attempt(operation, attempt, token)
    if status == 404 and reply == ATTEMPT_MISSING:
        # Status only observes absence. Retirement verifies the terminal attempt
        # and seals it against a late provision before reporting no account.
        operation = "retire"
        status, reply = _request_prior_attempt(operation, attempt, token)
        if status == 404 and reply == ATTEMPT_MISSING:
            return None
    _require_success(status, reply, operation)
    scope, phase, _wait, _released = _progress(reply, attempt, historical=True)
    return scope, phase


def require_prior_retired(attempt, summary):
    number = int(attempt.attempt)
    require(number - 1 <= MAX_PRIOR_ATTEMPTS, "snapshot_attempt_history_bound")
    if number == 1:
        return
    token = _oidc()
    require(token is not None, "snapshot_attempt_oidc_service")
    prior = [RunAttempt(attempt.run_id, str(i)) for i in range(1, number)]
    with ThreadPoolExecutor(max_workers=STATUS_WORKERS) as pool:
        states = list(pool.map(lambda previous: _reconcile_prior_attempt(previous, token), prior))
    pending = []
    for previous, state in zip(prior, states, strict=True):
        if state is None:
            summary(f"Sealed prior snapshot run {previous.run_id} attempt {previous.attempt} without an account.")
        elif state[1] != "complete":
            pending.append((previous, state[0][1]))
    for previous, database in pending:
        summary(f"Earlier snapshot attempt owns an unretired database {database}. Finish cleanup before retrying:")
        summary(resume_instruction(previous.run_id, previous.attempt))
    require(not pending, "snapshot_attempt_prior_retirement_required")


def retire(attempt, summary, *, expect_charge, historical=False):
    deadline = time.monotonic() + CLEANUP_BUDGET
    scope = None
    target = attempt.identity()
    for _ in range(128):
        if time.monotonic() >= deadline:
            break
        status, reply = _broker("retire", target)
        if status == 503:
            time.sleep(2)
            continue
        if status == 404 and reply == ATTEMPT_MISSING:
            require(not expect_charge and scope is None, "snapshot_attempt_missing_charge")
            summary("snapshot_account=not_provisioned")
            return
        if status == 409 and reply == ATTEMPT_ACTIVE:
            raise OperationError("snapshot_attempt_original_active")
        _require_success(status, reply, "retirement")
        current, phase, wait, released = _progress(reply, attempt, historical=historical)
        require(scope is None or scope == current, "snapshot_attempt_retirement_scope")
        account, database = current
        if phase == "complete":
            require(not expect_charge or released > 0, "snapshot_attempt_missing_charge")
            summary(f"snapshot_account=retired account_id={account} database_id={database} released_bytes={released}")
            return
        require(phase == "retiring", "snapshot_attempt_retirement_phase")
        if scope is None:
            summary(f"snapshot_account=retiring account_id={account} database_id={database}")
        scope = current
        time.sleep(min(max(1, wait / 1000), max(0, deadline - time.monotonic())))
    raise OperationError("snapshot_attempt_retirement_incomplete")


def inspect(attempt, *, through):
    require(type(through) is int and 1 <= through <= 32, "snapshot_attempt_inspect_through")
    status, reply = _broker("inspect", {"through": through})
    _require_success(status, reply, "inspect")
    require(isinstance(reply, dict) and set(reply) == {"database_id", "entries", "facts"}
            and reply["database_id"] == attempt.database, "snapshot_attempt_inspect_scope")
    entries, facts = reply["entries"], reply["facts"]
    require(isinstance(entries, dict) and len(entries) <= through
            and all(re.fullmatch(r"e#[0-9]{20}", key) and 1 <= int(key[2:]) <= through and isinstance(row, dict)
                    for key, row in entries.items())
            and isinstance(facts, dict) and len(facts) <= 256
            and all(key.startswith("write#") and isinstance(row, dict) for key, row in facts.items()),
            "snapshot_attempt_inspect_entries")
    return entries, facts
