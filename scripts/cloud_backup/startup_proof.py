"""Bind the source daemon to this run's actual admitted startup artifact."""

import hashlib
import os
from pathlib import Path

from ci_control import digest
from ownership import run_identity
from support import (ACCOUNT, canonical_uuid, decode_json, private_file, read_json,
                     require, save_json, stable)


def runner_path(path):
    runner = Path(os.environ.get("RUNNER_TEMP", ""))
    require(runner.is_absolute() and runner.is_dir() and path.is_absolute()
            and path.parent.resolve() == runner.resolve(), "ci_start_proof_runner_path")
    return path


def save(path, record, registered, baseline, source):
    runner_path(path)
    receipt = record["ownership"]
    require(receipt["owner"] == run_identity(source), "ci_start_proof_owner")
    save_json(path, {
        "schema": "lastdb.cloud-ci-start-admission.v1", "owner": receipt["owner"],
        "account": receipt["account"], "account_generation": receipt["account_generation"],
        "stream_id": registered["stream_id"], "state": registered["state"],
        "baseline": {name: baseline[name] for name in ("stream_id", "commit_id", "commit_sha256")},
        "artifact_id": record["artifact_id"], "artifact_sha256": record["artifact_sha256"],
        "receipt_sha256": record["receipt_sha256"],
        "registration_sha256": registered["registration_sha256"],
        "startup_capabilities_verified": True,
    })


def load(path, artifact, baseline, source):
    value = read_json(runner_path(path), 4096)
    runner = Path(os.environ["RUNNER_TEMP"]).resolve()
    require(artifact.is_absolute() and artifact.parent.resolve().is_relative_to(runner),
            "ci_start_proof_artifact_path")
    stream_file, before = private_file(artifact)
    with stream_file:
        raw = stream_file.read(16385)
        require(len(raw) <= 16384 and stable(before, os.fstat(stream_file.fileno())),
                "ci_start_proof_receipt_size")
    receipt = decode_json(raw)
    require(isinstance(value, dict) and value.get("schema") == "lastdb.cloud-ci-start-admission.v1"
            and value.get("owner") == run_identity(source)
            and value.get("account") == ACCOUNT
            and value.get("account_generation") == baseline["account_generation"]
            and value.get("state") == "startup_registered"
            and value.get("startup_capabilities_verified") is True
            and value.get("baseline") == {name: baseline[name] for name in (
                "stream_id", "commit_id", "commit_sha256")}, "ci_start_proof_identity")
    stream = canonical_uuid(value.get("stream_id"))
    require(stream != baseline["stream_id"] and receipt == {
        "schema": "lastdb.cloud-ci-start.v1", "owner": value["owner"], "account": ACCOUNT,
        "account_generation": baseline["account_generation"], "stream_id": stream},
        "ci_start_proof_receipt")
    require(hashlib.sha256(raw).hexdigest() == digest(value.get("receipt_sha256")),
            "ci_start_proof_digest")
    digest(value.get("artifact_sha256"))
    digest(value.get("registration_sha256"))
    return stream
