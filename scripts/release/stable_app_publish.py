#!/usr/bin/env python3
"""Publish only attested, exact-API app declarations with a dedicated KMS key."""

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

import boto3
from cryptography.hazmat.primitives import serialization
import rfc8785

from nightly_assets import document
from nightly_plan import _capture
from stable_plan import APPS, api
from stable_registry import catalog

SIGNED = ("app_id", "channel", "app_version", "api_version", "lastdb_build",
          "outbox_format_version", "source", "sha", "audience")


def request(url, payload=None, bearer=None):
    headers = {"Content-Type": "application/json", "User-Agent": "lastdb-ci-app-publisher"}
    if bearer:
        headers["Authorization"] = "Bearer " + bearer
    req = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(), headers=headers)
    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(req, timeout=45) as response:
            raw = response.read(1024 ** 2 + 1)
            if len(raw) > 1024 ** 2:
                raise ValueError("CI publisher response exceeds its bound")
            return json.loads(raw)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"CI app publication request failed (HTTP {error.code})") from None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def proof(path, artifact):
    result = document(path)
    source, run_id = os.environ["GITHUB_SHA"], os.environ["GITHUB_RUN_ID"]
    run_url = f"https://github.com/EdgeVector/lastdb/actions/runs/{run_id}"
    if (os.environ.get("GITHUB_REPOSITORY") != "EdgeVector/lastdb" or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or os.environ.get("GITHUB_EVENT_NAME") not in ("push", "schedule", "workflow_dispatch")
            or result.get("schema") != "lastdb.app.compatibility.proof.v1"
            or result.get("source") != source or result.get("lastdb_source") != source
            or result.get("run_url") != run_url or set(result.get("apps", {})) != set(APPS)
            or any(result.get(name) is not True for name in ("complete", "daemon_stopped", "home_removed"))
            or not re.fullmatch(r"[0-9a-f]{64}", result.get("bundle_sha256", ""))):
        raise ValueError("CI app compatibility proof differs from this accepted source")
    for app, entry in result["apps"].items():
        declaration = entry["declaration"]
        if (entry.get("complete") is not True or entry.get("api_version") != result["api_version"]
                or entry.get("outbox_format_version") != result["outbox_format_version"]
                or declaration.get("app_id") != app or declaration.get("sha") != entry["sha"]
                or not re.fullmatch(r"[0-9a-f]{40}", entry["sha"])
                or declaration.get("source") != f"https://github.com/EdgeVector/{app}.git"
                or declaration.get("api_version") != result["api_version"]
                or declaration.get("lastdb_build") != result["lastdb_build"]
                or declaration.get("channel") != "stable"
                or declaration.get("outbox_format_version", result["outbox_format_version"]) != result["outbox_format_version"]):
            raise ValueError("An app declaration differs from its observed interface or frozen source")
    rows = api(f"repos/EdgeVector/lastdb/actions/runs/{run_id}/artifacts?per_page=100")["artifacts"]
    matches = [row for row in rows if row["name"] == artifact]
    if len(matches) != 1:
        raise ValueError("The source proof artifact is absent or ambiguous")
    retained = matches[0]
    if (retained["expired"] or retained["size_in_bytes"] <= 0
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", retained.get("digest", ""))
            or datetime.fromisoformat(retained["expires_at"].replace("Z", "+00:00")) <= datetime.now(timezone.utc)):
        raise ValueError("The source proof artifact is expired or invalid")
    _capture(["gh", "attestation", "verify", str(path), "--repo", "EdgeVector/lastdb",
              "--signer-workflow", "EdgeVector/lastdb/.github/workflows/stable-release.yml",
              "--signer-digest", source, "--source-digest", source, "--source-ref", "refs/heads/main",
              "--deny-self-hosted-runners"], timeout=180)
    return result, retained


def signed_records(proved):
    kms = boto3.client("kms", region_name="us-east-1")
    arn = os.environ["REGISTRY_CI_PROD_KEY_ARN"]
    key = kms.get_public_key(KeyId=arn)
    if key["KeySpec"] != "ECC_NIST_EDWARDS25519" or key["KeyUsage"] != "SIGN_VERIFY":
        raise ValueError("CI publishing requires its dedicated Ed25519 key")
    raw = serialization.load_der_public_key(key["PublicKey"]).public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw)
    public = base64.b64encode(raw).decode()
    if public != os.environ["REGISTRY_CI_PROD_PUBLIC_KEY"]:
        raise ValueError("The CI publisher identity differs from its public configuration")
    records = []
    for app in APPS:
        for channel in ("stable", "next"):
            record = {**proved["apps"][app]["declaration"], "channel": channel}
            statement = {field: record[field] for field in SIGNED if field in record}
            signature = kms.sign(KeyId=arn, Message=rfc8785.dumps(statement), MessageType="RAW",
                                 SigningAlgorithm="ED25519_SHA_512")["Signature"]
            records.append({**record, "publisher_key": public, "publisher_sig": base64.b64encode(signature).decode()})
    return records


def publish(proved):
    profile = json.loads((Path(__file__).resolve().parents[2] / "folddb_profile/environments.json").read_text())
    base = profile["environments"]["prod"]["registry_api"].rstrip("/")
    body = {key: proved[key] for key in ("source", "run_url", "lastdb_source", "lastdb_build", "api_version", "outbox_format_version")}
    body.update(schema="lastdb.app.compatibility.publication.v1", run_attempt=int(os.environ["GITHUB_RUN_ATTEMPT"]),
                records=signed_records(proved))
    audience = base + "/v1/ci/apps/" + hashlib.sha256(rfc8785.dumps(body)).hexdigest()
    token_url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"] + "&audience=" + urllib.parse.quote(audience, safe="")
    token = request(token_url, bearer=os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"])["value"]
    reply = request(base + "/v1/ci/apps", body, token)
    del token
    if reply.get("status") != "listed" or len(reply.get("records", [])) != len(body["records"]):
        raise ValueError("The registry did not publish the complete proved app set")
    for channel in ("stable", "next"):
        for attempt in range(6):
            listing = catalog(proved["api_version"], channel=channel)
            entries = {entry["app_id"]: entry for entry in listing["apps"]}
            expected = [row for row in body["records"] if row["channel"] == channel]
            if all(row["app_id"] in entries and all(entries[row["app_id"]].get(k) == v for k, v in row.items()
                       if k != "description") for row in expected):
                break
            if attempt == 5:
                raise ValueError("The signed list does not contain the exact CI publication")
            time.sleep(7)
    return reply


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--proof", type=Path, required=True)
    parser.add_argument("--artifact", required=True)
    args = parser.parse_args()
    proved, retained = proof(args.proof, args.artifact)
    result = publish(proved)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
        stream.write(f'Published {len(result["records"])} signed app records for API {proved["api_version"]}.\n\n')
        stream.write(f'Source: {proved["source"]}. Proof: [{retained["name"]}]({retained["archive_download_url"]}), '
                     f'{retained["digest"]}; expires {retained["expires_at"]}.\n')
        stream.write('A dedicated KMS identity signed only the CI-proved declarations. The person key and primary were not accessed.\n')


if __name__ == "__main__":
    main()
