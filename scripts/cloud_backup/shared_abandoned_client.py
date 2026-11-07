"""Use real signed DEV requests where the normal uploader would continue automatically."""

import hashlib
import json
import re
import subprocess
import time
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, build_opener

from baseline import private_write
from shared_daily_io import NoRedirect, now_ms
from support import OperationError, decode_json, require


ROUTES = {
    "prepare": ("objects/prepare", "object-prepare-authority"),
    "complete": ("objects/complete", "object-complete-authority"),
    "inventory": ("snapshots/inventory/page", "inventory-page-authority"),
    "publish": ("snapshots/publish", "publish-authority"),
}
EXPIRED = {"code": "job_closed", "field": "shared.snapshot.staging_expired"}


def encoded(value):
    # These requests use ASCII strings, small integral counters and decimal u64 strings.
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def digest(domain, value):
    label, body = ("lastdb:shared-log:" + domain + ":v1").encode(), encoded(value)
    return hashlib.sha256(len(label).to_bytes(4, "big") + label
                          + len(body).to_bytes(8, "big") + body).hexdigest()


class Client:
    def __init__(self, operation, writer):
        self.account = operation.scope["account_id"]
        self.database = operation.scope["database_id"]
        self.device = writer["device_id"]
        self.base = operation.credentials["api_url"]
        self.api_key = operation.credentials["api_key"]
        self.environment = operation.programs.environment
        self.key = operation.root / "staging-device.der"
        self.message = operation.root / "staging-digest"
        seed = operation.device_file(self.device).read_bytes()
        require(len(seed) == 32, "shared_abandoned_device_seed")
        private_write(self.key, bytes.fromhex("302e020100300506032b657004220420") + seed)
        self.http = build_opener(NoRedirect())

    def sign(self, message):
        private_write(self.message, message.encode("ascii"))
        try:
            signed = subprocess.run(["openssl", "pkeyutl", "-sign", "-rawin", "-keyform", "DER",
                                     "-inkey", str(self.key), "-in", str(self.message)],
                                    capture_output=True, timeout=10, check=False, env=self.environment)
        finally:
            self.message.unlink()
        require(signed.returncode == 0 and len(signed.stdout) == 64, "shared_abandoned_signature")
        return signed.stdout.hex()

    def post(self, action, value, *, expired=False):
        path, domain = ROUTES[action]
        for _ in range(12):
            expires = str(now_ms() + 30_000)
            signature = self.sign(digest(domain, [self.account, self.device, expires, value]))
            request = Request(self.base + "/api/cloud/shared/v1/" + path, data=encoded(value), headers={
                "Content-Type": "application/json", "X-API-Key": self.api_key,
                "X-Lastdb-Device": self.device, "X-Lastdb-Device-Expires-Ms": expires,
                "X-Lastdb-Device-Signature": signature,
            })
            try:
                with self.http.open(request, timeout=30) as reply:
                    status, raw = reply.status, reply.read(16385)
            except HTTPError as error:
                with error:
                    status, raw = error.code, error.read(16385)
            except (URLError, OSError):
                raise OperationError("shared_abandoned_control_transport_" + action) from None
            require(len(raw) <= 16384, "shared_abandoned_control_response_bound")
            result = decode_json(raw) if raw else None
            if status == 503 and isinstance(result, dict) and result.get("code") in (
                    "retry_later", "outcome_unknown"):
                time.sleep(2)
                continue
            if expired:
                require(status == 400 and result == EXPIRED, "shared_abandoned_exact_staging_refusal")
                return result
            require(status == 200, "shared_abandoned_control_refused_" + action)
            return result
        raise OperationError("shared_abandoned_control_retry_" + action)

    def upload(self, snapshot, destination, payload):
        obj = {"version_id": str(uuid.uuid4()), "bytes": str(len(payload)),
               "sha256": hashlib.sha256(payload).hexdigest()}
        request = {"database_id": self.database, "object": obj,
                   "role": {"kind": "native_snapshot", "snapshot_id": snapshot,
                            "destination": destination}}
        reply = self.post("prepare", request)
        grant = reply.get("grant") if reply.get("status") == "upload" else None
        require(grant and grant["object"] == obj and int(grant["expires_at_ms"]) > now_ms(),
                "shared_abandoned_exact_upload_grant")
        url = urlsplit(grant["url"])
        if destination == "b2":
            bucket, prefix = "exemem-dev", "cloud-backup/v2/shared"
            host = url.hostname == "s3.us-east-005.backblazeb2.com"
            query = parse_qs(url.query)
            require(query.get("partNumber") == ["1"] and len(query.get("uploadId", [])) == 1,
                    "shared_abandoned_b2_multipart_grant")
        else:
            bucket, prefix = "exemem-sync-dev", "cloud-shared/v1"
            host = re.fullmatch(r"[a-f0-9]{32}\.r2\.cloudflarestorage\.com", url.hostname or "")
        expected_path = f"/{bucket}/{prefix}/{self.database}/objects/{obj['version_id']}"
        require(host and url.scheme == "https" and url.port is None and url.username is None
                and not url.fragment and url.path == expected_path, "shared_abandoned_provider_scope")
        headers = {} if destination == "b2" else {"If-None-Match": "*"}
        upload = Request(grant["url"], method="PUT", data=payload, headers=headers)
        try:
            with self.http.open(upload, timeout=30) as response:
                require(response.status == 200, "shared_abandoned_provider_put")
        except (URLError, OSError):
            raise OperationError("shared_abandoned_provider_put") from None
        return request

    def complete(self, request):
        for _ in range(12):
            reply = self.post("complete", request)
            if reply == {"status": "verified", "object": request["object"]}:
                return
            require(reply.get("status") == "pending", "shared_abandoned_exact_verified_object")
            time.sleep(2)
        raise OperationError("shared_abandoned_complete_deadline")
