"""Run-owned developer publication through the actual schema service APIs."""

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import http.client
import json
import os

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519


def canonical(value):
    # These controlled publication documents contain strings, booleans, and
    # small integers. Their sorted JSON bytes are the JCS protocol bytes.
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()


def b64(value):
    return base64.b64encode(value).decode()


class CiPublisher:
    def __init__(self):
        if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("RUNNER_ENVIRONMENT") != "github-hosted":
            raise RuntimeError("the generated publisher is restricted to hosted CI")
        root = ec.generate_private_key(ec.SECP256R1())
        self._developer = ed25519.Ed25519PrivateKey.generate()
        public = self._developer.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        root_der = root.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        issued = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=5)
        self.issued = issued.isoformat().replace("+00:00", "Z")
        self.expires = (issued + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        cert = {"version": 1, "purpose": "dev_cert", "alg": "ES256", "key_id": hashlib.sha256(root_der).hexdigest(),
                "dev_pubkey": b64(public), "user_hash": hashlib.sha256(public).hexdigest(),
                "issued_at": self.issued, "expires_at": self.expires, "env": "dev", "authorized_publisher": True}
        cert["sig"] = b64(root.sign(canonical(cert), ec.ECDSA(hashes.SHA256())))
        self._cert = b64(canonical(cert))
        self._key_id = hashlib.sha256(public).hexdigest()
        # Only the public trust anchor reaches the generated service process.
        # Private keys stay in memory and are never written or printed.
        self.environment = {"APP_IDENTITY_ROOT_PUBKEYS": b64(root_der), "ENVIRONMENT": "dev"}

    def request(self, port, path, purpose, body, payload):
        envelope = {"version": 1, "purpose": purpose, "alg": "Ed25519", "key_id": self._key_id,
                    "issued_at": self.issued, "expires_at": self.expires, "env": "dev",
                    "payload_hash": hashlib.sha256(canonical(payload)).hexdigest()}
        envelope["sig"] = b64(self._developer.sign(canonical(envelope)))
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=20)
        try:
            connection.request("POST", path, canonical(body), {"Content-Type": "application/json",
                               "X-Exemem-Dev-Cert": self._cert, "X-Signature": b64(canonical(envelope))})
            response = connection.getresponse()
            raw = response.read()
            if response.status not in (200, 201):
                raise RuntimeError(f"generated developer publication {path}: HTTP {response.status}: {raw[:500]!r}")
            return json.loads(raw)
        finally:
            connection.close()

    def publish(self, port, bodies):
        registered = set()
        for body in bodies:
            owner = body["app_id"]
            if owner not in registered:
                app = {"app_id": owner, "metadata": {"display_name": owner,
                       "description": "Generated CI app for the actual declaration operation",
                       "homepage_url": "https://example.com/ci"}, "version": "0.1.0"}
                self.request(port, "/v1/apps", "app_register", app, app)
                registered.add(owner)
            definition = dict(body["schema"])
            definition["descriptive_name"] = definition.get("descriptive_name", definition["name"])
            definition["name"] = owner + "/" + definition["name"]
            definition["owner_app_id"] = owner
            definition.pop("alternate_keys", None)
            definition.pop("independent_fields", None)
            self.request(port, "/v1/schemas", "schema_claim", {"schema": definition, "mutation_mappers": {}}, definition)
