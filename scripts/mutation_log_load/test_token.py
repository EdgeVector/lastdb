"""Offline LastDBNode token checks. No network and no AWS."""

import hashlib
import json
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from guard import database_id_for_run
from node_auth import (
    DEV_ENVIRONMENT, HEADER_LIMIT, MEMBER_LIST_REVISION, NodeKey, TokenError,
    account_id, authorization, encode_request, parse_authorization,
    verify_authorization, within_skew,
)
from routes import request_body


DOMAIN = "lastdb:share:node-read-authority:v1"
SEED = bytes(range(32))
ISSUED = 1_700_000_000_000
PATH = "/api/cloud/backups/v2/mutation-log/append"
NONCE = "ab" * 32


def setUpModule():
    root = Path(__file__).resolve().parents[2]
    offline = [
        "cargo", "build", "--locked", "--offline", "-p", "cloud_backup_protocol",
        "--bin", "node_read_token",
    ]
    online = ["cargo", "build", "--locked", "-p", "cloud_backup_protocol", "--bin", "node_read_token"]
    if subprocess.run(offline, cwd=root).returncode != 0:
        built = subprocess.run(online, cwd=root)
        if built.returncode != 0:
            raise RuntimeError("node_read_token did not build")


def frame(domain, body):
    domain_bytes = domain.encode("ascii")
    return (len(domain_bytes).to_bytes(4, "big") + domain_bytes
            + len(body).to_bytes(8, "big") + body)


class LastDBNodeToken(unittest.TestCase):
    def setUp(self):
        self.key = NodeKey.from_seed(SEED)
        self.database = database_id_for_run("17")
        self.body = request_body("append", {
            "database_id": self.database,
            "device_id": "full5-d0",
            "entries": [{
                "authored_at_ms": ISSUED,
                "namespace": "load",
                "payload": "load",
                "writer_seq": 1,
            }],
        })

    def test_digest_input_order_matches_the_protocol_tuple(self):
        header, encoded = authorization(
            self.key, PATH, self.database, ISSUED, self.body, NONCE)
        token = json.loads(encoded["token_json"])
        covered = json.loads(encoded["canonical"])
        self.assertEqual(set(token), {
            "version", "environment", "stream", "method", "path", "issued_at_ms",
            "nonce", "member_list_revision", "node_public_key",
        })
        self.assertEqual(token["version"], 2)
        self.assertEqual(token["environment"], DEV_ENVIRONMENT)
        self.assertEqual(token["method"], "POST")
        self.assertEqual(token["path"], PATH)
        self.assertEqual(token["issued_at_ms"], str(ISSUED))
        self.assertIsInstance(token["issued_at_ms"], str)
        self.assertEqual(token["nonce"], NONCE)
        self.assertEqual(token["member_list_revision"], MEMBER_LIST_REVISION)
        self.assertEqual(token["node_public_key"], self.key.public.hex())
        self.assertEqual(token["stream"]["stream_id"], self.database)
        self.assertEqual(
            token["stream"]["scope"]["account_id"], account_id(self.key.public))
        self.assertEqual(
            token["stream"]["scope"]["account_id"],
            hashlib.sha256(self.key.public).digest()[:16].hex())
        self.assertEqual(token["stream"]["scope"]["account_generation"], "1")
        self.assertEqual(covered[0], "POST")
        self.assertEqual(covered[1], PATH)
        self.assertEqual(covered[2], self.database)
        self.assertEqual(covered[3], str(ISSUED))
        self.assertEqual(covered[4], hashlib.sha256(self.body).hexdigest())
        self.assertEqual(covered[5], 2)
        self.assertEqual(covered[6]["stream_id"], self.database)
        self.assertEqual(covered[6]["scope"]["account_id"], account_id(self.key.public))
        self.assertEqual(covered[7], NONCE)
        self.assertEqual(covered[8], MEMBER_LIST_REVISION)
        self.assertEqual(covered[9], self.key.public.hex())
        self.assertEqual(len(covered), 10)
        digest = hashlib.sha256(frame(DOMAIN, encoded["canonical"].encode("utf-8"))).hexdigest()
        self.assertEqual(encoded["digest"], digest)
        self.assertTrue(header.startswith("LastDBNode "))
        self.assertFalse(header.startswith("LastDBShare"))
        self.assertLessEqual(len(header.encode("ascii")), HEADER_LIMIT)
        self.assertEqual(header.split(" ", 1)[1].count("."), 1)
        verified = verify_authorization(header, ISSUED, self.body)
        self.assertEqual(verified["stream"]["stream_id"], self.database)
        self.assertTrue(within_skew(ISSUED, ISSUED + 60_000))
        self.assertFalse(within_skew(ISSUED, ISSUED + 60_001))

    def test_a_changed_body_or_token_field_breaks_the_signature(self):
        header, _encoded = authorization(
            self.key, PATH, self.database, ISSUED, self.body, NONCE)
        public = Ed25519PublicKey.from_public_bytes(self.key.public)
        body, signature = parse_authorization(header)
        public.verify(signature, _encoded["digest"].encode("ascii"))
        with self.assertRaises(InvalidSignature):
            verify_authorization(header, ISSUED, self.body + b" ")
        body["path"] = PATH + "-other"
        forged = "LastDBNode " + _b64(json.dumps(body).encode("utf-8")) + "." + header.split(".", 1)[1]
        with self.assertRaises(InvalidSignature):
            verify_authorization(forged, ISSUED, self.body)
        with self.assertRaises(TokenError):
            parse_authorization("LastDBShare " + header.split(" ", 1)[1])

    def test_the_helper_rejects_a_mismatched_account(self):
        with self.assertRaises(TokenError):
            encode_request({
                "environment": DEV_ENVIRONMENT,
                "account_id": "ab" * 16,
                "account_generation": "1",
                "stream_id": self.database,
                "path": PATH,
                "issued_at_ms": str(ISSUED),
                "nonce": NONCE,
                "member_list_revision": "0",
                "node_public_key": self.key.public.hex(),
                "body": "",
            })


def _b64(data):
    import base64
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


if __name__ == "__main__":
    unittest.main()
