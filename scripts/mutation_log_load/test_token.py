"""Offline LastDBNode token checks. No network."""

import hashlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from canon import DOMAIN, canonical_bytes, input_digest
from node_auth import (NodeKey, TokenError, _b64url, account_id, authorization,
                       parse_authorization, verify_authorization, within_skew)


SEED = bytes(range(32))
ISSUED = 1_700_000_000_000
DATABASE = "c1da11d0-load-17"
PATH = "/api/cloud/backups/v2/mutation-log/append"


class LastDBNodeToken(unittest.TestCase):
    def test_digest_frame_matches_the_protocol(self):
        body = b'{"version":2}'
        domain = DOMAIN.encode("utf-8")
        frame = (len(domain).to_bytes(4, "big") + domain
                 + len(body).to_bytes(8, "big") + body)
        self.assertEqual(input_digest(DOMAIN, {"version": 2}),
                         hashlib.sha256(frame).hexdigest())

    def test_header_verifies_and_binds_the_load_database(self):
        key = NodeKey.from_seed(SEED)
        nonce = "ab" * 32
        header, digest = authorization(
            key, "post", PATH, DATABASE, ISSUED, nonce)
        scheme, token = header.split(" ", 1)
        self.assertEqual(scheme, "LastDBNode")
        self.assertEqual(token.count("."), 1)
        body = verify_authorization(header, ISSUED)
        self.assertEqual(body["database_id"], body["stream_id"])
        self.assertEqual(body["stream_id"], DATABASE)
        self.assertEqual(body["account_id"], account_id(key.public))
        self.assertEqual(body["account_id"], hashlib.sha256(key.public).digest()[:16].hex())
        self.assertEqual(body["path"], PATH)
        self.assertEqual(body["method"], "POST")
        self.assertEqual(digest, input_digest(DOMAIN, body))
        self.assertTrue(within_skew(ISSUED, ISSUED + 60_000))
        self.assertFalse(within_skew(ISSUED, ISSUED + 60_001))

    def test_a_mismatched_database_is_refused(self):
        key = NodeKey.from_seed(SEED)
        header, _digest = authorization(key, "POST", PATH, DATABASE, ISSUED, "cd" * 32)
        body, _signature = parse_authorization(header)
        body["database_id"] = "c1da11d0-load-18"
        forged = "LastDBNode " + _b64url(canonical_bytes(body)) + "." + header.split(".", 1)[1]
        with self.assertRaises(TokenError):
            verify_authorization(forged, ISSUED)


if __name__ == "__main__":
    unittest.main()
