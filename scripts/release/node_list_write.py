"""Sign and upload one list, then read it back and verify it. Used by node_list.py."""

from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import time

from node_list_read import node_index, read_pair, read_verified, s3_location, verify_pair
from node_list_rules import ListError, canonical, check_list, fmt, now_utc

SPKI_ED25519_PREFIX = bytes.fromhex("302a300506032b6570032100")


def aws(*args):
    try:
        done = subprocess.run(["aws", *args], check=True, capture_output=True, text=True, timeout=120)
    except subprocess.CalledProcessError as error:
        raise ListError(f"aws {args[0]} {args[1]} failed: {error.stderr.strip()[:500]}") from error
    return done.stdout.strip()


def kms_public_key(key_id, region):
    der = base64.b64decode(aws("kms", "get-public-key", "--key-id", key_id, "--region", region,
                               "--query", "PublicKey", "--output", "text"))
    if len(der) != 44 or not der.startswith(SPKI_ED25519_PREFIX):
        raise ListError("The KMS key is not an Ed25519 key")
    return der[12:]


def kms_sign(key_id, region, hex_digest):
    with tempfile.TemporaryDirectory() as folder:
        message = Path(folder) / "message"
        message.write_bytes(hex_digest.encode("ascii"))
        return aws("kms", "sign", "--key-id", key_id, "--region", region,
                   "--message", f"fileb://{message}", "--message-type", "RAW",
                   "--signing-algorithm", "ED25519_SHA_512", "--query", "Signature", "--output", "text")


def put_object(bucket, region, key, data):
    with tempfile.TemporaryDirectory() as folder:
        body = Path(folder) / "body"
        body.write_bytes(data)
        aws("s3api", "put-object", "--bucket", bucket, "--region", region, "--key", key,
            "--body", str(body), "--content-type", "application/json", "--cache-control", "max-age=60")


def write_list(args, channel, target, doc, keys):
    """Sign and upload one list, then read it back from the public URL and verify it."""
    index = node_index(args.env)
    bucket, bucket_region, prefix = s3_location(index)
    region = args.region or os.environ.get("AWS_REGION") or bucket_region
    key_id = args.kms_key or f"alias/lastdb-node-release-{args.env}"
    check_list(doc, channel, target)
    raw = canonical(doc)
    digest = hashlib.sha256(raw).hexdigest()
    public = kms_public_key(key_id, region)
    if public not in keys:
        raise ListError("The KMS signing key is not one of the pinned node_release_keys")
    envelope = {"alg": "ed25519", "key_id": hashlib.sha256(public).hexdigest(),
                "payload_sha256": digest, "sig": kms_sign(key_id, region, digest)}
    sig_raw = canonical(envelope)
    verify_pair(raw, sig_raw, keys, channel, target)  # refuse to upload what would not verify
    path = f"{prefix}/{channel}/{target}.json"
    put_object(bucket, bucket_region, path, raw)
    put_object(bucket, bucket_region, path + ".sig", sig_raw)
    last = None
    for _ in range(5):
        got_raw, got_sig = read_pair(index, channel, target)
        if got_raw == raw and got_sig == sig_raw:
            verify_pair(got_raw, got_sig, keys, channel, target)
            return
        last = "the downloaded bytes differ from the uploaded bytes"
        time.sleep(2)
    raise ListError(f"{channel}/{target}: read-back failed: {last}")


def next_doc(current, channel, target, releases):
    return {"list_version": 1, "kind": "lastdb-node", "channel": channel, "target": target,
            "generated_at": fmt(now_utc()), "seq": (current["seq"] if current else 0) + 1,
            "releases": releases}


def load_current(args, channel, target, keys):
    found = read_verified(node_index(args.env), channel, target, keys)
    return found[0] if found else None


def commit(args, channel, target, current, releases, keys, what):
    doc = next_doc(current, channel, target, releases)
    if args.dry_run:
        print(f"[dry-run] {args.env} {channel}/{target}: {what}; would write seq {doc['seq']}")
        return
    write_list(args, channel, target, doc, keys)
    print(f"{args.env} {channel}/{target}: {what}; wrote seq {doc['seq']} and verified it")
