"""Deterministic private content of the fixed CI baseline.

A native format change moves the CI baseline to a new stream. The baseline
operation and the operator who saves the protected baseline secret must make
the same 16 notes and the same 8 MiB file without a transfer of either value.
Both derive them from the account recovery phrase and the public stream ID.

The recovery phrase is BIP39: its 256-bit entropy is the account Ed25519 seed.
The vendored English word list is pinned by its published SHA256. A daemon
started on the derived seed must report the fixed CI account, so a wrong
decode fails before any upload.

This module stays compatible with Python 3.9 for the operator's local use.
"""

import hashlib
import hmac
import json
from pathlib import Path
import re

ACCOUNT = "2a81c95623f2445854a794b5b48471d6"
FILE_BYTES = 8 * 1024 * 1024
WORDS_SHA256 = "2f5eed53a4727b4bf8880d8f3f199efc90e58503646d9ff8eff3a2ed3b24dbda"
SCHEMA = "CloudCiBaselineF7"
RECORD_IDS = tuple("ci-baseline-{:02d}".format(number) for number in range(16))
DOMAIN = b"lastdb:cloud-ci-baseline:v1"
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class BaselineDataError(Exception):
    """A fixed failure code. It never carries a secret value."""


def check(condition, code):
    if not condition:
        raise BaselineDataError(code)


def words():
    path = Path(__file__).resolve().with_name("bip39_english.txt")
    raw = path.read_bytes()
    check(hashlib.sha256(raw).hexdigest() == WORDS_SHA256, "baseline_word_list_digest")
    values = raw.decode("ascii").split("\n")[:-1]
    check(len(values) == 2048 and len(set(values)) == 2048, "baseline_word_list_shape")
    return {word: index for index, word in enumerate(values)}


def seed(phrase):
    """Return the 32-byte Ed25519 seed of a 24-word phrase."""
    check(isinstance(phrase, bytes) and 0 < len(phrase) <= 4096, "baseline_phrase_size")
    try:
        items = phrase.decode("ascii").split()
    except UnicodeDecodeError:
        raise BaselineDataError("baseline_phrase_text") from None
    index = words()
    check(len(items) == 24 and all(item in index for item in items), "baseline_phrase_words")
    bits = 0
    for item in items:
        bits = (bits << 11) | index[item]
    entropy = (bits >> 8).to_bytes(32, "big")
    check(bits & 0xFF == hashlib.sha256(entropy).digest()[0], "baseline_phrase_checksum")
    return entropy


def content_key(identity_seed):
    check(isinstance(identity_seed, bytes) and len(identity_seed) == 32, "baseline_seed_size")
    return hmac.new(identity_seed, DOMAIN, hashlib.sha256).digest()


def stream(stream_id):
    check(isinstance(stream_id, str) and UUID.fullmatch(stream_id) is not None, "baseline_stream_id")
    return stream_id.encode("ascii")


def records(key, stream_id):
    selected = stream(stream_id)
    result = []
    for number, identifier in enumerate(RECORD_IDS):
        label = b"note:" + selected + b":" + str(number).encode("ascii")
        note = hmac.new(key, label, hashlib.sha256).hexdigest()[:32]
        result.append({"id": identifier, "note": "Cloud CI baseline " + note})
    return result


def file_bytes(key, stream_id):
    return hashlib.shake_256(key + b"file:" + stream(stream_id)).digest(FILE_BYTES)


def secret(key, stream_id, account_generation, commit_id, commit_sha256, file_sha256):
    """Return the exact LASTDB_CLOUD_DEV_BASELINE value."""
    check(isinstance(commit_id, str) and UUID.fullmatch(commit_id) is not None, "baseline_commit_id")
    for value in (commit_sha256, file_sha256):
        check(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None,
              "baseline_digest")
    check(isinstance(account_generation, str)
          and re.fullmatch(r"0|[1-9][0-9]{0,19}", account_generation) is not None, "baseline_generation")
    return {"account": ACCOUNT, "stream_id": stream_id, "account_generation": account_generation,
            "commit_id": commit_id, "commit_sha256": commit_sha256, "schema": SCHEMA,
            "records": records(key, stream_id), "file_bytes": FILE_BYTES, "file_sha256": file_sha256}


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def commitment(value):
    """A public check value. The notes keep 128 bits of key-derived entropy each."""
    return hashlib.sha256(b"lastdb:cloud-ci-baseline-secret:v1\n" + encode(value)).hexdigest()
