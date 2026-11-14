"""Account identity and BIP39 seed decoding for the fixed DEV CI account.

The recovery phrase is BIP39: its 256-bit entropy is the account Ed25519 seed.
The vendored English word list is pinned by its published SHA256. A daemon
started on the derived seed must report the fixed CI account, so a wrong
decode fails before any upload.

This module stays compatible with Python 3.9 for the operator's local use.
"""

import hashlib
from pathlib import Path

ACCOUNT = "2a81c95623f2445854a794b5b48471d6"
WORDS_SHA256 = "2f5eed53a4727b4bf8880d8f3f199efc90e58503646d9ff8eff3a2ed3b24dbda"


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
