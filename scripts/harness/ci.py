"""GitHub Actions summary, SHA-256, and private-file I/O helpers."""

import hashlib
import json
import os
from pathlib import Path

CHUNK_BYTES = 1024 * 1024


def append_step_summary(text):
    """Append markdown text to `$GITHUB_STEP_SUMMARY` when running in GitHub Actions."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(text)


def summary_line(line, *, echo=True):
    """Optionally print one line and append it to `$GITHUB_STEP_SUMMARY` if set."""
    if echo:
        print(line, flush=True)
    append_step_summary(line + "\n")


def file_sha256(path):
    """Compute the SHA-256 hex digest of `path` in 1 MiB chunks."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_json(path, value):
    """Write deterministically sorted, indented JSON to `path`."""
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_private(path, payload):
    """Create a mode-0600 file exclusively without following symlinks and fsync its contents."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
