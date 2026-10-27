#!/usr/bin/env python3
"""Fail when a tracked text file contains a high-confidence secret.

The required CI gate runs this scan. It reads the git index only. It does
not walk the home directory and it does not use the network.

A match prints the path, the line number, and the rule name. It does not
print the secret. Skip one fixture line with `secret-scan-ok:` and a reason
on that same line.
"""

import os
import re
import subprocess
import sys

RULES = (
    (
        "private-key",
        re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA |PGP )?PRIVATE KEY-----"),
    ),
    (
        "github-token",
        re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}\b"),
    ),
    (
        "github-pat",
        re.compile(r"\bgithub_pat_[A-Za-z0-9_]{22,}\b"),
    ),
    (
        "aws-access-key",
        re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    ),
    (
        "slack-token",
        re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}"),
    ),
    (
        "slack-webhook",
        re.compile(
            r"https://hooks\.slack\.com/services/T[A-Z0-9]+/B[A-Z0-9]+/[A-Za-z0-9]{10,}"
        ),
    ),
    (
        "stripe-live-key",
        re.compile(r"\b(?:sk|rk)_live_[A-Za-z0-9]{16,}\b"),
    ),
    (
        "google-api-key",
        re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    ),
    (
        "npm-token",
        re.compile(r"\bnpm_[A-Za-z0-9]{36}\b"),
    ),
    (
        "sentry-dsn",
        re.compile(
            r"https://[0-9a-f]{32}@(?:[a-z0-9-]+\.)?ingest(?:\.[a-z]{2})?\.sentry\.io"
        ),
    ),
)

OVERRIDE = "secret-scan-ok:"
MAX_FINDINGS = 50
BINARY_SUFFIXES = (
    ".gz",
    ".tgz",
    ".tar",
    ".zip",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
    ".ico",
    ".pdf",
    ".woff",
    ".woff2",
    ".wasm",
    ".so",
    ".dylib",
    ".a",
    ".o",
)


def git_files():
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        check=False,
        capture_output=True,
    )
    if out.returncode != 0:
        sys.stderr.write(out.stderr.decode("utf-8", "replace"))
        sys.exit("lint-secrets: git ls-files failed")
    return [path.decode("utf-8", "surrogateescape") for path in out.stdout.split(b"\0") if path]


def is_binary_sample(sample):
    if b"\0" in sample:
        return True
    return sample.startswith(b"\x1f\x8b") or sample.startswith(b"\xfd7zXZ")


def scan_file(path, findings):
    lower = path.lower()
    if lower.endswith(BINARY_SUFFIXES):
        return
    try:
        handle = open(path, "rb")
    except OSError as exc:
        sys.exit(f"lint-secrets: cannot read {path}: {exc}")
    with handle:
        sample = handle.read(8192)
        if is_binary_sample(sample):
            return
        data = sample + handle.read()
    text = data.decode("utf-8", "replace")
    for number, line in enumerate(text.splitlines(), 1):
        if OVERRIDE in line:
            continue
        for name, pattern in RULES:
            if pattern.search(line):
                findings.append((path, number, name))
                if len(findings) >= MAX_FINDINGS:
                    return


def main():
    root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        check=False,
        capture_output=True,
        text=True,
    )
    if root.returncode != 0:
        sys.exit("lint-secrets: not inside a git checkout")
    os.chdir(root.stdout.strip())
    findings = []
    for path in git_files():
        scan_file(path, findings)
        if len(findings) >= MAX_FINDINGS:
            break
    if not findings:
        print("lint-secrets: no high-confidence secrets in tracked text files")
        return
    print(f"lint-secrets: {len(findings)} finding(s). Values are not printed.", file=sys.stderr)
    for path, number, name in findings:
        print(f"{path}:{number}: {name}", file=sys.stderr)
    if len(findings) >= MAX_FINDINGS:
        print("lint-secrets: finding list stopped at the cap", file=sys.stderr)
    sys.exit(1)


if __name__ == "__main__":
    main()
