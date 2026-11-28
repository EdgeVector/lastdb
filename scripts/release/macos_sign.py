"""Developer ID signing and Apple notarization of the matched macOS release programs."""

from __future__ import annotations

import argparse
import base64
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

ENTITLEMENTS = Path(__file__).with_name("entitlements.plist")
NOTARY_SECONDS = 3600


def run_quiet(cmd, *, timeout, what, env=None):
    """Run a command whose arguments may carry a secret. Show output, never the arguments."""
    result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.returncode != 0:
        if result.stderr.strip():
            print(result.stderr.strip(), file=sys.stderr)
        raise RuntimeError(f"{what} failed with exit status {result.returncode}")
    return result.stdout


def apple_credentials():
    names = ("APPLE_CERTIFICATE", "APPLE_CERTIFICATE_PASSWORD", "APPLE_SIGNING_IDENTITY",
             "APPLE_ID", "APPLE_PASSWORD", "APPLE_TEAM_ID")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Developer ID signing needs these environment secrets of `node-release`: "
                           + ", ".join(missing))
    return {name: os.environ[name] for name in names}


def notary_credentials():
    """Return only the credentials that authenticate a notary-service request."""
    names = ("APPLE_ID", "APPLE_PASSWORD", "APPLE_TEAM_ID")
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise RuntimeError("Apple notary preflight needs these `node-release` environment secrets: "
                           + ", ".join(missing))
    return {name: os.environ[name] for name in names}


def notary_preflight():
    """Confirm that the configured account can reach Apple's notary service."""
    credentials = notary_credentials()
    auth = ["--apple-id", credentials["APPLE_ID"], "--password", credentials["APPLE_PASSWORD"],
            "--team-id", credentials["APPLE_TEAM_ID"]]
    result = subprocess.run(
        ["xcrun", "notarytool", "history", *auth, "--output-format", "json"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode == 0:
        try:
            json.loads(result.stdout)
        except ValueError as error:
            raise RuntimeError("Apple notary preflight returned an invalid response") from error
        print("Apple notary preflight succeeded; the configured account can submit notarizations.")
        return

    response = f"{result.stdout}\n{result.stderr}".lower()
    agreement_words = ("agreement", "license", "contract")
    if "403" in response or any(word in response for word in agreement_words):
        raise RuntimeError(
            "Apple notary preflight failed: the Apple Developer Program agreement for the configured "
            "team is missing or expired. An Account Holder must accept the current agreement in Apple "
            "Developer before rerunning this release. The `node-release` Apple secrets stay unchanged "
            "unless the account owner rotates them."
        )
    raise RuntimeError(
        "Apple notary preflight could not authenticate the configured `node-release` account. "
        "An owner must verify the Apple ID, app-specific password, team access, and current agreements "
        "without printing or rotating secrets in workflow logs."
    )


@contextmanager
def signing_keychain(credentials):
    """A temporary keychain holding the Developer ID identity; removed on exit."""
    with tempfile.TemporaryDirectory(prefix="lastdb-sign-") as directory:
        directory = Path(directory)
        keychain = directory / "release.keychain-db"
        password = hashlib.sha256(os.urandom(32)).hexdigest()
        certificate = directory / "identity.p12"
        before = subprocess.check_output(["security", "list-keychains", "-d", "user"], text=True, timeout=30)
        listed = [line.strip().strip('"') for line in before.splitlines() if line.strip()]
        try:
            certificate.write_bytes(base64.b64decode(credentials["APPLE_CERTIFICATE"]))
            certificate.chmod(0o600)
            run_quiet(["security", "create-keychain", "-p", password, str(keychain)], timeout=60,
                      what="Creating the temporary keychain")
            run_quiet(["security", "set-keychain-settings", "-lut", "7200", str(keychain)], timeout=60,
                      what="Configuring the temporary keychain")
            run_quiet(["security", "unlock-keychain", "-p", password, str(keychain)], timeout=60,
                      what="Unlocking the temporary keychain")
            run_quiet(["security", "import", str(certificate), "-k", str(keychain),
                       "-P", credentials["APPLE_CERTIFICATE_PASSWORD"], "-f", "pkcs12",
                       "-T", "/usr/bin/codesign", "-T", "/usr/bin/security"], timeout=60,
                      what="Importing the Developer ID identity")
            run_quiet(["security", "set-key-partition-list", "-S", "apple-tool:,apple:,codesign:",
                       "-s", "-k", password, str(keychain)], timeout=60,
                      what="Allowing codesign to use the Developer ID key")
            run_quiet(["security", "list-keychains", "-d", "user", "-s", str(keychain), *listed], timeout=60,
                      what="Adding the temporary keychain to the search list")
            identities = subprocess.check_output(
                ["security", "find-identity", "-v", "-p", "codesigning", str(keychain)], text=True, timeout=60)
            if credentials["APPLE_SIGNING_IDENTITY"] not in identities:
                raise RuntimeError("The imported certificate does not hold the configured signing identity")
            yield keychain
        finally:
            subprocess.run(["security", "list-keychains", "-d", "user", "-s", *listed],
                           check=False, timeout=60)
            subprocess.run(["security", "delete-keychain", str(keychain)], check=False, timeout=60)
            if certificate.exists():
                certificate.unlink()


def notarize(paths, credentials):
    """Submit the matched pair as one zip and wait for Apple's verdict."""
    with tempfile.TemporaryDirectory(prefix="lastdb-notary-") as directory:
        directory = Path(directory)
        staging = directory / "pair"
        staging.mkdir()
        for path in paths:
            (staging / path.name).write_bytes(path.read_bytes())
        archive = directory / "lastdb-notarize.zip"
        subprocess.run(["ditto", "-c", "-k", "--norsrc", "--noextattr", str(staging), str(archive)],
                       check=True, timeout=300)
        auth = ["--apple-id", credentials["APPLE_ID"], "--password", credentials["APPLE_PASSWORD"],
                "--team-id", credentials["APPLE_TEAM_ID"]]
        print("Submitting the CLI and daemon to the Apple notary service (waiting for its verdict)")
        result = subprocess.run(
            ["xcrun", "notarytool", "submit", str(archive), *auth, "--wait",
             "--timeout", f"{NOTARY_SECONDS}s", "--output-format", "json"],
            capture_output=True, text=True, timeout=NOTARY_SECONDS + 300)
        try:
            verdict = json.loads(result.stdout)
        except ValueError:
            verdict = {}
        submission, status = verdict.get("id"), verdict.get("status")
        print(f"Notary submission {submission}: {status}")
        if result.returncode != 0 or status != "Accepted":
            if submission:
                log = subprocess.run(["xcrun", "notarytool", "log", submission, *auth],
                                     capture_output=True, text=True, timeout=300)
                print(log.stdout.strip())
            # The notary client names Apple's refusal (for example an expired agreement) here.
            print(verdict.get("message") or result.stderr.strip(), file=sys.stderr)
            raise RuntimeError("Apple did not accept the notarization submission")


def developer_id_sign(paths, credentials):
    """Sign with the hardened runtime and a secure timestamp, then notarize the pair."""
    with signing_keychain(credentials) as keychain:
        for path in paths:
            subprocess.run(["codesign", "--force", "--options", "runtime", "--timestamp",
                            "--entitlements", str(ENTITLEMENTS), "--keychain", str(keychain),
                            "--sign", credentials["APPLE_SIGNING_IDENTITY"], str(path)],
                           check=True, timeout=300)
            subprocess.run(["codesign", "--verify", "--strict", "--verbose=2", str(path)],
                           check=True, timeout=60)
            details = subprocess.run(["codesign", "--display", "--verbose=4", str(path)],
                                     capture_output=True, text=True, timeout=60).stderr
            if ("runtime" not in details or "Timestamp=" not in details
                    or f"TeamIdentifier={credentials['APPLE_TEAM_ID']}" not in details):
                raise RuntimeError(f"{path.name} lacks the hardened runtime, a timestamp, or the team identity")
    notarize(paths, credentials)
    # Gatekeeper reads the notarization ticket online for a bare executable, which may lag a few seconds.
    # `--type execute` only assesses app bundles and always rejects a bare Mach-O ("does not seem to
    # be an app"); `--type open` with the primary-signature context assesses the binary itself.
    for path in paths:
        for attempt in range(6):
            check = subprocess.run(["spctl", "--assess", "--type", "open", "--context",
                                    "context:primary-signature", "-vv", str(path)],
                                   capture_output=True, text=True, timeout=120)
            print((check.stdout + check.stderr).strip())
            if check.returncode == 0 and "Notarized Developer ID" in check.stdout + check.stderr:
                break
            time.sleep(10)
        else:
            raise RuntimeError(f"Gatekeeper does not accept {path.name} as a notarized Developer ID program")


def main():
    parser = argparse.ArgumentParser(description="Developer ID signing helpers")
    parser.add_argument("command", choices=("preflight",))
    args = parser.parse_args()
    if args.command == "preflight":
        notary_preflight()


if __name__ == "__main__":
    main()
