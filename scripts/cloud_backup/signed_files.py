"""Prove signed Files with an owned DEV database and two generated daemon homes."""

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import secrets
import tempfile
import time
import uuid

import baseline_data
from node import OwnedNode
from programs import Programs
from support import OperationError, dev_endpoint, failure, require, write_private


@contextmanager
def running(node, *, remove_saved=False):
    try:
        node.start()
        yield
        node.stop(remove_saved=remove_saved)
    finally:
        node.cleanup_stop()


def retry(stage, command, complete):
    deadline = time.monotonic() + 150
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = command()
        except OperationError as error:
            last_error = error
        else:
            if complete(result):
                return result
        time.sleep(2)
    raise OperationError(stage, cli_refusal=last_error.cli_refusal if last_error else None)


def summary(message):
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
        output.write(message + "\n")


def transfer(programs, root, publisher, recipient, api_key):
    owner = lambda *args, **kw: programs.command(publisher.home, list(args), timeout=20, **kw)
    reader = lambda *args: programs.command(recipient.home, list(args), timeout=20)
    contact_path = root / "reader.contact"
    invite_path = root / "share.invitation"
    source = root / "fresh-file.bin"
    payload = secrets.token_bytes(2 * 32768 + 17)
    digest = hashlib.sha256(payload).hexdigest()
    write_private(source, payload)
    # Create locally before giving the publisher cloud credentials. The UUID is
    # recorded for exact DEV cleanup even if the first cloud operation fails.
    name = f"ci-files-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    made = owner("db", "create", name, "--spec-name", "lastdb-files", "--spec-version", "1", "--json")
    database = str(uuid.UUID(made["id"], version=4))
    require(database == made["id"] and made["name"] == name, "files_created_database")
    summary(f"Owned DEV mutation log `{database}` for `{name}`.")
    print(f"Signed Files DEV database {database}", flush=True)
    selected = ("--db", database, "share", "files")
    manifest = owner(*selected, "add", str(source))
    require(manifest["sha256"] == digest and manifest["size_bytes"] == len(payload)
            and manifest["chunks"] == 3, "files_import_receipt")
    connected = owner("backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
                      "--expected-user-hash", baseline_data.ACCOUNT, "--json", input_bytes=api_key)
    require(connected["ok"] is True and connected["credentials_saved"] is True
            and connected["registration_attempted"] is False
            and connected["account_id"] == baseline_data.ACCOUNT, "files_dev_connection")
    contact = reader("share", "contact", "--out", str(contact_path))
    # The canonical DEV create is asynchronous. The same member update can be
    # retried until the daemon enrolls the named database.
    invite = retry("files_invite_deadline", lambda: owner(
        "--db", database, "share", "invite", "--recipient", str(contact_path),
        "--recipient-key", contact["recipient_public_key"], "--out", str(invite_path)),
        lambda receipt: receipt.get("database_id") == database and receipt.get("committed") is True)
    require(invite_path.is_file() and invite["invitation"] == str(invite_path), "files_signed_invite")
    publisher_key = owner("share", "contact", "--out", str(root / "publisher.contact"))
    received = reader("share", "files", "receive", str(invite_path), "--publisher",
                      publisher_key["recipient_public_key"], "--name", "download")
    row = received["database"]
    require(row["id"] == database and row["kind"] == "received" and row["access"] == "reader", "files_signed_receive")
    retry("files_download_deadline", lambda: reader("--db", "download", "share", "files", "list"),
          lambda listing: listing.get("database") == database and listing.get("files") == [manifest])
    output = root / "saved-file.bin"
    saved = reader("--db", "download", "share", "files", "save", "--file", manifest["id"],
                   "--out", str(output))
    require(saved["database"] == database and output.read_bytes() == payload, "files_download_bytes")
    message = f"Signed Files upload and download matched all {len(payload)} bytes. SHA-256 {digest}."
    print(message, flush=True)
    summary(message)


def main(operation):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true"
            and os.environ.get("GITHUB_REF") == "refs/heads/main", "files_accepted_ci_only")
    dev_endpoint()
    phrase = os.environ.pop("LASTDB_CLOUD_DEV_RECOVERY_PHRASE").strip().encode()
    api_key = os.environ.pop("LASTDB_CLOUD_DEV_API_KEY").encode()
    require(0 < len(api_key) <= 4096, "files_dev_api_key")
    programs = Programs(args.bin_dir.resolve(strict=True), args.version, dict(os.environ))
    with tempfile.TemporaryDirectory(prefix="ldb-files-", dir="/tmp") as temporary:
        root = Path(temporary).resolve()
        publisher = OwnedNode(root / "publisher", programs)
        publisher.home.mkdir(mode=0o700)
        write_private(publisher.home / "identity.key", baseline_data.seed(phrase))
        del phrase
        recipient_home = root / "recipient"
        initialized = programs.command(None, [
            "admin", "backup", "init", "--into", str(recipient_home),
            "--recovery-phrase-out", str(recipient_home / "recovery-phrase.txt"), "--json",
        ], timeout=20)
        recipient = OwnedNode(recipient_home, programs, account=initialized["account_id"])
        with running(publisher, remove_saved=True), running(recipient):
            operation(programs, root, publisher, recipient, api_key)
    message = "Both generated DEV daemons stopped cleanly and their temporary homes were removed."
    print(message, flush=True)
    summary(message)


def run_operation(operation):
    try:
        main(operation)
    except OperationError as error:
        print(json.dumps(failure(error)), flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    run_operation(transfer)
