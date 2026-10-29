#!/usr/bin/env python3
"""Manage one disposable AWS bucket from an approved main-branch CI job only."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import uuid


ACCOUNT = "152335099025"
REGION = "us-west-2"
REPOSITORY = "EdgeVector/lastdb"
ROLE = "lastdb-schema-checks-ci"
ALLOWED_KEYS = {"definitions.json", "direct-cas"}
MAX_RESPONSE_BYTES = 1024 * 1024


class BucketError(Exception):
    """Refuse a resource operation whose ownership or scope is uncertain."""


@dataclass(frozen=True)
class Context:
    run_id: str
    attempt: str
    bucket: str
    temporary: Path
    receipt: Path


def context(output):
    required = {
        "GITHUB_ACTIONS": "true",
        "RUNNER_ENVIRONMENT": "github-hosted",
        "RUNNER_OS": "Linux",
        "GITHUB_SERVER_URL": "https://github.com",
        "GITHUB_REPOSITORY": REPOSITORY,
        "GITHUB_REF": "refs/heads/main",
        "AWS_REGION": REGION,
    }
    if any(os.environ.get(key) != value for key, value in required.items()):
        raise BucketError("Require the GitHub-hosted Linux main-branch job in the approved region")
    if any(value for key, value in os.environ.items() if key.startswith("AWS_ENDPOINT_URL")):
        raise BucketError("Disposable AWS checks refuse endpoint overrides")
    run_id, attempt = os.environ.get("GITHUB_RUN_ID", ""), os.environ.get("GITHUB_RUN_ATTEMPT", "")
    if not re.fullmatch(r"[1-9][0-9]{0,19}", run_id) or not re.fullmatch(r"[1-9][0-9]{0,5}", attempt):
        raise BucketError("Require numeric GitHub run and attempt identities")
    temporary = Path(os.environ["RUNNER_TEMP"])
    if not temporary.is_absolute() or not temporary.is_dir():
        raise BucketError("RUNNER_TEMP must be an existing absolute directory")
    temporary = temporary.resolve(strict=True)
    workspace = Path(os.environ["GITHUB_WORKSPACE"]).resolve(strict=True)
    if temporary == workspace or temporary.is_relative_to(workspace):
        raise BucketError("Ownership receipts must stay outside the source checkout")
    if not output.is_absolute() or output.is_symlink():
        raise BucketError("The receipt directory must be an absolute path without a symlink")
    folder = output.resolve(strict=False)
    if folder == temporary or not folder.is_relative_to(temporary):
        raise BucketError("The receipt directory must stay strictly under RUNNER_TEMP")
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    if folder.stat().st_uid != os.getuid():
        raise BucketError("The receipt directory belongs to another owner")
    bucket = f"lastdb-schema-ci-{ACCOUNT}-{run_id}-{attempt}"
    if len(bucket) > 63:
        raise BucketError("The derived bucket name exceeds the S3 limit")
    receipt = folder / f"aws-bucket-{run_id}-{attempt}.json"
    if receipt.is_symlink():
        raise BucketError("The ownership receipt cannot be a symlink")
    return Context(run_id, attempt, bucket, temporary, receipt)


def aws(service, operation, *arguments):
    command = [
        "aws", "--region", REGION, "--output", "json", "--no-cli-pager", "--no-paginate",
        "--cli-connect-timeout", "10", "--cli-read-timeout", "20",
        service, operation, *arguments,
    ]
    environment = dict(os.environ, AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off",
                       AWS_MAX_ATTEMPTS="1", AWS_IGNORE_CONFIGURED_ENDPOINT_URLS="true")
    # Separate temporary streams prevent stderr from entering the JSON parser.
    # Neither stream is printed or retained after this bounded command.
    temporary = os.environ["RUNNER_TEMP"]
    with tempfile.TemporaryFile(dir=temporary) as output, tempfile.TemporaryFile(dir=temporary) as errors:
        result = subprocess.run(command, shell=False, stdout=output, stderr=errors,
                                env=environment, timeout=45, check=False)
        output.seek(0)
        body = output.read(MAX_RESPONSE_BYTES + 1)
        errors.seek(0)
        error_text = errors.read(16384).decode("utf-8", errors="replace")
    if result.returncode:
        match = re.search(r"An error occurred \(([A-Za-z0-9]{1,64})\)", error_text)
        code = match[1] if match else f"exit {result.returncode}"
        raise BucketError(f"AWS {service} {operation} failed ({code}); no existing bucket is adopted")
    if len(body) > MAX_RESPONSE_BYTES:
        raise BucketError(f"AWS {operation} response exceeded its bound")
    value = json.loads(body) if body.strip() else {}
    if not isinstance(value, dict):
        raise BucketError(f"AWS {operation} returned an unexpected JSON shape")
    return value


def identity():
    result = aws("sts", "get-caller-identity")
    expected = rf"arn:aws:sts::{ACCOUNT}:assumed-role/{ROLE}/[^/]+"
    if result.get("Account") != ACCOUNT or not re.fullmatch(expected, result.get("Arn", "")):
        raise BucketError("Require the dedicated CI role in the approved AWS account")
    return result["Arn"]


def tags(ctx, receipt):
    return {
        "schema-check-owner": receipt["owner_token"],
        "schema-check-repository": REPOSITORY,
        "schema-check-run": ctx.run_id,
        "schema-check-attempt": ctx.attempt,
    }


def file_command(ctx, variable):
    path = Path(os.environ[variable])
    if not path.is_absolute() or path.is_symlink() or not path.resolve().is_relative_to(ctx.temporary):
        raise BucketError(f"{variable} must identify a runner temporary file")
    return path


def summarize(ctx, text):
    with file_command(ctx, "GITHUB_STEP_SUMMARY").open("a", encoding="utf-8") as output:
        output.write(f"### Disposable schema AWS bucket\n\n{text}\n\n")


def create(ctx):
    principal = identity()
    if ctx.receipt.exists():
        raise BucketError("An ownership receipt already exists; create never adopts or resets a bucket")
    # An existing bucket is an error in us-west-2. Do not retry creation or
    # reinterpret BucketAlreadyOwnedByYou as a successful fresh creation.
    aws("s3api", "create-bucket", "--bucket", ctx.bucket,
        "--create-bucket-configuration", f"LocationConstraint={REGION}")
    receipt = {
        "version": 1, "bucket": ctx.bucket, "account": ACCOUNT, "region": REGION,
        "repository": REPOSITORY, "run_id": ctx.run_id, "run_attempt": ctx.attempt,
        "principal": principal, "owner_token": uuid.uuid4().hex,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    # Commit the ownership fact before tags or any later configuration call.
    with ctx.receipt.open("x", encoding="utf-8") as output:
        json.dump(receipt, output, indent=2, sort_keys=True)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    scope = ["--bucket", ctx.bucket, "--expected-bucket-owner", ACCOUNT]
    tagging = {"TagSet": [{"Key": key, "Value": value} for key, value in tags(ctx, receipt).items()]}
    aws("s3api", "put-bucket-tagging", *scope, "--tagging", json.dumps(tagging))
    public = {key: True for key in (
        "BlockPublicAcls", "IgnorePublicAcls", "BlockPublicPolicy", "RestrictPublicBuckets",
    )}
    aws("s3api", "put-public-access-block", *scope,
        "--public-access-block-configuration", json.dumps(public))
    encryption = {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]}
    aws("s3api", "put-bucket-encryption", *scope,
        "--server-side-encryption-configuration", json.dumps(encryption))
    location = aws("s3api", "get-bucket-location", *scope)
    if location.get("LocationConstraint") != REGION:
        raise BucketError("The new bucket is outside the approved region")
    values = {
        "SCHEMA_S3_TEST_MODE": "isolated-aws-bucket", "SCHEMA_S3_TEST_BUCKET": ctx.bucket,
        "SCHEMA_S3_TEST_EXPECTED_ACCOUNT": ACCOUNT, "AWS_REGION": REGION,
    }
    with file_command(ctx, "GITHUB_ENV").open("a", encoding="utf-8") as output:
        output.writelines(f"{key}={value}\n" for key, value in values.items())
    summarize(ctx, f"Created `{ctx.bucket}` in `{REGION}` for run `{ctx.run_id}` attempt `{ctx.attempt}`.\n\n"
              f"Ownership receipt: `{ctx.receipt}`.\n\n"
              "The bucket has public access blocked and S3 encryption configured. "
              "The checks use generated definitions only.")


def load_receipt(ctx):
    if not ctx.receipt.is_file() or ctx.receipt.stat().st_size > 8192:
        raise BucketError("The ownership receipt is missing, invalid, or too large")
    with ctx.receipt.open(encoding="utf-8") as source:
        receipt = json.load(source)
    expected = {
        "version": 1, "bucket": ctx.bucket, "account": ACCOUNT, "region": REGION,
        "repository": REPOSITORY, "run_id": ctx.run_id, "run_attempt": ctx.attempt,
    }
    if not isinstance(receipt, dict) or any(receipt.get(key) != value for key, value in expected.items()):
        raise BucketError("The receipt does not belong to this exact CI run, account, and bucket")
    token = receipt.get("owner_token")
    if not isinstance(token, str) or not re.fullmatch(r"[0-9a-f]{32}", token):
        raise BucketError("The ownership token is invalid")
    return receipt


def owned_objects(ctx, receipt):
    scope = ["--bucket", ctx.bucket, "--expected-bucket-owner", ACCOUNT]
    result = aws("s3api", "get-bucket-tagging", *scope)
    tag_set = result.get("TagSet")
    if not isinstance(tag_set, list) or any(not isinstance(tag, dict) for tag in tag_set):
        raise BucketError("The bucket has no verifiable owner tags; retain it")
    actual = {tag.get("Key"): tag.get("Value") for tag in tag_set}
    if len(actual) != len(tag_set) or actual != tags(ctx, receipt):
        raise BucketError("The bucket owner tags differ from the receipt; retain it")
    versioning = aws("s3api", "get-bucket-versioning", *scope)
    if versioning.get("Status") is not None or versioning.get("MFADelete") is not None:
        raise BucketError("A bucket with any versioning state requires separate recovery; retain it")
    listing = aws("s3api", "list-objects-v2", *scope, "--max-keys", "100")
    contents = listing.get("Contents", [])
    if listing.get("IsTruncated") is not False or not isinstance(contents, list) or len(contents) > 100:
        raise BucketError("The bounded object list is incomplete or invalid; retain the bucket")
    keys = [item.get("Key") if isinstance(item, dict) else None for item in contents]
    if any(not isinstance(key, str) or key not in ALLOWED_KEYS for key in keys):
        raise BucketError("The bucket contains an unknown object key; retain it without deletion")
    if len(set(keys)) != len(keys) or listing.get("KeyCount") != len(keys):
        raise BucketError("The bounded object count is inconsistent; retain the bucket")
    return scope, keys


def cleanup(ctx):
    if not ctx.receipt.exists():
        summarize(ctx, "No ownership receipt exists. Cleanup touched no bucket.")
        return
    receipt = load_receipt(ctx)
    identity()
    scope, keys = owned_objects(ctx, receipt)
    if keys:
        request = {"Objects": [{"Key": key} for key in keys], "Quiet": False}
        deleted = aws("s3api", "delete-objects", *scope, "--delete", json.dumps(request))
        if deleted.get("Errors"):
            raise BucketError("S3 refused one or more known-object deletes; retain the bucket and receipt")
        returned = deleted.get("Deleted", [])
        if (not isinstance(returned, list) or any(not isinstance(item, dict) for item in returned)
                or {item.get("Key") for item in returned} != set(keys)):
            raise BucketError("S3 did not confirm every known-object delete; retain the bucket and receipt")
    aws("s3api", "delete-bucket", *scope)
    summarize(ctx, f"Deleted the owned disposable bucket `{ctx.bucket}`.\n\n"
              f"The receipt remains at `{ctx.receipt}`.\n\n"
              "Cleanup permits only `definitions.json` and `direct-cas`. "
              "Unknown objects, versioning, or changed ownership stop cleanup before any delete.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("create", "cleanup"))
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    ctx = context(arguments.output)
    try:
        (create if arguments.command == "create" else cleanup)(ctx)
    except (BucketError, OSError, ValueError, subprocess.TimeoutExpired) as error:
        message = f"{error}. Bucket: {ctx.bucket}. Receipt: {ctx.receipt}. No broad cleanup is permitted."
        print(message, file=sys.stderr)
        summarize(ctx, f"Stopped: {message}")
        return 1
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (BucketError, OSError, ValueError, KeyError) as error:
        print(f"Disposable schema bucket guard refused the operation: {error}", file=sys.stderr)
        raise SystemExit(1) from None
