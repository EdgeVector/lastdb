"""Retire only ledger-owned DEV scopes after revocation and the complete drain."""

import re

import shared_daily_io as io
import shared_daily_scopes as scopes
from support import decode_json, require


AWS_BUCKET = "exemem-native-backup-dev-us-west-2-152335099025"
R2_SECRET = "arn:aws:secretsmanager:us-west-2:152335099025:secret:ExememR2Credentials-dev-ZkqnBw"
B2_SECRET = "arn:aws:secretsmanager:us-west-2:152335099025:secret:ExememByteB2Credentials-dev-00448c3b20aa-J4sJI4"


def validate_targets(targets):
    require(isinstance(targets, list) and [t["name"] for t in targets] == ["aws", "r2", "b2"],
            "shared_daily_provider_set")
    a, r, b = targets
    require(a == {"name": "aws", "bucket": AWS_BUCKET, "region": io.REGION,
                  "endpoint": "https://s3.us-west-2.amazonaws.com"}, "shared_daily_aws_target")
    require(r["bucket"] == "exemem-sync-dev" and r["region"] == "auto"
            and r["secret_arn"] == R2_SECRET and re.fullmatch(
                r"https://[0-9a-f]{32}\.r2\.cloudflarestorage\.com", r["endpoint"]),
            "shared_daily_r2_target")
    require(b == {"name": "b2", "bucket": "exemem-dev", "region": "us-east-005",
                  "endpoint": "https://s3.us-east-005.backblazeb2.com", "secret_arn": B2_SECRET},
            "shared_daily_b2_target")


def configured_targets():
    stack = io.aws("cloudformation", "describe-stacks", {"StackName": "ExememNativeBackup-dev"})["Stacks"][0]
    parameters = {p["ParameterKey"]: p["ParameterValue"] for p in stack["Parameters"]}
    require(parameters["CloudByteRoutingEnabled"] == "true", "shared_daily_routing_enabled")
    targets = [
        {"name": "aws", "bucket": AWS_BUCKET, "region": io.REGION,
         "endpoint": "https://s3.us-west-2.amazonaws.com"},
        {"name": "r2", "bucket": parameters["CloudByteR2Bucket"], "region": "auto",
         "endpoint": f"https://{parameters['CloudByteR2AccountId']}.r2.cloudflarestorage.com",
         "secret_arn": parameters["CloudByteR2SecretArn"]},
        {"name": "b2", "bucket": parameters["CloudByteB2Bucket"], "region": parameters["CloudByteB2Region"],
         "endpoint": f"https://s3.{parameters['CloudByteB2Region']}.backblazeb2.com",
         "secret_arn": parameters["CloudByteB2SecretArn"]},
    ]
    validate_targets(targets)
    return targets


def credentials(target):
    if target["name"] == "aws":
        return None
    raw = io.aws("secretsmanager", "get-secret-value", {"SecretId": target["secret_arn"]})["SecretString"]
    require(len(raw) <= 4096, "shared_daily_provider_credentials_bound")
    secret = decode_json(raw.encode())
    identifier = secret.get("access_key_id", secret.get("key_id"))
    value = secret.get("secret_access_key", secret.get("app_key"))
    require(isinstance(identifier, str) and isinstance(value, str)
            and 0 < len(identifier) <= 256 and 0 < len(value) <= 2048, "shared_daily_provider_credentials")
    return identifier, value


def prefix(database, target):
    base = "cloud-backup/v2/shared" if target["name"] == "b2" else "cloud-shared/v1"
    return f"{base}/{io.database_id(database)}/objects/"


def contents(database, target, secret):
    root = prefix(database, target)
    operation = "list-objects-v2" if target["name"] == "r2" else "list-object-versions"
    request = {"Bucket": target["bucket"], "Prefix": root,
               "MaxKeys": 1000}
    found, marker = [], {}
    for _ in range(4):
        result = io.aws("s3api", operation, {**request, **marker}, target=target, credentials=secret)
        rows = (result.get("Contents", []) if target["name"] == "r2" else
                [*result.get("Versions", []), *result.get("DeleteMarkers", [])])
        for row in rows:
            require(row["Key"].startswith(root) and re.fullmatch(
                r"[0-9a-f-]{36}", row["Key"][len(root):]), "shared_daily_exact_object_key")
            found.append({name: row[name] for name in ("Key", "VersionId") if name in row})
        require(len(found) <= 2048, "shared_daily_object_count_bound")
        if not result.get("IsTruncated", False):
            return found
        marker = ({"ContinuationToken": result["NextContinuationToken"]} if target["name"] == "r2" else
                  {"KeyMarker": result["NextKeyMarker"], "VersionIdMarker": result["NextVersionIdMarker"]})
    require(False, "shared_daily_object_page_bound")


def remove_provider(database, target):
    secret = credentials(target)
    root = prefix(database, target)
    incomplete = io.aws("s3api", "list-multipart-uploads", {
        "Bucket": target["bucket"], "Prefix": root, "MaxUploads": 1000}, target=target, credentials=secret)
    require(not incomplete.get("IsTruncated", False), "shared_daily_multipart_directory_bound")
    for upload in incomplete.get("Uploads", []):
        require(upload["Key"].startswith(root), "shared_daily_multipart_scope")
        io.aws("s3api", "abort-multipart-upload", {"Bucket": target["bucket"],
               "Key": upload["Key"], "UploadId": upload["UploadId"]}, target=target, credentials=secret)
    objects = contents(database, target, secret)
    for offset in range(0, len(objects), 1000):
        selected = objects[offset:offset + 1000]
        reply = io.aws("s3api", "delete-objects", {"Bucket": target["bucket"],
                       "Delete": {"Objects": selected, "Quiet": False}}, target=target, credentials=secret)
        require(not reply.get("Errors") and len(reply.get("Deleted", [])) == len(selected),
                "shared_daily_exact_provider_deletion")
    require(not contents(database, target, secret), "shared_daily_provider_absence")
    remaining = io.aws("s3api", "list-multipart-uploads", {"Bucket": target["bucket"], "Prefix": root,
                       "MaxUploads": 1000}, target=target, credentials=secret)
    require(not remaining.get("Uploads") and not remaining.get("IsTruncated", False),
            "shared_daily_multipart_absence")
    return len(objects)


def remove_control(database, saved):
    partition = "shared#" + database
    rows = io.query(partition)
    require(all(row["record_id"]["S"] != "database" for row in rows), "shared_daily_authority_reappeared")
    for offset in range(0, len(rows), 23):
        checks = [{"ConditionCheck": {"TableName": io.TABLE, "Key": io.key(partition, "database"),
                   "ConditionExpression": "attribute_not_exists(record_id)"}},
                  {"ConditionCheck": {"TableName": io.TABLE, "Key": io.key(io.LEDGER, database),
                   "ConditionExpression": "revision = :r", "ExpressionAttributeValues": {":r": saved["revision"]}}}]
        deletes = [{"Delete": {"TableName": io.TABLE, "Key": {name: row[name] for name in ("account_id", "record_id")},
                    "ConditionExpression": "revision = :r", "ExpressionAttributeValues": {":r": row["revision"]}}}
                   for row in rows[offset:offset + 23]]
        io.aws("dynamodb", "transact-write-items", {"TransactItems": [*checks, *deletes]})
    require(not io.query(partition), "shared_daily_control_absence")
    io.aws("dynamodb", "delete-item", {"TableName": io.TABLE, "Key": io.key(io.LEDGER, database),
           "ConditionExpression": "revision = :r", "ExpressionAttributeValues": {":r": saved["revision"]}})
    require(io.get(io.LEDGER, database) is None, "shared_daily_ledger_absence")


def run(summary):
    identity = io.admission()
    summary.update(source=identity["source"], owner=identity, removed_scopes=[],
                   fenced_scopes=[], draining_scopes=[], active_scopes=[])
    for saved in scopes.directory():
        value = io.body(saved)
        database = value["scope"]["database_id"]
        summary["current_scope"] = database
        if value["phase"] == "active":
            if io.terminal(value["owner"]):
                scopes.fence(database)
                summary["fenced_scopes"].append(database)
            else:
                require(io.now_ms() - value["created_at_ms"] < io.MAX_AGE_MS, "shared_daily_active_owner_overdue")
                summary["active_scopes"].append(database)
            continue
        require(io.get("shared#" + database, "database") is None, "shared_daily_fence_changed")
        elapsed = io.now_ms() - value["fenced_at_ms"]
        require(elapsed >= 0, "shared_daily_fence_clock")
        if elapsed < io.GRACE_MS:
            summary["draining_scopes"].append(database)
            continue
        validate_targets(value["targets"])
        deleted = {target["name"]: remove_provider(database, target) for target in value["targets"]}
        remove_control(database, saved)
        summary["removed_scopes"].append({"database_id": database, "objects": deleted})
    summary.pop("current_scope", None)
    summary["result"] = "passed"
