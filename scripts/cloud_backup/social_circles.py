"""Exercise a Social circle's whole-database reads and scoped writes in DEV."""

from contextlib import closing
import json
import os
import uuid

from node import Connection
from signed_files import retry, run_operation, summary
from social_app import call, observe_access, window
from support import decode_json, require

SCOPE = {"name": "whole-database", "version": 1}
ALBUM = "dev-albums/Album"
COMMENT = "dev-comments/Comment"


def declare(node, database, schema):
    namespace, name = schema.split("/")
    return node.call(f"/api/databases/{database}/schemas/declare", {
        "namespace": namespace, "schema": {
            "name": name, "descriptive_name": name, "purpose_statement": f"Disposable DEV {name} record.",
            "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
            "fields": ["text"], "field_types": {"text": "String"}, "field_descriptions": {"text": "Sample text."},
        },
    })["definition_id"]


def mutation(schema, definition, key, text):
    return {"type": "mutation", "schema": schema, "definition_id": definition,
            "key_value": {"hash": key, "range": None}, "alternates": {},
            "mutation_type": "create", "expected": {"type": "absent"}, "record": {"text": text}}


def query(node, database, schema, key):
    return node.call(f"/api/databases/{database}/query", {
        "schema_name": schema, "filter": {"HashKey": key},
    })["results"]


def wait_text(node, database, schema, key, text, stage):
    retry(stage, lambda: query(node, database, schema, key),
          lambda rows: len(rows) == 1 and rows[0]["fields"]["text"] == text)


def submit(node, database, session, update, expected):
    mutation_id = str(uuid.uuid4())
    with closing(Connection(node.socket)) as connection:
        connection.request("POST", f"/api/databases/{database}/mutation", json.dumps(update), {
            "Content-Type": "application/json", "Idempotency-Key": mutation_id,
            "X-LastDB-Database-Session": session,
        })
        with connection.getresponse() as reply:
            require(reply.status == expected, f"circle_write_http_{reply.status}_expected_{expected}")
            body = decode_json(reply.read(65537))
    if expected == 202:
        require(body["mutation_id"] == mutation_id and body["status"] == "submitted", "circle_submission_receipt")


def transfer(programs, root, publisher, recipient, api_key):
    name = f"ci-circles-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}"
    with window(publisher) as owner:
        database = call(owner, "/api/create", {"name": name, "sharingMode": "whole_database"})["id"]
        summary(f"Owned DEV mutation log `{database}` for `{name}`.")
        definitions = {schema: declare(publisher, database, schema) for schema in (ALBUM, COMMENT)}
        for schema in (ALBUM, COMMENT):
            publisher.call(f"/api/databases/{database}/mutation",
                           mutation(schema, definitions[schema], "seed", f"Initial {schema}"))
        connect(programs, publisher, api_key)
        contact = root / "recipient.contact"
        reader = programs.command(recipient.home, ["share", "contact", "--out", str(contact)])["recipient_public_key"]
        sender = programs.command(publisher.home, ["share", "contact", "--out", str(root / "owner.contact")])["recipient_public_key"]
        require(reader != sender, "circle_two_identities")
        call(owner, "/api/remember", {"name": "Ana", "nodeKey": reader, "contactCode": contact.read_text().strip()})
        retry("circle_cloud_ready", lambda: call(owner, f"/api/database?databaseId={database}"),
              lambda row: row["sharing"].get("revision") == "0")
        circle = {"name": "Family", "members": [reader], "writeSchemas": [COMMENT]}
        granted = call(owner, "/api/circles", {"databaseId": database, "expectedRevision": "0", "circles": [circle]})
        try:
            require(granted["revision"] == "1", "circle_grant_revision")
            receipt = call(owner, "/api/circle-invitation", {"databaseId": database, "nodeKey": reader})
            require(receipt["publisher"] == sender and receipt["databaseId"] == database, "circle_sealed_invitation")
            with window(recipient) as remote:
                exercise(publisher, recipient, remote, database, receipt, definitions)
        finally:
            roster = call(owner, f"/api/database?databaseId={database}")["sharing"]
            call(owner, "/api/circles", {"databaseId": database, "expectedRevision": roster["revision"], "circles": []})
            revoked = call(owner, f"/api/database?databaseId={database}")["sharing"]["members"]
            require(len(revoked) == 1 and revoked[0]["state"] == "revoked", "circle_cleanup_revoked")


def connect(programs, publisher, api_key):
    receipt = programs.command(publisher.home, [
        "backup", "connect", "--existing", "--env", "dev", "--api-key-stdin",
        "--expected-user-hash", publisher.account, "--json",
    ], input_bytes=api_key, timeout=20)
    require(receipt["environment"] == "dev" and receipt["account_id"] == publisher.account
            and receipt["credentials_saved"] is True, "circle_dev_connection")


def exercise(publisher, recipient, remote, database, receipt, definitions):
    accepted = call(remote, "/api/accept", {"receipt": receipt, "publisher": receipt["publisher"], "name": "family"})
    require(accepted["id"] == database and accepted["sharingMode"] == "whole_database", "circle_received_database")
    for schema in (ALBUM, COMMENT):
        wait_text(recipient, database, schema, "seed", f"Initial {schema}", "circle_whole_database_read")
    observe_access(remote, database, "active", 1, role="submit", write_schemas=[definitions[COMMENT]])
    session = call(remote, "/api/pick", {"spec": SCOPE, "intent": "write", "databaseId": database})["databaseSession"]
    submit(recipient, database, session, mutation(ALBUM, definitions[ALBUM], "blocked", "Cannot write"), 403)
    submit(recipient, database, session, mutation(COMMENT, definitions[COMMENT], "allowed", "From Ana"), 202)
    for node in (publisher, recipient):
        wait_text(node, database, COMMENT, "allowed", "From Ana", "circle_authorized_write_applied")
        require(not query(node, database, ALBUM, "blocked"), "circle_forbidden_write_missing")
    message = "Ana read Album and Comment. LastDB refused her Album write with HTTP 403, committed her Comment on the publisher, and synced it back."
    print(message, flush=True)
    summary(message)


if __name__ == "__main__":
    run_operation(transfer)
